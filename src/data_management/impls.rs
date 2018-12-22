use super::errors::*;
use super::{DmlBase, Handler, HandlerTypes, Object, PodType};
use allocator::{Action, SegmentAllocator, SegmentId};
use cache::{AddSize, Cache, ChangeKeyError, RemoveError};
use checksum::{Builder, Checksum, State};
use compression::{Compress, Compression};
use futures::executor::block_on;
use futures::future::{ok, FutureObj};
use futures::prelude::*;
use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use serde::de::DeserializeOwned;
use serde::ser::Error as SerError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use size::{SizeMut, StaticSize};
use stable_deref_trait::StableDeref;
use std::collections::HashMap;
use std::mem::{replace, transmute, ManuallyDrop};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::yield_now;
use storage_pool::{DiskOffset, StoragePoolLayer};
use vdev::{Block, BLOCK_SIZE};

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub struct ModifiedObjectId(u64);

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub enum ObjectKey<G> {
    Unmodified { offset: DiskOffset, generation: G },
    Modified(ModifiedObjectId),
    InWriteback(ModifiedObjectId),
}

pub enum ObjectRef<P> {
    Unmodified(P),
    Modified(ModifiedObjectId),
    InWriteback(ModifiedObjectId),
}

impl<C, D, I, G> super::ObjectRef for ObjectRef<ObjectPointer<C, D, I, G>>
where
    C: 'static,
    D: 'static,
    I: 'static,
    G: Copy + 'static,
    ObjectPointer<C, D, I, G>: Serialize + DeserializeOwned + StaticSize,
{
    type ObjectPointer = ObjectPointer<C, D, I, G>;
    fn get_unmodified(&self) -> Option<&ObjectPointer<C, D, I, G>> {
        if let ObjectRef::Unmodified(ref p) = *self {
            Some(p)
        } else {
            None
        }
    }
}

impl<C, D, I, G: Copy> ObjectRef<ObjectPointer<C, D, I, G>> {
    fn as_key(&self) -> ObjectKey<G> {
        match *self {
            ObjectRef::Unmodified(ref ptr) => ObjectKey::Unmodified {
                offset: ptr.offset,
                generation: ptr.generation,
            },
            ObjectRef::Modified(mid) => ObjectKey::Modified(mid),
            ObjectRef::InWriteback(mid) => ObjectKey::InWriteback(mid),
        }
    }
}

impl<P: StaticSize> StaticSize for ObjectRef<P> {
    fn size() -> usize {
        P::size()
    }
}

impl<P: Serialize> Serialize for ObjectRef<P> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self {
            ObjectRef::Modified(_) => Err(S::Error::custom(
                "ObjectRef: Tried to serialize a modified ObjectRef",
            )),
            ObjectRef::InWriteback(_) => Err(S::Error::custom(
                "ObjectRef: Tried to serialize a modified ObjectRef which is currently written back",
            )),
            ObjectRef::Unmodified(ref ptr) => ptr.serialize(serializer),
        }
    }
}

impl<'de, C, D, I, G: Copy> Deserialize<'de> for ObjectRef<ObjectPointer<C, D, I, G>>
where
    ObjectPointer<C, D, I, G>: Deserialize<'de>,
{
    fn deserialize<E>(deserializer: E) -> Result<Self, E::Error>
    where
        E: Deserializer<'de>,
    {
        ObjectPointer::<C, D, I, G>::deserialize(deserializer).map(ObjectRef::Unmodified)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectPointer<C, D, I, G> {
    compression: C,
    checksum: D,
    offset: DiskOffset,
    size: Block<u32>,
    info: I,
    generation: G,
}

impl<C: StaticSize, D: StaticSize, I: StaticSize, G: StaticSize> StaticSize
    for ObjectPointer<C, D, I, G>
{
    fn size() -> usize {
        C::size() + D::size() + I::size() + G::size() + <DiskOffset as StaticSize>::size() + 4
    }
}

impl<C, D, I, G: Copy> From<ObjectPointer<C, D, I, G>> for ObjectRef<ObjectPointer<C, D, I, G>> {
    fn from(ptr: ObjectPointer<C, D, I, G>) -> Self {
        ObjectRef::Unmodified(ptr)
    }
}

impl<C, D, I, G: Copy> ObjectPointer<C, D, I, G> {
    pub fn offset(&self) -> DiskOffset {
        self.offset
    }
    pub fn size(&self) -> Block<u32> {
        self.size
    }
    pub fn generation(&self) -> G {
        self.generation
    }
}

/// The Data Management Unit.
pub struct Dmu<C: 'static, E: 'static, SPL: StoragePoolLayer, H: 'static, I: 'static, G: 'static> {
    default_compression: C,
    default_checksum_builder: <SPL::Checksum as Checksum>::Builder,
    pool: SPL,
    cache: RwLock<E>,
    written_back: Mutex<HashMap<ModifiedObjectId, ObjectPointer<C, SPL::Checksum, I, G>>>,
    modified_info: Mutex<HashMap<ModifiedObjectId, I>>,
    handler: H,
    allocation_data: Box<[Mutex<Option<(SegmentId, SegmentAllocator)>>]>,
    next_modified_node_id: AtomicU64,
    next_disk_id: AtomicU64,
}

impl<C, E, SPL, H> Dmu<C, E, SPL, H, H::Info, H::Generation>
where
    SPL: StoragePoolLayer,
    H: HandlerTypes,
{
    /// Returns a new `Dmu`.
    pub fn new(
        default_compression: C,
        default_checksum_builder: <SPL::Checksum as Checksum>::Builder,
        pool: SPL,
        cache: E,
        handler: H,
    ) -> Self {
        let disk_cnt = pool.disk_count();
        Dmu {
            default_compression,
            default_checksum_builder,
            pool,
            cache: RwLock::new(cache),
            written_back: Mutex::new(HashMap::new()),
            modified_info: Mutex::new(HashMap::new()),
            handler,
            allocation_data: (0..disk_cnt)
                .map(|_| Mutex::new(None))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            next_modified_node_id: AtomicU64::new(1),
            next_disk_id: AtomicU64::new(0),
        }
    }

    /// Returns the underlying handler.
    pub fn handler(&self) -> &H {
        &self.handler
    }

    /// Returns the underlying cache.
    pub fn cache(&self) -> &RwLock<E> {
        &self.cache
    }

    /// Returns the underlying storage pool.
    pub fn pool(&self) -> &SPL {
        &self.pool
    }
}

impl<C, E, SPL, H> DmlBase for Dmu<C, E, SPL, H, H::Info, H::Generation>
where
    C: Compression + StaticSize,
    E: Cache<Key = ObjectKey<H::Generation>>,
    SPL: StoragePoolLayer,
    SPL::Checksum: StaticSize,
    H: HandlerTypes,
{
    type ObjectRef = ObjectRef<Self::ObjectPointer>;
    type ObjectPointer = ObjectPointer<C, SPL::Checksum, H::Info, H::Generation>;
    type Info = H::Info;
}

impl<C, E, SPL, H, I, G> Dmu<C, E, SPL, H, I, G>
where
    C: Compression + StaticSize,
    E: Cache<Key = ObjectKey<G>, Value = RwLock<H::Object>>,
    SPL: StoragePoolLayer,
    SPL::Checksum: StaticSize,
    H: Handler<ObjectRef<ObjectPointer<C, SPL::Checksum, I, G>>, Info = I, Generation = G>,
    H::Object: Object<ObjectRef<ObjectPointer<C, SPL::Checksum, I, G>>>,
    I: PodType,
    G: PodType,
{
    fn steal(
        &self,
        or: &mut <Self as DmlBase>::ObjectRef,
        info: H::Info,
    ) -> Result<Option<<Self as super::HandlerDml>::CacheValueRefMut>, Error> {
        let mid = self.next_modified_node_id.fetch_add(1, Ordering::Relaxed);
        let mid = ModifiedObjectId(mid);
        let entry = {
            let mut cache = self.cache.write();
            let was_present = cache.force_change_key(&or.as_key(), ObjectKey::Modified(mid));
            if !was_present {
                return Ok(None);
            }
            self.modified_info.lock().insert(mid, info);
            cache.get(&ObjectKey::Modified(mid), false).unwrap()
        };
        let obj = CacheValueRef::write(entry);
        if let ObjectRef::Unmodified(ptr) = replace(or, ObjectRef::Modified(mid)) {
            let actual_size = self.pool.actual_size(ptr.offset.disk_id() as u16, ptr.size);
            self.handler
                .copy_on_write(ptr.offset, actual_size, ptr.generation, ptr.info);
        }
        Ok(Some(obj))
    }

    /// Will be called when `or` is not in cache but was modified.
    /// Resolves two cases:
    ///     - Previous write back (`Modified(_)`)
    ///       Will change `or` to `InWriteback(_)`.
    ///     - Previous eviction after write back (`InWriteback(_)`)
    ///       Will change `or` to `Unmodified(_)`.
    fn fix_or(&self, or: &mut <Self as DmlBase>::ObjectRef) {
        match *or {
            ObjectRef::Unmodified(_) => unreachable!(),
            ObjectRef::Modified(mid) => {
                *or = ObjectRef::InWriteback(mid);
            }
            ObjectRef::InWriteback(mid) => {
                // The object must have been written back recently.
                let ptr = self.written_back.lock().remove(&mid).unwrap();
                *or = ObjectRef::Unmodified(ptr);
            }
        }
    }

    /// Fetches asynchronously an object from disk and inserts it into the
    /// cache.
    fn fetch(&self, op: &<Self as DmlBase>::ObjectPointer) -> Result<(), Error> {
        let compression = op.compression.clone();
        let offset = op.offset;
        let generation = op.generation;

        let compressed_data = self.pool.read(op.size, op.offset, op.checksum.clone())?;

        let object: H::Object = {
            let data = compression
                .decompress(compressed_data)
                .chain_err(|| ErrorKind::DecompressionError)?;
            Object::unpack(data).chain_err(|| ErrorKind::DeserializationError)?
        };
        let key = ObjectKey::Unmodified { offset, generation };
        self.insert_object_into_cache(key, RwLock::new(object));
        Ok(())
    }

    /// Fetches asynchronously an object from disk and inserts it into the
    /// cache.
    fn try_fetch_async(
        &self,
        op: &<Self as DmlBase>::ObjectPointer,
    ) -> Result<
        impl TryFuture<
                Ok = (
                    ObjectPointer<C, <SPL as StoragePoolLayer>::Checksum, I, G>,
                    Box<[u8]>,
                ),
                Error = Error,
            > + Send,
        Error,
    > {
        let ptr = op.clone();

        Ok(self
            .pool
            .read_async(op.size, op.offset, op.checksum.clone())?
            .map_err(Error::from)
            .and_then(move |data| ok((ptr, data))))
    }

    fn insert_object_into_cache(&self, key: ObjectKey<H::Generation>, mut object: E::Value) {
        let size = object.get_mut().size();
        let mut cache = self.cache.write();
        if !cache.contains_key(&key) {
            cache.insert(key, object, size);
        }
    }

    fn evict(&self, mut cache: RwLockWriteGuard<E>) -> Result<(), Error> {
        // TODO we may need to evict multiple objects
        // Algorithm overview:
        // Find some evictable object
        // - unpinned
        // - not in writeback state
        // - can_be_evicted
        // If it's `Unmodified` -> done
        // Change its key (`Modified`) to `InWriteback`
        // Pin object
        // Unlock cache
        // Serialize, compress, checksum
        // Fetch generation
        // Allocate
        // Write out
        // Try to remove from cache
        // If ok -> done
        // If this fails, call copy_on_write as object has been modified again

        let evict_result = cache.evict(|&key, entry, cache_contains_key| {
            let object = entry.get_mut();
            let can_be_evicted = match key {
                ObjectKey::InWriteback(_) => false,
                ObjectKey::Unmodified { .. } => true,
                ObjectKey::Modified(_) => object
                    .for_each_child(|or| {
                        let is_unmodified = loop {
                            if let ObjectRef::Unmodified(_) = *or {
                                break true;
                            }
                            if cache_contains_key(&or.as_key()) {
                                break false;
                            }
                            self.fix_or(or);
                        };
                        if is_unmodified {
                            Ok(())
                        } else {
                            Err(())
                        }
                    })
                    .is_ok(),
            };
            if can_be_evicted {
                Some(object.size())
            } else {
                None
            }
        });
        let (key, mut object) = match evict_result {
            None => return Ok(()),
            Some((key, object)) => (key, object),
        };

        let mid = match key {
            ObjectKey::InWriteback(_) => unreachable!(),
            ObjectKey::Unmodified { .. } => return Ok(()),
            ObjectKey::Modified(mid) => mid,
        };

        let size = object.get_mut().size();
        cache.insert(ObjectKey::InWriteback(mid), object, size);
        let entry = cache.get(&ObjectKey::InWriteback(mid), false).unwrap();

        drop(cache);
        let object = CacheValueRef::read(entry);

        self.handle_write_back(object, mid, true)?;
        Ok(())
    }

    fn handle_write_back(
        &self,
        object: <Self as super::HandlerDml>::CacheValueRef,
        mid: ModifiedObjectId,
        evict: bool,
    ) -> Result<<Self as DmlBase>::ObjectPointer, Error> {
        let object_size = super::Size::size(&*object);
        if object_size > 4 * 1024 * 1024 {
            warn!("Writing back large object: {}", object.debug_info());
        }

        let compression = self.default_compression.clone();
        let generation = self.handler.current_generation();

        let mut compressed_data = {
            let mut compress = compression.compress();
            {
                object
                    .pack(&mut compress)
                    .chain_err(|| ErrorKind::SerializationError)?;
                drop(object);
            }
            compress.finish()
        };

        let info = self.modified_info.lock().remove(&mid).unwrap();

        assert!(compressed_data.len() <= u32::max_value() as usize);
        let size = compressed_data.len();
        let size = Block(((size as usize + BLOCK_SIZE - 1) / BLOCK_SIZE) as u32);
        assert!(size.to_bytes() as usize >= compressed_data.len());
        let offset = self.allocate(size)?;
        if size.to_bytes() as usize != compressed_data.len() {
            let mut v = compressed_data.into_vec();
            v.resize(size.to_bytes() as usize, 0);
            compressed_data = v.into_boxed_slice();
        }

        let checksum = {
            let mut state = self.default_checksum_builder.build();
            state.ingest(&compressed_data);
            state.finish()
        };

        self.pool.begin_write(compressed_data, offset)?;

        let obj_ptr = ObjectPointer {
            offset,
            size,
            checksum,
            compression,
            generation,
            info,
        };

        let was_present;
        {
            let mut cache = self.cache.write();
            // We can safely ignore pins.
            // If it's pinned, it must be a readonly request.
            was_present = if evict {
                cache.force_remove(&ObjectKey::InWriteback(mid), object_size)
            } else {
                cache.force_change_key(
                    &ObjectKey::InWriteback(mid),
                    ObjectKey::Unmodified {
                        offset: obj_ptr.offset,
                        generation: obj_ptr.generation,
                    },
                )
            };
            if was_present {
                self.written_back.lock().insert(mid, obj_ptr.clone());
            }
        }
        if !was_present {
            // The object has been `stolen`.  Notify the handler.
            let actual_size = self.pool
                .actual_size(obj_ptr.offset.disk_id() as u16, obj_ptr.size);
            self.handler.copy_on_write(
                obj_ptr.offset,
                actual_size,
                obj_ptr.generation,
                obj_ptr.info,
            );
        }

        Ok(obj_ptr)
    }

    fn allocate(&self, size: Block<u32>) -> Result<DiskOffset, Error> {
        if size >= Block(2048) {
            warn!("Very large allocation requested: {:?}", size);
        }
        let start_disk_id = (self.next_disk_id.fetch_add(1, Ordering::Relaxed)
            % u64::from(self.pool.disk_count())) as u16;
        let disk_id = (start_disk_id..self.pool.disk_count())
            .chain(0..start_disk_id)
            .max_by_key(|&disk_id| {
                self.pool
                    .effective_free_size(disk_id, self.handler.get_free_space(disk_id))
            })
            .unwrap();
        let size = self.pool.actual_size(disk_id, size);
        let disk_size = self.pool.size_in_blocks(disk_id);
        let disk_offset = {
            let mut x = self.allocation_data[disk_id as usize].lock();

            if x.is_none() {
                let segment_id = SegmentId::get(DiskOffset::new(disk_id as usize, Block(0)));
                let allocator = self.handler
                    .get_allocation_bitmap(segment_id, self)
                    .chain_err(|| ErrorKind::HandlerError)?;
                *x = Some((segment_id, allocator));
            }
            let &mut (ref mut segment_id, ref mut allocator) = x.as_mut().unwrap();

            let first_seen_segment_id = *segment_id;
            loop {
                if let Some(segment_offset) = allocator.allocate(size.as_u32()) {
                    break segment_id.disk_offset(segment_offset);
                }
                let next_segment_id = segment_id.next(disk_size);
                trace!(
                    "Next allocator segment: {:?} -> {:?} ({:?})",
                    segment_id,
                    next_segment_id,
                    disk_size,
                );
                if next_segment_id == first_seen_segment_id {
                    bail!(ErrorKind::OutOfSpaceError);
                }
                *allocator = self.handler
                    .get_allocation_bitmap(next_segment_id, self)
                    .chain_err(|| ErrorKind::HandlerError)?;
                *segment_id = next_segment_id;
            }
        };
        info!("Allocated {:?} at {:?}", size, disk_offset);
        self.handler
            .update_allocation_bitmap(disk_offset, size, Action::Allocate, self)
            .chain_err(|| ErrorKind::HandlerError)?;
        Ok(disk_offset)
    }

    /// Tries to allocate `size` blocks at `disk_offset`.  Might fail if
    /// already in use.
    pub fn allocate_raw_at(&self, disk_offset: DiskOffset, size: Block<u32>) -> Result<(), Error> {
        let disk_id = disk_offset.disk_id();
        let num_disks = self.pool.num_disks(disk_id as u16);
        let size = size * num_disks as u32;
        let segment_id = SegmentId::get(disk_offset);
        let mut x = self.allocation_data[disk_id as usize].lock();
        let mut allocator = self.handler
            .get_allocation_bitmap(segment_id, self)
            .chain_err(|| ErrorKind::HandlerError)?;
        if allocator.allocate_at(size.as_u32(), SegmentId::get_block_offset(disk_offset)) {
            *x = Some((segment_id, allocator));
            self.handler
                .update_allocation_bitmap(disk_offset, size, Action::Allocate, self)
                .chain_err(|| ErrorKind::HandlerError)?;
            Ok(())
        } else {
            bail!("Cannot allocate raw at {:?} / {:?}", disk_offset, size)
        }
    }

    fn prepare_write_back(
        &self,
        mid: ModifiedObjectId,
        dep_mids: &mut Vec<ModifiedObjectId>,
    ) -> Result<Option<<Self as super::HandlerDml>::CacheValueRef>, ()> {
        loop {
            let mut cache = self.cache.write();
            if cache.contains_key(&ObjectKey::InWriteback(mid)) {
                // TODO wait
                drop(cache);
                yield_now();
                continue;
            }
            let result =
                cache.change_key(&ObjectKey::Modified(mid), |_, entry, cache_contains_key| {
                    let object = entry.get_mut();
                    let mut modified_children = false;
                    object
                        .for_each_child::<!, _>(|or| loop {
                            let mid = match *or {
                                ObjectRef::Unmodified(_) => break Ok(()),
                                ObjectRef::InWriteback(mid) | ObjectRef::Modified(mid) => mid,
                            };
                            if cache_contains_key(&or.as_key()) {
                                modified_children = true;
                                dep_mids.push(mid);
                                break Ok(());
                            }
                            self.fix_or(or);
                        })
                        .unwrap();
                    if modified_children {
                        Err(())
                    } else {
                        Ok(ObjectKey::InWriteback(mid))
                    }
                });
            return match result {
                Ok(()) => Ok(Some(
                    cache
                        .get(&ObjectKey::InWriteback(mid), false)
                        .map(CacheValueRef::read)
                        .unwrap(),
                )),
                Err(ChangeKeyError::NotPresent) => Ok(None),
                Err(ChangeKeyError::Pinned) => {
                    // TODO wait
                    warn!("Pinned node");
                    drop(cache);
                    yield_now();
                    continue;
                }
                Err(ChangeKeyError::CallbackError(())) => Err(()),
            };
        }
    }
}

impl<C, E, SPL, H, I, G> super::HandlerDml for Dmu<C, E, SPL, H, I, G>
where
    C: Compression + StaticSize,
    E: Cache<Key = ObjectKey<G>, Value = RwLock<H::Object>>,
    SPL: StoragePoolLayer,
    SPL::Checksum: StaticSize,
    H: Handler<ObjectRef<ObjectPointer<C, SPL::Checksum, I, G>>, Info = I, Generation = G>,
    H::Object: Object<<Self as DmlBase>::ObjectRef>,
    I: PodType,
    G: PodType,
{
    type Object = H::Object;
    type CacheValueRef = CacheValueRef<E::ValueRef, RwLockReadGuard<'static, H::Object>>;
    type CacheValueRefMut = CacheValueRef<E::ValueRef, RwLockWriteGuard<'static, H::Object>>;

    fn try_get(&self, or: &Self::ObjectRef) -> Option<Self::CacheValueRef> {
        let result = {
            // Drop order important
            let cache = self.cache.read();
            cache.get(&or.as_key(), false)
        };
        result.map(CacheValueRef::read)
    }

    fn try_get_mut(&self, or: &Self::ObjectRef) -> Option<Self::CacheValueRefMut> {
        if let ObjectRef::Modified(_) = *or {
            let result = {
                let cache = self.cache.read();
                cache.get(&or.as_key(), true)
            };
            result.map(CacheValueRef::write)
        } else {
            None
        }
    }

    fn get(&self, or: &mut Self::ObjectRef) -> Result<Self::CacheValueRef, Error> {
        let mut cache = self.cache.read();
        loop {
            if let Some(entry) = cache.get(&or.as_key(), true) {
                drop(cache);
                return Ok(CacheValueRef::read(entry));
            }
            if let ObjectRef::Unmodified(ref ptr) = *or {
                drop(cache);
                self.fetch(ptr)?;
                cache = self.cache.read();
            } else {
                self.fix_or(or);
            }
        }
    }

    fn get_mut(
        &self,
        or: &mut Self::ObjectRef,
        info: Self::Info,
    ) -> Result<Self::CacheValueRefMut, Error> {
        // Fast path
        if let Some(obj) = self.try_get_mut(or) {
            return Ok(obj);
        }
        // Object either not mutable or not present.
        loop {
            // Try to steal it if present.
            if let Some(obj) = self.steal(or, info)? {
                return Ok(obj);
            }
            // Fetch it.
            self.get(or)?;
        }
    }

    fn insert(&self, mut object: Self::Object, info: H::Info) -> Self::ObjectRef {
        let mid = ModifiedObjectId(self.next_modified_node_id.fetch_add(1, Ordering::Relaxed));
        self.modified_info.lock().insert(mid, info);
        let key = ObjectKey::Modified(mid);
        let size = object.size();
        self.cache.write().insert(key, RwLock::new(object), size);
        ObjectRef::Modified(mid)
    }

    fn insert_and_get_mut(
        &self,
        mut object: Self::Object,
        info: Self::Info,
    ) -> (Self::CacheValueRefMut, Self::ObjectRef) {
        let mid = ModifiedObjectId(self.next_modified_node_id.fetch_add(1, Ordering::Relaxed));
        self.modified_info.lock().insert(mid, info);
        let key = ObjectKey::Modified(mid);
        let size = object.size();
        let entry = {
            let mut cache = self.cache.write();
            cache.insert(key, RwLock::new(object), size);
            cache.get(&key, false).unwrap()
        };
        (CacheValueRef::write(entry), ObjectRef::Modified(mid))
    }

    fn remove(&self, or: Self::ObjectRef) {
        match self.cache.write().remove(&or.as_key(), |obj| obj.size()) {
            Ok(_) | Err(RemoveError::NotPresent) => {}
            // TODO
            Err(RemoveError::Pinned) => unimplemented!(),
        };
        if let ObjectRef::Unmodified(ref ptr) = or {
            let actual_size = self.pool.actual_size(ptr.offset.disk_id() as u16, ptr.size);
            self.handler
                .copy_on_write(ptr.offset, actual_size, ptr.generation, ptr.info);
        }
    }

    fn get_and_remove(&self, mut or: Self::ObjectRef) -> Result<H::Object, Error> {
        let obj = loop {
            self.get(&mut or)?;
            match self.cache.write().remove(&or.as_key(), |obj| obj.size()) {
                Ok(obj) => break obj,
                Err(RemoveError::NotPresent) => {}
                // TODO
                Err(RemoveError::Pinned) => unimplemented!(),
            };
        };
        if let ObjectRef::Unmodified(ref ptr) = or {
            let actual_size = self.pool.actual_size(ptr.offset.disk_id() as u16, ptr.size);
            self.handler
                .copy_on_write(ptr.offset, actual_size, ptr.generation, ptr.info);
        }
        Ok(obj.into_inner())
    }

    fn ref_from_ptr(r: Self::ObjectPointer) -> Self::ObjectRef {
        r.into()
    }

    fn evict(&self) -> Result<(), Error> {
        // TODO shortcut without locking cache
        let cache = self.cache.write();
        if cache.size() > cache.capacity() {
            self.evict(cache)?;
        }
        Ok(())
    }
}

impl<C, E, SPL, H, I, G> super::Dml for Dmu<C, E, SPL, H, I, G>
where
    C: Compression + StaticSize,
    E: Cache<Key = ObjectKey<G>, Value = RwLock<H::Object>>,
    SPL: StoragePoolLayer,
    SPL::Checksum: StaticSize,
    H: Handler<ObjectRef<ObjectPointer<C, SPL::Checksum, I, G>>, Info = I, Generation = G>,
    H::Object: Object<<Self as DmlBase>::ObjectRef>,
    I: PodType,
    G: PodType,
{
    fn write_back<F>(&self, mut acquire_or_lock: F) -> Result<Self::ObjectPointer, Error>
    where
        F: FnMut<()>,
        F::Output: DerefMut<Target = Self::ObjectRef>,
    {
        let (object, mid) = loop {
            let mut or = acquire_or_lock();
            let mid = match *or {
                ObjectRef::Unmodified(ref p) => return Ok(p.clone()),
                ObjectRef::InWriteback(mid) | ObjectRef::Modified(mid) => mid,
            };
            let mut mids = Vec::new();
            match self.prepare_write_back(mid, &mut mids) {
                Ok(None) => self.fix_or(&mut or),
                Ok(Some(object)) => break (object, mid),
                Err(()) => {
                    drop(or);
                    while let Some(&mid) = mids.last() {
                        match self.prepare_write_back(mid, &mut mids) {
                            Ok(None) => {}
                            Ok(Some(object)) => {
                                self.handle_write_back(object, mid, false)?;
                            }
                            Err(()) => continue,
                        };
                        mids.pop();
                    }
                }
            }
        };
        self.handle_write_back(object, mid, false)
    }

    type Prefetch =
        FutureObj<'static, Result<(<Self as DmlBase>::ObjectPointer, Box<[u8]>), Error>>;
    fn prefetch(&self, or: &Self::ObjectRef) -> Result<Option<Self::Prefetch>, Error> {
        if self.cache.read().contains_key(&or.as_key()) {
            return Ok(None);
        }
        Ok(match *or {
            ObjectRef::Modified(_) | ObjectRef::InWriteback(_) => None,
            ObjectRef::Unmodified(ref p) => Some(FutureObj::new(Box::new(
                self.try_fetch_async(p)?.into_future(),
            ))),
        })
    }

    fn finish_prefetch(&self, p: Self::Prefetch) -> Result<(), Error> {
        let (ptr, compressed_data) = block_on(p)?;
        let object: H::Object = {
            let data = ptr.compression
                .decompress(compressed_data)
                .chain_err(|| ErrorKind::DecompressionError)?;
            Object::unpack(data).chain_err(|| ErrorKind::DeserializationError)?
        };
        let key = ObjectKey::Unmodified {
            offset: ptr.offset,
            generation: ptr.generation,
        };
        self.insert_object_into_cache(key, RwLock::new(object));
        Ok(())
    }

    fn drop_cache(&self) {
        let mut cache = self.cache.write();
        let keys: Vec<_> = cache
            .iter()
            .cloned()
            .filter(|&key| {
                if let ObjectKey::Unmodified { .. } = key {
                    true
                } else {
                    false
                }
            })
            .collect();
        for key in keys {
            let _ = cache.remove(&key, |obj| obj.size());
        }
    }
}

pub struct CacheValueRef<T, U> {
    head: T,
    guard: ManuallyDrop<U>,
}

impl<T, U> Drop for CacheValueRef<T, U> {
    fn drop(&mut self) {
        unsafe {
            ManuallyDrop::drop(&mut self.guard);
        }
    }
}

impl<T: AddSize, U> AddSize for CacheValueRef<T, U> {
    fn add_size(&self, size_delta: isize) {
        self.head.add_size(size_delta)
    }
}

impl<T, U> CacheValueRef<T, RwLockReadGuard<'static, U>>
where
    T: StableDeref<Target = RwLock<U>>,
{
    fn read(head: T) -> Self {
        let guard = unsafe { transmute(RwLock::read(&head)) };
        CacheValueRef {
            head,
            guard: ManuallyDrop::new(guard),
        }
    }
}

impl<T, U> CacheValueRef<T, RwLockWriteGuard<'static, U>>
where
    T: StableDeref<Target = RwLock<U>>,
{
    fn write(head: T) -> Self {
        let guard = unsafe { transmute(RwLock::write(&head)) };
        CacheValueRef {
            head,
            guard: ManuallyDrop::new(guard),
        }
    }
}

unsafe impl<T, U> StableDeref for CacheValueRef<T, RwLockReadGuard<'static, U>> {}

impl<T, U> Deref for CacheValueRef<T, RwLockReadGuard<'static, U>> {
    type Target = U;
    fn deref(&self) -> &U {
        &*self.guard
    }
}

unsafe impl<T, U> StableDeref for CacheValueRef<T, RwLockWriteGuard<'static, U>> {}

impl<T, U> Deref for CacheValueRef<T, RwLockWriteGuard<'static, U>> {
    type Target = U;
    fn deref(&self) -> &U {
        &*self.guard
    }
}

impl<T, U> DerefMut for CacheValueRef<T, RwLockWriteGuard<'static, U>> {
    fn deref_mut(&mut self) -> &mut U {
        &mut *self.guard
    }
}

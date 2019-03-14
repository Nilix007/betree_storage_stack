//! This module provides the Database Layer.
use crate::atomic_option::AtomicOption;
use crate::cache::ClockCache;
use crate::checksum::{XxHash, XxHashBuilder};
use crate::compression;
use crate::cow_bytes::SlicedCowBytes;
use crate::data_management::{self, Dmu, HandlerDml};
use crate::size::StaticSize;
use crate::storage_pool::{Configuration, DiskOffset, StoragePoolLayer, StoragePoolUnit};
use crate::tree::{DefaultMessageAction, Inner as TreeInner, Node, Tree, TreeBaseLayer, TreeLayer};
use crate::vdev::Block;
use bincode::{deserialize, serialize_into};
use byteorder::{BigEndian, ByteOrder, LittleEndian};
use parking_lot::{Mutex, RwLock};
use seqlock::SeqLock;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::iter::FromIterator;
use std::mem::replace;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

mod dataset;
mod errors;
mod handler;
mod snapshot;
mod superblock;
use self::handler::Handler;
use self::superblock::Superblock;

pub use self::dataset::Dataset;
pub use self::errors::*;
pub use self::handler::update_allocation_bitmap_msg;
pub use self::snapshot::Snapshot;

type ObjectPointer =
    data_management::impls::ObjectPointer<compression::None, XxHash, DatasetId, Generation>;
type ObjectRef = data_management::impls::ObjectRef<ObjectPointer>;
type Object = Node<ObjectRef>;

type RootDmu = Arc<
    Dmu<
        compression::None,
        ClockCache<data_management::impls::ObjectKey<Generation>, RwLock<Object>>,
        StoragePoolUnit<XxHash>,
        Handler,
        DatasetId,
        Generation,
    >,
>;
type RootTree =
    Tree<RootDmu, DefaultMessageAction, Arc<TreeInner<ObjectRef, DatasetId, DefaultMessageAction>>>;

type DatasetTree = RootTree;

/// The database type.
pub struct Database {
    root_tree: RootTree,
    open_datasets: HashMap<DatasetId, DatasetTree>,
}

impl Database {
    /// Opens a database given by the storage pool configuration.
    pub fn open(cfg: &Configuration) -> Result<Self> {
        Self::open_with_cache_size(cfg, 256 * 1024 * 1024, false)
    }

    /// Creates a database given by the storage pool configuration.
    ///
    /// Note that any existing database will be overwritten!
    pub fn create(cfg: &Configuration) -> Result<Self> {
        Self::open_with_cache_size(cfg, 256 * 1024 * 1024, true)
    }

    /// Opens or creates a database given by the storage pool configuration and
    /// sets the given cache size.
    ///
    /// Note that any existing database will be overwritten if `create` is true!
    pub fn open_with_cache_size(
        cfg: &Configuration,
        cache_size: usize,
        create: bool,
    ) -> Result<Self> {
        let spl = StoragePoolUnit::<XxHash>::new(cfg).chain_err(|| ErrorKind::SplConfiguration)?;
        let disk_cnt = spl.disk_count();
        let handler = Handler {
            root_tree_inner: AtomicOption::new(),
            root_tree_snapshot: RwLock::new(None),
            current_generation: SeqLock::new(Generation(1)),
            free_space: HashMap::from_iter(
                (0..disk_cnt).map(|disk_id| (disk_id, AtomicU64::new(0))),
            ),
            delayed_messages: Mutex::new(Vec::new()),
            last_snapshot_generation: RwLock::new(HashMap::new()),
            allocations: AtomicU64::new(0),
            old_root_allocation: SeqLock::new(None),
        };
        let dmu = Arc::new(Dmu::new(
            compression::None,
            XxHashBuilder,
            spl,
            ClockCache::new(cache_size),
            handler,
        ));

        let (tree, root_ptr) = if create {
            Superblock::<ObjectPointer>::clear_superblock(dmu.pool())?;
            let tree = RootTree::empty_tree(DatasetId(0), DefaultMessageAction, dmu);
            tree.dmu()
                .handler()
                .root_tree_inner
                .set(Arc::clone(tree.inner()));
            {
                let dmu = tree.dmu();
                for disk_id in 0..disk_cnt {
                    dmu.allocate_raw_at(DiskOffset::new(disk_id as usize, Block(0)), Block(2))
                        .chain_err(|| "Superblock allocation failed")?;
                }
            }
            let root_ptr = tree.sync()?;
            (tree, root_ptr)
        } else {
            let root_ptr = match Superblock::<ObjectPointer>::fetch_superblocks(dmu.pool()) {
                Some(p) => p,
                None => bail!(ErrorKind::InvalidSuperblock),
            };
            let tree = RootTree::open(DatasetId(0), root_ptr.clone(), DefaultMessageAction, dmu);
            *tree.dmu().handler().old_root_allocation.lock_write() =
                Some((root_ptr.offset(), root_ptr.size()));
            tree.dmu()
                .handler()
                .root_tree_inner
                .set(Arc::clone(tree.inner()));
            (tree, root_ptr)
        };
        *tree.dmu().handler().current_generation.lock_write() = root_ptr.generation().next();
        *tree.dmu().handler().root_tree_snapshot.write() = Some(TreeInner::new_ro(
            RootDmu::ref_from_ptr(root_ptr),
            DefaultMessageAction,
        ));
        Ok(Database {
            root_tree: tree,
            open_datasets: Default::default(),
        })
    }

    fn sync_ds(&self, ds_id: DatasetId, ds_tree: &DatasetTree) -> Result<()> {
        let ptr = ds_tree.sync()?;
        let msg = DatasetData::update_ptr(ptr)?;
        let key = &ds_data_key(ds_id) as &[_];
        self.root_tree.insert(key, msg)?;
        Ok(())
    }

    fn flush_delayed_messages(&self) -> Result<()> {
        loop {
            let v = replace(
                &mut *self.root_tree.dmu().handler().delayed_messages.lock(),
                Vec::new(),
            );
            if v.is_empty() {
                break;
            }
            for (key, msg) in v {
                self.root_tree.insert(key, msg)?;
            }
        }
        Ok(())
    }

    /// Synchronizes the database.
    pub fn sync(&mut self) -> Result<()> {
        let mut ds_locks = Vec::with_capacity(self.open_datasets.len());
        for (&ds_id, ds_tree) in &self.open_datasets {
            loop {
                if let Some(lock) = ds_tree.try_lock_root() {
                    ds_locks.push(lock);
                    break;
                }
                info!("Sync: syncing tree of {:?}", ds_id);
                self.sync_ds(ds_id, ds_tree)?;
            }
        }
        let root_ptr = loop {
            self.flush_delayed_messages()?;
            let allocations_before = self
                .root_tree
                .dmu()
                .handler()
                .allocations
                .load(Ordering::Acquire);
            info!("Sync: syncing root tree");
            let root_ptr = self.root_tree.sync()?;
            let allocations_after = self
                .root_tree
                .dmu()
                .handler()
                .allocations
                .load(Ordering::Acquire);
            let allocations = allocations_after - allocations_before;
            if allocations <= 1 {
                break root_ptr;
            } else {
                info!("Sync: resyncing -- seen {} allocations", allocations);
            }
        };
        let pool = self.root_tree.dmu().pool();
        pool.flush()?;
        Superblock::<ObjectPointer>::write_superblock(pool, &root_ptr)?;
        pool.flush()?;
        let handler = self.root_tree.dmu().handler();
        *handler.old_root_allocation.lock_write() = None;
        handler.bump_generation();
        handler
            .root_tree_snapshot
            .write()
            .as_mut()
            .unwrap()
            .update_root_node(RootDmu::ref_from_ptr(root_ptr));
        Ok(())
    }
}

fn ss_key(ds_id: DatasetId, name: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + 8 + name.len());
    key.push(3);
    key.extend_from_slice(&ds_id.pack());
    key.extend_from_slice(name);
    key
}

fn ss_data_key(ds_id: DatasetId, ss_id: Generation) -> [u8; 17] {
    let mut key = [0; 17];
    key[0] = 4;
    key[1..9].copy_from_slice(&ds_id.pack());
    key[9..].copy_from_slice(&ss_id.pack());
    key
}

fn ss_data_key_max(mut ds_id: DatasetId) -> [u8; 9] {
    ds_id.0 += 1;
    let mut key = [0; 9];
    key[0] = 4;
    key[1..9].copy_from_slice(&ds_id.pack());
    key
}

fn dead_list_min_key(ds_id: DatasetId, ss_id: Generation) -> [u8; 17] {
    let mut key = [0; 17];
    key[0] = 5;
    key[1..9].copy_from_slice(&ds_id.pack());
    key[9..].copy_from_slice(&ss_id.pack());
    key
}

fn dead_list_max_key(ds_id: DatasetId, ss_id: Generation) -> [u8; 17] {
    dead_list_min_key(ds_id, ss_id.next())
}

fn dead_list_max_key_ds(mut ds_id: DatasetId) -> [u8; 9] {
    ds_id.0 += 1;
    let mut key = [0; 9];
    key[0] = 5;
    key[1..9].copy_from_slice(&ds_id.pack());
    key
}

fn dead_list_key(ds_id: DatasetId, cur_gen: Generation, offset: DiskOffset) -> [u8; 25] {
    let mut key = [0; 25];
    key[0] = 5;
    key[1..9].copy_from_slice(&ds_id.pack());
    key[9..17].copy_from_slice(&cur_gen.pack());
    BigEndian::write_u64(&mut key[17..], offset.as_u64());
    key
}

fn offset_from_dead_list_key(key: &[u8]) -> DiskOffset {
    DiskOffset::from_u64(BigEndian::read_u64(&key[17..]))
}

fn ds_data_key(id: DatasetId) -> [u8; 9] {
    let mut key = [0; 9];
    key[0] = 2;
    key[1..].copy_from_slice(&id.pack());
    key
}

fn fetch_ds_data<T>(root_tree: &T, id: DatasetId) -> Result<DatasetData<ObjectPointer>>
where
    T: TreeBaseLayer<DefaultMessageAction>,
{
    let key = &ds_data_key(id) as &[_];
    let data = root_tree.get(key)?.ok_or(ErrorKind::DoesNotExist)?;
    DatasetData::unpack(&data)
}

fn fetch_ss_data<T>(
    root_tree: &T,
    ds_id: DatasetId,
    ss_id: Generation,
) -> Result<DatasetData<ObjectPointer>>
where
    T: TreeBaseLayer<DefaultMessageAction>,
{
    let key = ss_data_key(ds_id, ss_id);
    let data = root_tree.get(key)?.ok_or(ErrorKind::DoesNotExist)?;
    DatasetData::unpack(&data)
}

#[derive(Debug, Serialize, Deserialize)]
struct DeadListData {
    size: Block<u32>,
    birth: Generation,
}

impl DeadListData {
    fn pack(&self) -> Result<[u8; 12]> {
        let mut buf = [0; 12];
        serialize_into(&mut buf[..], self)?;
        Ok(buf)
    }
    fn unpack(b: &[u8]) -> Result<Self> {
        Ok(deserialize(b)?)
    }
}

#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
struct DatasetId(u64);

impl DatasetId {
    fn pack(self) -> [u8; 8] {
        let mut b = [0; 8];
        BigEndian::write_u64(&mut b, self.0);
        b
    }

    fn unpack(b: &[u8]) -> Self {
        DatasetId(BigEndian::read_u64(b))
    }

    fn next(self) -> Self {
        DatasetId(self.0 + 1)
    }
}

impl StaticSize for DatasetId {
    fn size() -> usize {
        8
    }
}

#[derive(Debug)]
struct DatasetData<P> {
    previous_snapshot: Option<Generation>,
    ptr: P,
}

impl<P> DatasetData<P> {
    fn update_previous_snapshot(x: Option<Generation>) -> SlicedCowBytes {
        let mut b = [0; 8];
        let x = if let Some(generation) = x {
            assert_ne!(generation.0, 0);
            generation.0
        } else {
            0
        };
        LittleEndian::write_u64(&mut b[..], x);

        DefaultMessageAction::upsert_msg(0, &b)
    }
}

impl<P: Serialize> DatasetData<P> {
    fn update_ptr(ptr: P) -> Result<SlicedCowBytes> {
        let mut v = Vec::new();
        serialize_into(&mut v, &ptr)?;
        let msg = DefaultMessageAction::upsert_msg(8, &v);
        Ok(msg)
    }

    fn pack(&self) -> Result<Vec<u8>> {
        let mut v = vec![0; 8];
        let x = if let Some(generation) = self.previous_snapshot {
            assert_ne!(generation.0, 0);
            generation.0
        } else {
            0
        };
        LittleEndian::write_u64(&mut v, x);
        serialize_into(&mut v, &self.ptr)?;
        Ok(v)
    }
}
impl<P: DeserializeOwned> DatasetData<P> {
    fn unpack(b: &[u8]) -> Result<Self> {
        let x = LittleEndian::read_u64(b.get(..8).ok_or("invalid data")?);
        let ptr = deserialize(&b[8..])?;
        Ok(DatasetData {
            previous_snapshot: if x > 0 { Some(Generation(x)) } else { None },
            ptr,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
struct Generation(u64);

impl StaticSize for Generation {
    fn size() -> usize {
        8
    }
}

impl Generation {
    fn pack(self) -> [u8; 8] {
        let mut b = [0; 8];
        BigEndian::write_u64(&mut b, self.0);
        b
    }

    fn unpack(b: &[u8]) -> Self {
        Generation(BigEndian::read_u64(b))
    }

    fn next(self) -> Self {
        Generation(self.0 + 1)
    }
}

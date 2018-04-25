use super::errors::*;
use super::{dead_list_key, DatasetId, DeadListData, Generation, Object, ObjectPointer, ObjectRef,
            TreeInner};
use allocator::{Action, SegmentAllocator, SegmentId, SEGMENT_SIZE};
use atomic_option::AtomicOption;
use byteorder::{BigEndian, ByteOrder};
use cow_bytes::SlicedCowBytes;
use data_management::{self, HandlerDml};
use owning_ref::OwningRef;
use parking_lot::{Mutex, RwLock};
use seqlock::SeqLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use storage_pool::DiskOffset;
use tree::{DefaultMessageAction, Tree, TreeBaseLayer};
use vdev::Block;

/// Returns a message for updating the allocation bitmap.
pub fn update_allocation_bitmap_msg(
    offset: DiskOffset,
    size: Block<u32>,
    action: Action,
) -> SlicedCowBytes {
    let segment_offset = SegmentId::get_block_offset(offset);
    let data = vec![action.as_byte(); size.0 as usize].into_boxed_slice();
    DefaultMessageAction::upsert_msg(segment_offset, &data)
}

pub(super) struct Handler {
    pub root_tree_inner: AtomicOption<Arc<TreeInner<ObjectRef, DatasetId, DefaultMessageAction>>>,
    pub root_tree_snapshot: RwLock<Option<TreeInner<ObjectRef, DatasetId, DefaultMessageAction>>>,
    pub current_generation: SeqLock<Generation>,
    pub free_space: HashMap<u16, AtomicU64>,
    pub delayed_messages: Mutex<Vec<(Box<[u8]>, SlicedCowBytes)>>,
    pub last_snapshot_generation: RwLock<HashMap<DatasetId, Generation>>,
    pub allocations: AtomicU64,
    pub old_root_allocation: SeqLock<Option<(DiskOffset, Block<u32>)>>,
}

impl Handler {
    fn current_root_tree<'a, X>(
        &'a self,
        dmu: &'a X,
    ) -> impl TreeBaseLayer<DefaultMessageAction> + 'a
    where
        X: HandlerDml<
            Object = Object,
            ObjectRef = ObjectRef,
            ObjectPointer = ObjectPointer,
            Info = DatasetId,
        >,
    {
        Tree::from_inner(self.root_tree_inner.get().unwrap().as_ref(), dmu, false)
    }

    fn last_root_tree<'a, X>(
        &'a self,
        dmu: &'a X,
    ) -> Option<impl TreeBaseLayer<DefaultMessageAction> + 'a>
    where
        X: HandlerDml<
            Object = Object,
            ObjectRef = ObjectRef,
            ObjectPointer = ObjectPointer,
            Info = DatasetId,
        >,
    {
        OwningRef::new(self.root_tree_snapshot.read())
            .try_map(|lock| lock.as_ref().ok_or(()))
            .ok()
            .map(|inner| Tree::from_inner(inner, dmu, false))
    }

    pub(super) fn bump_generation(&self) {
        self.current_generation.lock_write().0 += 1;
    }
}

impl data_management::HandlerTypes for Handler {
    type Generation = Generation;
    type Info = DatasetId;
}

pub(super) fn segment_id_to_key(segment_id: SegmentId) -> [u8; 9] {
    let mut key = [0; 9];
    BigEndian::write_u64(&mut key[1..], segment_id.0);
    key
}

impl data_management::Handler<ObjectRef> for Handler {
    type Object = Object;
    type Error = Error;

    fn current_generation(&self) -> Self::Generation {
        self.current_generation.read()
    }

    fn update_allocation_bitmap<X>(
        &self,
        offset: DiskOffset,
        size: Block<u32>,
        action: Action,
        dmu: &X,
    ) -> Result<()>
    where
        X: HandlerDml<
            Object = Object,
            ObjectRef = ObjectRef,
            ObjectPointer = ObjectPointer,
            Info = DatasetId,
        >,
    {
        self.allocations.fetch_add(1, Ordering::Release);
        let key = segment_id_to_key(SegmentId::get(offset));
        let msg = update_allocation_bitmap_msg(offset, size, action);
        self.current_root_tree(dmu).insert(&key[..], msg)?;
        Ok(())
    }

    fn get_allocation_bitmap<X>(&self, id: SegmentId, dmu: &X) -> Result<SegmentAllocator>
    where
        X: HandlerDml<
            Object = Object,
            ObjectRef = ObjectRef,
            ObjectPointer = ObjectPointer,
            Info = DatasetId,
        >,
    {
        let key = segment_id_to_key(id);

        let mut segment = self.current_root_tree(dmu)
            .get(&key[..])?
            .map(|b| b.to_vec())
            .unwrap_or_default();
        segment.resize(SEGMENT_SIZE, 0);
        let mut segment = segment.into_boxed_slice();

        if let Some(tree) = self.last_root_tree(dmu) {
            if let Some(old_segment) = tree.get(&key[..])? {
                for (s, &x) in segment.iter_mut().zip(&old_segment[..]) {
                    *s |= x;
                }
            }
        }

        let mut allocator = SegmentAllocator::new(segment);

        if let Some((offset, size)) = self.old_root_allocation.read() {
            if SegmentId::get(offset) == id {
                allocator.allocate_at(size.as_u32(), SegmentId::get_block_offset(offset));
            }
        }

        Ok(allocator)
    }

    fn get_free_space(&self, _disk_id: u16) -> Block<u64> {
        // TODO account free space per vdev
        Block(0)
    }

    fn copy_on_write(
        &self,
        offset: DiskOffset,
        size: Block<u32>,
        generation: Generation,
        dataset_id: DatasetId,
    ) {
        if self.last_snapshot_generation
            .read()
            .get(&dataset_id)
            .cloned() < Some(generation)
        {
            // Deallocate
            let key = &segment_id_to_key(SegmentId::get(offset)) as &[_];
            let msg = update_allocation_bitmap_msg(offset, size, Action::Deallocate);
            self.delayed_messages.lock().push((key.into(), msg));
        } else {
            // Add to dead list
            let key = &dead_list_key(dataset_id, self.current_generation.read(), offset) as &[_];

            let data = DeadListData {
                birth: generation,
                size,
            }.pack()
                .unwrap();

            let msg = DefaultMessageAction::insert_msg(&data);
            self.delayed_messages.lock().push((key.into(), msg));
        }
    }
}

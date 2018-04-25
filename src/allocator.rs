//! This module provides `SegmentAllocator` and `SegmentId` for bitmap
//! allocation of 1GiB segments.

use byteorder::{BigEndian, ByteOrder};
use cow_bytes::CowBytes;
use std::u32;
use storage_pool::DiskOffset;
use vdev::Block;

/// 256KiB, so that `vdev::BLOCK_SIZE * SEGMENT_SIZE == 1GiB`
pub const SEGMENT_SIZE: usize = 1 << SEGMENT_SIZE_LOG_2;
const SEGMENT_SIZE_LOG_2: usize = 18;
const SEGMENT_SIZE_MASK: usize = SEGMENT_SIZE - 1;

/// Simple first-fit bitmap allocator
pub struct SegmentAllocator {
    data: Box<[u8; SEGMENT_SIZE]>,
}

impl SegmentAllocator {
    /// Constructs a new `SegmentAllocator` given the segment allocation bitmap.
    /// The `bitmap` must have a length of `SEGMENT_SIZE` and must contain only
    /// `0u8` and `1u8`.
    pub fn new(bitmap: Box<[u8]>) -> Self {
        assert_eq!(bitmap.len(), SEGMENT_SIZE);
        assert!(bitmap.iter().all(|&y| y <= 1));
        SegmentAllocator {
            // TODO any better ideas?
            data: unsafe { Box::from_raw(Box::into_raw(bitmap) as *mut [u8; SEGMENT_SIZE]) },
        }
    }

    /// Allocates a block of the given `size`.
    /// Returns `None` if the allocation request cannot be satisfied.
    pub fn allocate(&mut self, size: u32) -> Option<u32> {
        if size == 0 {
            return Some(0);
        }
        let offset = {
            let mut idx = 0;
            loop {
                loop {
                    if idx + size > SEGMENT_SIZE as u32 {
                        return None;
                    }
                    if self.data[idx as usize] == 0 {
                        break;
                    }
                    idx += 1;
                }

                let start_idx = (idx + 1) as usize;
                let end_idx = (idx + size) as usize;
                if let Some(first_alloc_idx) =
                    self.data[start_idx..end_idx].iter().position(|&e| e == 1)
                {
                    idx = (idx + 1) + first_alloc_idx as u32 + 1;
                } else {
                    break idx as u32;
                }
            }
        };
        self.mark(offset, size, Action::Allocate);
        Some(offset)
    }

    /// Allocates a block of the given `size` at `offset`.
    /// Returns `false` if the allocation request cannot be satisfied.
    pub fn allocate_at(&mut self, size: u32, offset: u32) -> bool {
        if size == 0 {
            return true;
        }
        if offset + size > SEGMENT_SIZE as u32 {
            return false;
        }

        let start_idx = offset as usize;
        let end_idx = (offset + size) as usize;
        if !self.data[start_idx..end_idx].iter().all(|&e| e == 0) {
            return false;
        }
        self.mark(offset, size, Action::Allocate);
        true
    }

    /// Deallocates the allocated block.
    pub fn deallocate(&mut self, offset: u32, size: u32) {
        self.mark(offset, size, Action::Deallocate);
    }

    fn mark(&mut self, offset: u32, size: u32, action: Action) {
        let start_idx = offset as usize;
        let end_idx = (offset + size) as usize;
        for x in self.data[start_idx..end_idx].iter_mut() {
            assert_eq!(*x, 1 - action.as_byte());
            *x = action.as_byte();
        }
    }
}

// TODO better wording
/// Allocation action
#[derive(Clone, Copy)]
pub enum Action {
    /// Deallocate an allocated block.
    Deallocate,
    /// Allocate a deallocated block.
    Allocate,
}

impl Action {
    /// Returns 1 if allocation and 0 if deallocation.
    pub fn as_byte(self) -> u8 {
        match self {
            Action::Deallocate => 0,
            Action::Allocate => 1,
        }
    }
}

/// Identifier for 1GiB segments of a `StoragePool`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SegmentId(pub u64);

impl SegmentId {
    /// Returns the corresponding segment of the given disk offset.
    pub fn get(offset: DiskOffset) -> Self {
        SegmentId(offset.as_u64() & !(SEGMENT_SIZE_MASK as u64))
    }

    /// Returns the block offset into the segment.
    pub fn get_block_offset(offset: DiskOffset) -> u32 {
        offset.as_u64() as u32 & SEGMENT_SIZE_MASK as u32
    }

    /// Returns the disk offset at the start of this segment.
    pub fn as_disk_offset(&self) -> DiskOffset {
        DiskOffset::from_u64(self.0)
    }

    /// Returns the disk offset of the block in this segment at the given
    /// offset.
    pub fn disk_offset(&self, segment_offset: u32) -> DiskOffset {
        DiskOffset::from_u64(self.0 + u64::from(segment_offset))
    }

    /// Returns the key of this segment for messages and queries.
    pub fn key(&self, key_prefix: &[u8]) -> CowBytes {
        // Shave off the two lower bytes because they are always null.
        let mut segment_key = [0; 8];
        BigEndian::write_u64(&mut segment_key[..], self.0);
        assert_eq!(&segment_key[6..], &[0, 0]);

        let mut key = CowBytes::new();
        key.push_slice(key_prefix);
        key.push_slice(&segment_key[..6]);
        key
    }

    /// Returns the ID of the disk that belongs to this segment.
    pub fn disk_id(&self) -> usize {
        self.as_disk_offset().disk_id()
    }

    /// Returns the next segment ID.
    /// Wraps around at the end of the disk.
    pub fn next(&self, disk_size: Block<u64>) -> SegmentId {
        let disk_offset = self.as_disk_offset();
        if disk_offset.block_offset().as_u64() + SEGMENT_SIZE as u64 >= disk_size.as_u64() {
            SegmentId::get(DiskOffset::new(disk_offset.disk_id(), Block(0)))
        } else {
            SegmentId(self.0 + SEGMENT_SIZE as u64)
        }
    }
}

use size::StaticSize;
use std::fmt;
use vdev::Block;

/// 12-bit disk ID, 52-bit block offset (see
/// [`BLOCK_SIZE`](../vdev/constant.BLOCK_SIZE.html))
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DiskOffset(u64);

impl DiskOffset {
    /// Constructs a new `DiskOffset`.
    /// The given `block_offset` may not be larger than (1 << 52) - 1.
    pub fn new(disk_id: usize, block_offset: Block<u64>) -> Self {
        let block_offset = block_offset.as_u64();
        assert_eq!(block_offset >> 52, 0, "the block offset is too large");
        let x = ((disk_id as u64) << 52) | block_offset;
        DiskOffset(x)
    }
    /// Returns the 12-bit disk ID.
    pub fn disk_id(&self) -> usize {
        (self.0 >> 52) as usize
    }
    /// Returns the block offset.
    pub fn block_offset(&self) -> Block<u64> {
        Block(self.0 & ((1 << 52) - 1))
    }
    /// Returns this object as `u64`.
    /// The disk id will be the upper 12 bits.
    /// The block offset will be the lower 52 bits.
    pub fn as_u64(&self) -> u64 {
        self.0
    }

    /// Constructs a disk offset from the given `u64`.
    pub fn from_u64(x: u64) -> Self {
        DiskOffset(x)
    }
}

impl StaticSize for DiskOffset {
    fn size() -> usize {
        8
    }
}

impl fmt::Debug for DiskOffset {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("DiskOffset")
            .field("disk_id", &self.disk_id())
            .field("block_offset", &self.block_offset())
            .finish()
    }
}

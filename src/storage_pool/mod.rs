//! This module provides the `StoragePoolLayer`
//! which manages vdevs and features a write-back queue with read-write
//! ordering.

use checksum::Checksum;
use futures::Future;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::fmt;
use std::io;
use vdev::{Block, Error as VdevError};

// TODO read-only storage pool layer?
// TODO Clone necessary?

/// A `StoragePoolLayer` which manages vdevs and features a write-back queue
pub trait StoragePoolLayer: Clone + Send + Sync + 'static {
    /// The checksum type used for verifying the data integrity.
    type Checksum: Checksum;

    /// A serializable configuration type for this `StoragePoolLayer` object.
    type Configuration: Serialize + DeserializeOwned + fmt::Display + fmt::Debug;

    /// Constructs a new object using the given `Configuration`.
    fn new(configuration: &Self::Configuration) -> Result<Self, io::Error>;

    /// Reads `size` blocks from the given `offset`.
    fn read(
        &self,
        size: Block<u32>,
        offset: DiskOffset,
        checksum: Self::Checksum,
    ) -> Result<Box<[u8]>, VdevError> {
        self.read_async(size, offset, checksum)
            .and_then(Future::wait)
    }

    /// Future returned by `read_async`.
    type ReadAsync: Future<Item = Box<[u8]>, Error = VdevError> + Send;

    /// Reads `size` blocks asynchronously from the given `offset`.
    fn read_async(
        &self,
        size: Block<u32>,
        offset: DiskOffset,
        checksum: Self::Checksum,
    ) -> Result<Self::ReadAsync, VdevError>;

    /// Issues a write request that might happen in the background.
    fn begin_write(&self, data: Box<[u8]>, offset: DiskOffset) -> Result<(), VdevError>;

    /// Writes the given `data` at `offset` for every `LeafVdev`.
    fn write_raw(&self, data: Box<[u8]>, offset: Block<u64>) -> Result<(), VdevError>;

    /// Reads `size` blocks from  the given `offset` for every `LeafVdev`.
    fn read_raw(&self, size: Block<u32>, offset: Block<u64>) -> Vec<Box<[u8]>>;

    /// Returns the actual size of a data block for a specific `Vdev`
    /// which may be larger due to parity data.
    fn actual_size(&self, disk_id: u16, size: Block<u32>) -> Block<u32>;

    /// Returns the size for a specific `Vdev`.
    fn size_in_blocks(&self, disk_id: u16) -> Block<u64>;

    /// Return the number of leaf vdevs for a specific `Vdev`.
    fn num_disks(&self, disk_id: u16) -> usize;

    /// Returns the effective free size for a specific `Vdev`.
    fn effective_free_size(&self, disk_id: u16, free_size: Block<u64>) -> Block<u64>;

    /// Returns the number of `Vdev`s.
    fn disk_count(&self) -> u16;

    /// Flushes the write-back queue and the underlying storage backend.
    fn flush(&self) -> Result<(), VdevError>;
}

mod disk_offset;
pub use self::disk_offset::DiskOffset;

pub mod configuration;
pub use self::configuration::Configuration;

mod unit;
pub use self::unit::StoragePoolUnit;

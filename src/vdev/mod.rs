//! This module provides an abstraction over various 'virtual devices' (short
//! *vdev*)
//! that are built on top of storage devices.

use crate::checksum::Checksum;
use async_trait::async_trait;
use std::sync::atomic::{AtomicU64, Ordering};

/// Internal block size (4KiB)
pub const BLOCK_SIZE: usize = 4096;

/// Provides statistics about (failed) requests performed by vdevs.
#[derive(Debug, Clone, Copy)]
pub struct Statistics {
    /// The total number of blocks of issued read requests
    pub read: Block<u64>,
    /// The total number of blocks of issued write requests
    pub written: Block<u64>,
    /// The total number of blocks of failed read requests due to read failures
    pub failed_reads: Block<u64>,
    /// The total number of blocks of failed read requests due to checksum
    /// errors
    pub checksum_errors: Block<u64>,
    /// The total number of blocks of failed write requests
    pub failed_writes: Block<u64>,
}

#[derive(Default)]
struct AtomicStatistics {
    read: AtomicU64,
    written: AtomicU64,
    failed_reads: AtomicU64,
    checksum_errors: AtomicU64,
    repaired: AtomicU64,
    failed_writes: AtomicU64,
}

impl AtomicStatistics {
    fn as_stats(&self) -> Statistics {
        Statistics {
            read: Block(self.read.load(Ordering::Relaxed)),
            written: Block(self.written.load(Ordering::Relaxed)),
            failed_reads: Block(self.failed_reads.load(Ordering::Relaxed)),
            checksum_errors: Block(self.checksum_errors.load(Ordering::Relaxed)),
            failed_writes: Block(self.failed_writes.load(Ordering::Relaxed)),
        }
    }
}

/// Result of a successful scrub request
#[derive(Debug)]
pub struct ScrubResult {
    /// The actual data scrubbed
    pub data: Box<[u8]>,
    /// The total number of faulted blocks detected
    pub faulted: Block<u32>,
    /// The total number of successfully rewritten blocks
    ///
    /// Note: The actual data on disk may still be faulted,
    /// but the underlying disk signaled a successful write.
    pub repaired: Block<u32>,
}

impl From<ScrubResult> for Box<[u8]> {
    fn from(x: ScrubResult) -> Self {
        x.data
    }
}

/// Trait for reading blocks of data.
#[async_trait]
pub trait VdevRead<C: Checksum>: Send + Sync {
    /// Reads `size` data blocks at `offset` and verifies the data with the
    /// `checksum`.
    /// May issue write operations to repair faulted data blocks of components.
    async fn read(
        &self,
        size: Block<u32>,
        offset: Block<u64>,
        checksum: C,
    ) -> Result<Box<[u8]>, Error>;

    /// Reads `size` blocks at `offset` and verifies the data with the
    /// `checksum`.
    /// In contrast to `read`, this function will read and verify data from
    /// every child vdev.
    /// May issue write operations to repair faulted data blocks of child vdevs.
    async fn scrub(
        &self,
        size: Block<u32>,
        offset: Block<u64>,
        checksum: C,
    ) -> Result<ScrubResult, Error>;

    /// Reads `size` blocks at `offset` of every child vdev. Does not verify
    /// the data.
    async fn read_raw(&self, size: Block<u32>, offset: Block<u64>)
        -> Result<Vec<Box<[u8]>>, Error>;
}

/// Trait for writing blocks of data.
#[async_trait]
pub trait VdevWrite {
    /// Writes the `data` at `offset`. Returns success if the data has been
    /// written to
    /// enough replicas so that the data can be retrieved later on.
    ///
    /// Note: `data.len()` must be a multiple of `BLOCK_SIZE`.
    async fn write(&self, data: Box<[u8]>, offset: Block<u64>) -> Result<(), Error>;

    /// Flushes pending data (in caches) to disk.
    fn flush(&self) -> Result<(), Error>;

    /// Writes the `data` at `offset` on all child vdevs like mirroring.
    /// Returns success
    /// if the data has been written to enough replicas so that the data can be
    /// retrieved later on.
    ///
    /// Note: `data.len()` must be a multiple of `BLOCK_SIZE`.
    async fn write_raw(&self, data: Box<[u8]>, offset: Block<u64>) -> Result<(), Error>;
}

/// Trait for general information about a vdev.
pub trait Vdev: Send + Sync {
    /// Returns the actual size of a data block which may be larger due to
    /// parity data.
    fn actual_size(&self, size: Block<u32>) -> Block<u32>;

    /// Returns the number of underlying block devices.
    fn num_disks(&self) -> usize;

    /// Returns the total size of this vdev.
    fn size(&self) -> Block<u64>;

    /// Returns the effective free size which may be smaller due to parity data.
    fn effective_free_size(&self, free_size: Block<u64>) -> Block<u64>;

    /// Turns self into a trait object.
    fn boxed<C: Checksum>(self) -> Box<dyn VdevBoxed<C>>
    where
        Self: VdevRead<C> + VdevWrite + Sized + 'static,
    {
        Box::new(self)
    }

    /// Returns the (unique) ID of this vdev.
    fn id(&self) -> &str;

    /// Returns statistics about this vedv
    fn stats(&self) -> Statistics;

    /// Executes `f` for each child vdev.
    fn for_each_child(&self, f: &mut dyn FnMut(&dyn Vdev));
}

/// Trait for reading from a leaf vdev.
#[async_trait]
pub trait VdevLeafRead<R: AsMut<[u8]> + Send>: Send + Sync {
    /// Reads `buffer.as_mut().len()` bytes at `offset`. Does not verify the
    /// data.
    async fn read_raw(&self, buffer: R, offset: Block<u64>) -> Result<R, Error>;

    /// Shall be called if this vdev returned faulty data for a read request
    /// so that the statistics for this vdev show this incident.
    fn checksum_error_occurred(&self, size: Block<u32>);
}

/// Trait for writing to a leaf vdev.
#[async_trait]
pub trait VdevLeafWrite: Send + Sync {
    /// Writes the `data` at `offset`.
    ///
    /// Note: `data.as_mut().len()` must be a multiple of `BLOCK_SIZE`.
    /// `is_repair` shall be set to `true` if this write request is a rewrite
    /// of data
    /// because of a failed or faulty read so that the statistics for this vdev
    /// can be updated.
    async fn write_raw<W: AsRef<[u8]> + Send + 'static>(
        &self,
        data: W,
        offset: Block<u64>,
        is_repair: bool,
    ) -> Result<(), Error>;

    /// Flushes pending data (in caches) to disk.
    fn flush(&self) -> Result<(), Error>;
}

/// Just a super trait of `Vdev + VdevRead<C> + VdevWrite`.
pub trait VdevBoxed<C: Checksum>: Vdev + VdevRead<C> + VdevWrite + Send + Sync {}

impl<C: Checksum, V: Vdev + VdevWrite + VdevRead<C>> VdevBoxed<C> for V {}

#[async_trait]
impl<T: VdevLeafWrite> VdevWrite for T {
    async fn write(&self, data: Box<[u8]>, offset: Block<u64>) -> Result<(), Error> {
        VdevLeafWrite::write_raw(self, data, offset, false).await
    }

    fn flush(&self) -> Result<(), Error> {
        VdevLeafWrite::flush(self)
    }

    async fn write_raw(&self, data: Box<[u8]>, offset: Block<u64>) -> Result<(), Error> {
        VdevLeafWrite::write_raw(self, data, offset, false).await
    }
}

#[cfg(test)]
#[macro_use]
pub mod test;

mod block;
pub use self::block::Block;

mod errors;
pub use self::errors::{Error, ErrorKind};

#[macro_use]
mod util;

mod file;
pub use self::file::File;

mod parity1;
pub use self::parity1::Parity1;

mod mirror;
pub use self::mirror::Mirror;

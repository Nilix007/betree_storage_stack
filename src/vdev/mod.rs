//! This module provides an abstraction over various 'virtual devices' (short
//! *vdev*)
//! that are built on top of storage devices.

use checksum::Checksum;
use futures::Future;
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
pub trait VdevRead<C: Checksum>: Clone + Send + Sync + 'static {
    /// The [`Future`](../../futures/future/trait.Future.html) corresponding to
    /// [`read`](trait.VdevRead.html#tymethod.read).
    type Read: Future<Item = Box<[u8]>, Error = Error> + Send + 'static;
    /// Reads `size` data blocks at `offset` and verifies the data with the
    /// `checksum`.
    /// May issue write operations to repair faulted data blocks of components.
    fn read(&self, size: Block<u32>, offset: Block<u64>, checksum: C) -> Self::Read;

    /// The [`Future`](../../futures/future/trait.Future.html) corresponding to
    /// [`scrub`](trait.VdevRead.html#tymethod.scrub).
    type Scrub: Future<Item = ScrubResult, Error = Error> + Send + 'static;
    /// Reads `size` blocks at `offset` and verifies the data with the
    /// `checksum`.
    /// In contrast to `read`, this function will read and verify data from
    /// every child vdev.
    /// May issue write operations to repair faulted data blocks of child vdevs.
    fn scrub(&self, size: Block<u32>, offset: Block<u64>, checksum: C) -> Self::Scrub;

    /// The [`Future`](../../futures/future/trait.Future.html) corresponding to
    /// [`read_raw`](trait.VdevRead.html#tymethod.read_raw).
    type ReadRaw: Future<Item = Vec<Box<[u8]>>, Error = Error> + Send;
    /// Reads `size` blocks at `offset` of every child vdev. Does not verify
    /// the data.
    fn read_raw(&self, size: Block<u32>, offset: Block<u64>) -> Self::ReadRaw;
}

/// Trait for writing blocks of data.
pub trait VdevWrite {
    /// The [`Future`](../../futures/future/trait.Future.html) corresponding to
    /// [`write`](trait.Vdev.html#tymethod.write).
    type Write: Future<Item = (), Error = Error> + Send + 'static;
    /// Writes the `data` at `offset`. Returns success if the data has been
    /// written to
    /// enough replicas so that the data can be retrieved later on.
    ///
    /// Note: `data.len()` must be a multiple of `BLOCK_SIZE`.
    fn write(&self, data: Box<[u8]>, offset: Block<u64>) -> Self::Write;

    /// Flushes pending data (in caches) to disk.
    fn flush(&self) -> Result<(), Error>;

    /// The [`Future`](../../futures/future/trait.Future.html) corresponding to
    /// [`write_raw`](trait.Vdev.html#tymethod.write_raw).
    type WriteRaw: Future<Item = (), Error = Error> + Send + 'static;
    /// Writes the `data` at `offset` on all child vdevs like mirroring.
    /// Returns success
    /// if the data has been written to enough replicas so that the data can be
    /// retrieved later on.
    ///
    /// Note: `data.len()` must be a multiple of `BLOCK_SIZE`.
    fn write_raw(&self, data: Box<[u8]>, offset: Block<u64>) -> Self::WriteRaw;
}

/// Trait for general information about a vdev.
pub trait Vdev: Send + Sync + 'static {
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
    fn boxed<C: Checksum>(self) -> Box<VdevBoxed<C>>
    where
        Self: VdevRead<C> + VdevWrite,
    {
        Box::new(self)
    }

    /// Returns the (unique) ID of this vdev.
    fn id(&self) -> &str;

    /// Returns statistics about this vedv
    fn stats(&self) -> Statistics;

    /// Executes `f` for each child vdev.
    fn for_each_child(&self, f: &mut FnMut(&Vdev));
}

/// Trait for reading from a leaf vdev.
pub trait VdevLeafRead<R: AsMut<[u8]> + Send>: Clone + Send + Sync + 'static {
    /// The [`Future`](../../futures/future/trait.Future.html) corresponding to
    /// [`read_raw`](trait.VdevLeaf.html#tymethod.read_raw).
    type ReadRaw: Future<Item = R, Error = Error> + Send;
    /// Reads `buffer.as_mut().len()` bytes at `offset`. Does not verify the
    /// data.
    fn read_raw(&self, buffer: R, offset: Block<u64>) -> Self::ReadRaw;

    /// Shall be called if this vdev returned faulty data for a read request
    /// so that the statistics for this vdev show this incident.
    fn checksum_error_occurred(&self, size: Block<u32>);
}

/// Trait for writing to a leaf vdev.
pub trait VdevLeafWrite: Clone + Send + Sync + 'static {
    /// The [`Future`](../../futures/future/trait.Future.html) corresponding to
    /// [`write_raw`](trait.Vdev.html#tymethod.write_raw).
    type WriteRaw: Future<Item = (), Error = Error> + Send + 'static;

    /// Writes the `data` at `offset`.
    ///
    /// Note: `data.as_mut().len()` must be a multiple of `BLOCK_SIZE`.
    /// `is_repair` shall be set to `true` if this write request is a rewrite
    /// of data
    /// because of a failed or faulty read so that the statistics for this vdev
    /// can be updated.
    fn write_raw<W: AsRef<[u8]> + Send + 'static>(
        &self,
        data: W,
        offset: Block<u64>,
        is_repair: bool,
    ) -> Self::WriteRaw;

    /// Flushes pending data (in caches) to disk.
    fn flush(&self) -> Result<(), Error>;
}

/// Just a sibling of `Vdev + VdevWrite + VdevRead<C>` which returns the
/// `Future`s as trait objects.
pub trait VdevBoxed<C: Checksum>: Send + Sync {
    /// Reads `size` data blocks at `offset` and verifies the data with the
    /// `checksum`.
    /// May issue write operations to repair faulted data blocks of components.
    fn read(
        &self,
        size: Block<u32>,
        offset: Block<u64>,
        checksum: C,
    ) -> Box<Future<Item = Box<[u8]>, Error = Error> + Send + 'static>;
    /// Writes the `data` at `offset`. Returns success if the data has been
    /// written to
    /// enough replicas so that the data can be retrieved later on.
    ///
    /// Note: `data.len()` must be a multiple of `BLOCK_SIZE`.
    fn write(
        &self,
        data: Box<[u8]>,
        offset: Block<u64>,
    ) -> Box<Future<Item = (), Error = Error> + Send + 'static>;
    /// Returns the actual size of a data block which may be larger due to
    /// parity data.
    fn actual_size(&self, size: Block<u32>) -> Block<u32>;
    /// Returns the total size of this vdev.
    fn size(&self) -> Block<u64>;
    /// Returns the effective free size which may be smaller due to parity data.
    fn effective_free_size(&self, free_size: Block<u64>) -> Block<u64>;
    /// Returns the number of underlying block devices.
    fn num_disks(&self) -> usize;
    /// Flushes pending data (in caches) to disk.
    fn flush(&self) -> Result<(), Error>;
    /// Clones this vdev.
    fn clone_boxed(&self) -> Box<VdevBoxed<C>>;
    /// Writes the `data` at `offset`.
    ///
    /// Note: `data.len()` must be a multiple of `BLOCK_SIZE`.
    fn write_raw(
        &self,
        data: Box<[u8]>,
        offset: Block<u64>,
    ) -> Box<Future<Item = (), Error = Error> + Send + 'static>;
    /// Reads `size` blocks at `offset` and verifies the data with the
    /// `checksum`.
    /// In contrast to `read`, this function will read and verify data from
    /// every child vdev.
    /// May issue write operations to repair faulted data blocks of child vdevs.
    fn scrub(
        &self,
        size: Block<u32>,
        offset: Block<u64>,
        checksum: C,
    ) -> Box<Future<Item = ScrubResult, Error = Error> + Send + 'static>;
    /// Reads `size` blocks at `offset` of every child vdev. Does not verify
    /// the data.
    fn read_raw(
        &self,
        size: Block<u32>,
        offset: Block<u64>,
    ) -> Box<Future<Item = Vec<Box<[u8]>>, Error = Error> + Send + 'static>;
}

impl<C: Checksum, V: Vdev + VdevWrite + VdevRead<C>> VdevBoxed<C> for V {
    default fn read(
        &self,
        size: Block<u32>,
        offset: Block<u64>,
        checksum: C,
    ) -> Box<Future<Item = Box<[u8]>, Error = Error> + Send + 'static> {
        Box::new(VdevRead::<C>::read(self, size, offset, checksum))
    }

    default fn write(
        &self,
        data: Box<[u8]>,
        offset: Block<u64>,
    ) -> Box<Future<Item = (), Error = Error> + Send + 'static> {
        Box::new(VdevWrite::write(self, data, offset))
    }

    fn actual_size(&self, size: Block<u32>) -> Block<u32> {
        Vdev::actual_size(self, size)
    }

    fn size(&self) -> Block<u64> {
        Vdev::size(self)
    }

    fn effective_free_size(&self, free_size: Block<u64>) -> Block<u64> {
        Vdev::effective_free_size(self, free_size)
    }

    fn num_disks(&self) -> usize {
        Vdev::num_disks(self)
    }

    fn flush(&self) -> Result<(), Error> {
        VdevWrite::flush(self)
    }
    fn clone_boxed(&self) -> Box<VdevBoxed<C>> {
        Box::new(V::clone(self))
    }
    default fn write_raw(
        &self,
        data: Box<[u8]>,
        offset: Block<u64>,
    ) -> Box<Future<Item = (), Error = Error> + Send + 'static> {
        Box::new(VdevWrite::write_raw(self, data, offset))
    }
    default fn scrub(
        &self,
        size: Block<u32>,
        offset: Block<u64>,
        checksum: C,
    ) -> Box<Future<Item = ScrubResult, Error = Error> + Send + 'static> {
        Box::new(VdevRead::<C>::scrub(self, size, offset, checksum))
    }
    default fn read_raw(
        &self,
        size: Block<u32>,
        offset: Block<u64>,
    ) -> Box<Future<Item = Vec<Box<[u8]>>, Error = Error> + Send + 'static> {
        Box::new(VdevRead::<C>::read_raw(self, size, offset))
    }
}

impl<T: VdevLeafWrite> VdevWrite for T {
    type Write = <T as VdevLeafWrite>::WriteRaw;
    type WriteRaw = <T as VdevLeafWrite>::WriteRaw;

    fn write(&self, data: Box<[u8]>, offset: Block<u64>) -> Self::Write {
        VdevLeafWrite::write_raw(self, data, offset, false)
    }

    fn flush(&self) -> Result<(), Error> {
        VdevLeafWrite::flush(self)
    }

    fn write_raw(&self, data: Box<[u8]>, offset: Block<u64>) -> Self::WriteRaw {
        VdevLeafWrite::write_raw(self, data, offset, false)
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

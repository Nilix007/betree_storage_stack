use super::errors::*;
use super::util::alloc_uninitialized;
use super::{
    AtomicStatistics, Block, ScrubResult, Statistics, Vdev, VdevLeafRead, VdevLeafWrite, VdevRead,
};
use crate::checksum::Checksum;
use async_trait::async_trait;
use libc::{c_ulong, ioctl};
use std::fs;
use std::io;
use std::os::unix::fs::FileExt;
use std::os::unix::fs::FileTypeExt;
use std::os::unix::io::AsRawFd;
use std::sync::atomic::Ordering;

/// `LeafVdev` that is backed by a file.
pub struct File {
    file: fs::File,
    id: String,
    size: Block<u64>,
    stats: AtomicStatistics,
}

impl File {
    /// Creates a new `File`.
    pub fn new(file: fs::File, id: String) -> Result<Self, io::Error> {
        let file_type = file.metadata()?.file_type();
        let size = if file_type.is_file() {
            Block::from_bytes(file.metadata()?.len())
        } else if file_type.is_block_device() {
            get_block_device_size(&file)?
        } else {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("Unsupported file type: {:?}", file_type),
            ));
        };
        Ok(File {
            file,
            id,
            size,
            stats: Default::default(),
        })
    }
}

#[cfg(target_os = "linux")]
fn get_block_device_size(file: &fs::File) -> Result<Block<u64>, io::Error> {
    const BLKGETSIZE64: c_ulong = 2148012658;
    let mut size: u64 = 0;
    let result = unsafe { ioctl(file.as_raw_fd(), BLKGETSIZE64, &mut size) };
    if result == 0 {
        Ok(Block::from_bytes(size))
    } else {
        Err(io::Error::last_os_error())
    }
}

#[async_trait]
impl<C: Checksum> VdevRead<C> for File {
    async fn read(
        &self,
        size: Block<u32>,
        offset: Block<u64>,
        checksum: C,
    ) -> Result<Box<[u8]>, Error> {
        self.stats.read.fetch_add(size.as_u64(), Ordering::Relaxed);
        let size_in_bytes = size.to_bytes() as usize;
        let mut buf = alloc_uninitialized(size_in_bytes);
        if let Err(e) = self.file.read_exact_at(&mut buf, offset.to_bytes()) {
            self.stats
                .failed_reads
                .fetch_add(size.as_u64(), Ordering::Relaxed);
            bail!(e)
        }
        match checksum
            .verify(&buf)
            .map_err(Error::from)
            .chain_err(|| ErrorKind::ReadError(self.id.clone()))
        {
            Ok(()) => Ok(buf),
            Err(e) => {
                self.stats
                    .checksum_errors
                    .fetch_add(size.as_u64(), Ordering::Relaxed);
                Err(e)
            }
        }
    }

    async fn scrub(
        &self,
        size: Block<u32>,
        offset: Block<u64>,
        checksum: C,
    ) -> Result<ScrubResult, Error> {
        let data = self.read(size, offset, checksum).await?;
        Ok(ScrubResult {
            data,
            repaired: Block(0),
            faulted: Block(0),
        })
    }

    async fn read_raw(
        &self,
        size: Block<u32>,
        offset: Block<u64>,
    ) -> Result<Vec<Box<[u8]>>, Error> {
        self.stats.read.fetch_add(size.as_u64(), Ordering::Relaxed);
        let size_in_bytes = size.to_bytes() as usize;
        let mut buf = alloc_uninitialized(size_in_bytes);
        match self.file.read_exact_at(&mut buf, offset.to_bytes()) {
            Ok(()) => Ok(vec![buf]),
            Err(e) => {
                self.stats
                    .failed_reads
                    .fetch_add(size.as_u64(), Ordering::Relaxed);
                bail!(e)
            }
        }
    }
}

impl Vdev for File {
    fn actual_size(&self, size: Block<u32>) -> Block<u32> {
        size
    }

    fn num_disks(&self) -> usize {
        1
    }

    fn size(&self) -> Block<u64> {
        self.size
    }

    fn effective_free_size(&self, free_size: Block<u64>) -> Block<u64> {
        free_size
    }

    fn id(&self) -> &str {
        &self.id
    }

    fn stats(&self) -> Statistics {
        self.stats.as_stats()
    }

    fn for_each_child(&self, _f: &mut dyn FnMut(&dyn Vdev)) {}
}

#[async_trait]
impl<T: AsMut<[u8]> + Send + 'static> VdevLeafRead<T> for File {
    async fn read_raw(&self, mut buf: T, offset: Block<u64>) -> Result<T, Error> {
        let size = Block::from_bytes(buf.as_mut().len() as u32);
        self.stats.read.fetch_add(size.as_u64(), Ordering::Relaxed);
        match self.file.read_exact_at(buf.as_mut(), offset.to_bytes()) {
            Ok(()) => Ok(buf),
            Err(e) => {
                self.stats
                    .failed_reads
                    .fetch_add(size.as_u64(), Ordering::Relaxed);
                bail!(e)
            }
        }
    }

    fn checksum_error_occurred(&self, size: Block<u32>) {
        self.stats
            .checksum_errors
            .fetch_add(size.as_u64(), Ordering::Relaxed);
    }
}

#[async_trait]
impl VdevLeafWrite for File {
    async fn write_raw<T: AsRef<[u8]> + Send + 'static>(
        &self,
        data: T,
        offset: Block<u64>,
        is_repair: bool,
    ) -> Result<(), Error> {
        let block_cnt = Block::from_bytes(data.as_ref().len() as u64).as_u64();
        self.stats.written.fetch_add(block_cnt, Ordering::Relaxed);
        match self
            .file
            .write_all_at(data.as_ref(), offset.to_bytes())
            .map_err(Error::from)
            .chain_err(|| ErrorKind::WriteError(self.id.clone()))
        {
            Ok(()) => {
                if is_repair {
                    self.stats.repaired.fetch_add(block_cnt, Ordering::Relaxed);
                }
                Ok(())
            }
            Err(e) => {
                self.stats
                    .failed_writes
                    .fetch_add(block_cnt, Ordering::Relaxed);
                Err(e)
            }
        }
    }
    fn flush(&self) -> Result<(), Error> {
        Ok(self.file.sync_data()?)
    }
}

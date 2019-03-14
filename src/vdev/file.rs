use super::errors::*;
use super::util::alloc_uninitialized;
use super::{
    AtomicStatistics, Block, ScrubResult, Statistics, Vdev, VdevLeafRead, VdevLeafWrite, VdevRead,
};
use crate::checksum::Checksum;
use futures::future::{lazy, ready, FutureObj, Ready};
use futures::prelude::*;
use libc::{c_ulong, ioctl};
use std::fs;
use std::io;
use std::os::unix::fs::FileExt;
use std::os::unix::fs::FileTypeExt;
use std::os::unix::io::AsRawFd;
use std::sync::atomic::Ordering;
use std::sync::Arc;

/// `LeafVdev` that is backed by a file.
pub struct File {
    inner: Arc<Inner>,
}

struct Inner {
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
            inner: Arc::new(Inner {
                file,
                id,
                size,
                stats: Default::default(),
            }),
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

impl<C: Checksum> VdevRead<C> for File {
    type Read = FutureObj<'static, Result<Box<[u8]>, Error>>;
    type Scrub = FutureObj<'static, Result<ScrubResult, Error>>;
    type ReadRaw = FutureObj<'static, Result<Vec<Box<[u8]>>, Error>>;

    fn read(&self, size: Block<u32>, offset: Block<u64>, checksum: C) -> Self::Read {
        self.inner
            .stats
            .read
            .fetch_add(size.as_u64(), Ordering::Relaxed);
        let inner = Arc::clone(&self.inner);
        FutureObj::new(Box::new(lazy(move |_| {
            let size_in_bytes = size.to_bytes() as usize;
            let mut buf = alloc_uninitialized(size_in_bytes);
            match inner.file.read_exact_at(&mut buf, offset.to_bytes()) {
                Ok(()) => {}
                Err(e) => {
                    inner
                        .stats
                        .failed_reads
                        .fetch_add(size.as_u64(), Ordering::Relaxed);
                    bail!(e)
                }
            };
            match checksum
                .verify(&buf)
                .map_err(Error::from)
                .chain_err(|| ErrorKind::ReadError(inner.id.clone()))
            {
                Ok(()) => Ok(buf),
                Err(e) => {
                    inner
                        .stats
                        .checksum_errors
                        .fetch_add(size.as_u64(), Ordering::Relaxed);
                    Err(e)
                }
            }
        })))
    }

    fn scrub(&self, size: Block<u32>, offset: Block<u64>, checksum: C) -> Self::Scrub {
        fn to_scrub_result(data: Box<[u8]>) -> Ready<Result<ScrubResult, Error>> {
            ready(Ok(ScrubResult {
                data,
                repaired: Block(0),
                faulted: Block(0),
            }))
        }
        FutureObj::new(Box::new(
            self.read(size, offset, checksum).and_then(to_scrub_result),
        ))
    }
    fn read_raw(&self, size: Block<u32>, offset: Block<u64>) -> Self::ReadRaw {
        self.inner
            .stats
            .read
            .fetch_add(size.as_u64(), Ordering::Relaxed);
        let inner = Arc::clone(&self.inner);
        FutureObj::new(Box::new(lazy(move |_| {
            let size_in_bytes = size.to_bytes() as usize;
            let mut buf = alloc_uninitialized(size_in_bytes);
            match inner.file.read_exact_at(&mut buf, offset.to_bytes()) {
                Ok(()) => {}
                Err(e) => {
                    inner
                        .stats
                        .failed_reads
                        .fetch_add(size.as_u64(), Ordering::Relaxed);
                    bail!(e)
                }
            };
            Ok(vec![buf])
        })))
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
        self.inner.size
    }

    fn effective_free_size(&self, free_size: Block<u64>) -> Block<u64> {
        free_size
    }

    fn id(&self) -> &str {
        &self.inner.id
    }

    fn stats(&self) -> Statistics {
        self.inner.stats.as_stats()
    }

    fn for_each_child(&self, _f: &mut FnMut(&Vdev)) {}
}

impl<T: AsMut<[u8]> + Send + 'static> VdevLeafRead<T> for File {
    type ReadRaw = FutureObj<'static, Result<T, Error>>;

    fn read_raw(&self, mut buf: T, offset: Block<u64>) -> Self::ReadRaw {
        let size = Block::from_bytes(buf.as_mut().len() as u32);
        self.inner
            .stats
            .read
            .fetch_add(size.as_u64(), Ordering::Relaxed);
        let inner = Arc::clone(&self.inner);
        FutureObj::new(Box::new(lazy(move |_| {
            match inner.file.read_exact_at(buf.as_mut(), offset.to_bytes()) {
                Ok(()) => Ok(buf),
                Err(e) => {
                    inner
                        .stats
                        .failed_reads
                        .fetch_add(size.as_u64(), Ordering::Relaxed);
                    bail!(e)
                }
            }
        })))
    }

    fn checksum_error_occurred(&self, size: Block<u32>) {
        self.inner
            .stats
            .checksum_errors
            .fetch_add(size.as_u64(), Ordering::Relaxed);
    }
}

impl VdevLeafWrite for File {
    type WriteRaw = FutureObj<'static, Result<(), Error>>;

    fn write_raw<T: AsRef<[u8]> + Send + 'static>(
        &self,
        data: T,
        offset: Block<u64>,
        is_repair: bool,
    ) -> Self::WriteRaw {
        let block_cnt = Block::from_bytes(data.as_ref().len() as u64).as_u64();
        self.inner
            .stats
            .written
            .fetch_add(block_cnt, Ordering::Relaxed);
        let inner = Arc::clone(&self.inner);
        FutureObj::new(Box::new(lazy(move |_| {
            match inner
                .file
                .write_all_at(data.as_ref(), offset.to_bytes())
                .map_err(Error::from)
                .chain_err(|| ErrorKind::WriteError(inner.id.clone()))
            {
                Ok(()) => {
                    if is_repair {
                        inner.stats.repaired.fetch_add(block_cnt, Ordering::Relaxed);
                    }
                    Ok(())
                }
                Err(e) => {
                    inner
                        .stats
                        .failed_writes
                        .fetch_add(block_cnt, Ordering::Relaxed);
                    Err(e)
                }
            }
        })))
    }
    fn flush(&self) -> Result<(), Error> {
        Ok(self.inner.file.sync_data()?)
    }
}

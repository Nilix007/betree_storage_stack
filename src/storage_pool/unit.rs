use super::{Configuration, DiskOffset, StoragePoolLayer};
use bounded_future_queue::BoundedFutureQueue;
use checksum::Checksum;
use futures::executor::{block_on, ThreadPool};
use futures::future::join_all;
use futures::prelude::*;
use parking_lot::Mutex;
use std::io;
use std::sync::Arc;
use vdev::{Block, Error as VdevError, VdevBoxed};

/// Actual implementation of the `StoragePoolLayer`.
#[derive(Clone)]
pub struct StoragePoolUnit<C: Checksum> {
    inner: Arc<Inner<C>>,
}

pub(super) type WriteBackQueue =
    BoundedFutureQueue<DiskOffset, Box<Future<Item = (), Error = VdevError> + Send + 'static>>;

struct Inner<C> {
    devices: Vec<Box<VdevBoxed<C>>>,
    write_back_queue: Mutex<WriteBackQueue>,
    pool: ThreadPool,
}

impl<C: Checksum> StoragePoolLayer for StoragePoolUnit<C> {
    type Checksum = C;
    type Configuration = Configuration;

    fn new(configuration: &Self::Configuration) -> Result<Self, io::Error> {
        let devices = configuration.build()?;
        Ok(StoragePoolUnit {
            inner: Arc::new(Inner {
                write_back_queue: Mutex::new(BoundedFutureQueue::new(20 * devices.len())),
                devices,
                pool: ThreadPool::new()?,
            }),
        })
    }

    type ReadAsync = Box<Future<Item = Box<[u8]>, Error = VdevError> + Send>;

    fn read_async(
        &self,
        size: Block<u32>,
        offset: DiskOffset,
        checksum: C,
    ) -> Result<Self::ReadAsync, VdevError> {
        self.inner.write_back_queue.lock().wait(&offset)?;
        Ok(Box::new(
            self.inner.devices[offset.disk_id()]
                .read(size, offset.block_offset(), checksum)
                .with_executor(self.inner.pool.clone()),
        ))
    }

    fn begin_write(&self, data: Box<[u8]>, offset: DiskOffset) -> Result<(), VdevError> {
        let write = self.inner.devices[offset.disk_id()]
            .write(data, offset.block_offset())
            .with_executor(self.inner.pool.clone());
        self.inner.write_back_queue.lock().enqueue(
            offset,
            Box::new(write) as Box<Future<Item = _, Error = _> + Send>,
        )
    }

    fn write_raw(&self, data: Box<[u8]>, offset: Block<u64>) -> Result<(), VdevError> {
        let vec: Vec<_> = self.inner
            .devices
            .iter()
            .map(|vdev| vdev.write_raw(data.clone(), offset))
            .collect();
        block_on(join_all(vec)).map(|_| ())
    }

    fn read_raw(&self, size: Block<u32>, offset: Block<u64>) -> Vec<Box<[u8]>> {
        let mut vec = Vec::new();
        for vdev in &self.inner.devices {
            if let Ok(v) = block_on(vdev.read_raw(size, offset)) {
                vec.extend(v);
            }
        }
        vec
    }

    fn actual_size(&self, disk_id: u16, size: Block<u32>) -> Block<u32> {
        self.inner.devices[disk_id as usize].actual_size(size)
    }

    fn size_in_blocks(&self, disk_id: u16) -> Block<u64> {
        self.inner.devices[disk_id as usize].size()
    }

    fn num_disks(&self, disk_id: u16) -> usize {
        self.inner.devices[disk_id as usize].num_disks()
    }

    fn effective_free_size(&self, disk_id: u16, free_size: Block<u64>) -> Block<u64> {
        self.inner.devices[disk_id as usize].effective_free_size(free_size)
    }

    fn disk_count(&self) -> u16 {
        self.inner.devices.len() as u16
    }

    fn flush(&self) -> Result<(), VdevError> {
        self.inner.write_back_queue.lock().flush()?;
        for vdev in &self.inner.devices {
            vdev.flush()?;
        }
        Ok(())
    }
}

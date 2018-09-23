use super::errors::*;
use super::util::*;
use super::{
    AtomicStatistics, Block, ScrubResult, Statistics, Vdev, VdevLeafRead, VdevLeafWrite, VdevRead,
    VdevWrite,
};
use buffer::SplittableBuffer;
use checksum::Checksum;
use futures::future::join_all;
use std::future::{from_generator, Future, FutureObj};
use std::sync::atomic::Ordering;
use std::sync::Arc;

/// This `vdev` will mirror all data to its child vdevs.
pub struct Mirror<V> {
    inner: Arc<Inner<V>>,
}

struct Inner<V> {
    vdevs: Box<[V]>,
    id: String,
    stats: AtomicStatistics,
}

impl<V> Mirror<V> {
    /// Creates a new `Mirror`.
    pub fn new(vdevs: Box<[V]>, id: String) -> Self {
        Mirror {
            inner: Arc::new(Inner {
                vdevs,
                id,
                stats: Default::default(),
            }),
        }
    }
}

struct ReadResult<V> {
    data: Option<Box<[u8]>>,
    failed_disks: Vec<usize>,
    inner: Arc<Inner<V>>,
}

async fn handle_repair<F, V, R>(size: Block<u32>, offset: Block<u64>, f: F) -> Result<R, Error>
where
    F: Future<Output = Result<ReadResult<V>, !>> + Send + 'static,
    V: VdevLeafWrite,
    R: From<ScrubResult>,
{
    let ReadResult {
        data,
        failed_disks,
        inner,
    } = await!(f)?;
    inner.stats.read.fetch_add(size.as_u64(), Ordering::Relaxed);

    let data = match data {
        Some(data) => data,
        None => {
            inner
                .stats
                .failed_reads
                .fetch_add(size.as_u64(), Ordering::Relaxed);
            bail!(ErrorKind::ReadError(inner.id.clone()));
        }
    };
    let faulted = failed_disks.len() as u32;
    let repair: Vec<_> = failed_disks
        .into_iter()
        .map(|idx| {
            inner.vdevs[idx]
                .write_raw(data.clone(), offset, true)
                .wrap_unfailable_result()
        }).collect();
    let mut total_repaired = 0;
    for write_result in await!(join_all(repair))? {
        if write_result.is_err() {
            // TODO
        } else {
            total_repaired += 1;
        }
    }
    inner
        .stats
        .repaired
        .fetch_add(u64::from(total_repaired) * size.as_u64(), Ordering::Relaxed);
    Ok(ScrubResult {
        data,
        faulted: size * faulted,
        repaired: size * total_repaired,
    }.into())
}

impl<C: Checksum, V: Vdev + VdevRead<C> + VdevLeafRead<Box<[u8]>> + VdevLeafWrite> VdevRead<C>
    for Mirror<V>
{
    type Read = FutureObj<'static, Result<Box<[u8]>, Error>>;
    type Scrub = FutureObj<'static, Result<ScrubResult, Error>>;
    type ReadRaw = Box<Future<Output = Result<Vec<Box<[u8]>>, Error>> + Send + 'static>;

    fn read(&self, size: Block<u32>, offset: Block<u64>, checksum: C) -> Self::Read {
        let inner = Arc::clone(&self.inner);
        let f = move || {
            // Switch disk every 32 MiB. (which is 2^25 bytes)
            // TODO 32 MiB too large?
            let start_idx = (offset.to_bytes() >> 25) as usize % inner.vdevs.len();
            let mut failed_disks = Vec::new();
            let mut data = None;
            let disk_cnt = inner.vdevs.len();
            for idx in 0..disk_cnt {
                let idx = (idx + start_idx) % inner.vdevs.len();
                let f = inner.vdevs[idx].read(size, offset, checksum.clone());
                match await!(f) {
                    Ok(x) => {
                        data = Some(x);
                        break;
                    }
                    Err(_) => failed_disks.push(idx),
                }
            }
            Ok(ReadResult {
                data,
                failed_disks,
                inner,
            })
        };
        FutureObj::new(Box::new(handle_repair(size, offset, from_generator(f))))
    }

    fn scrub(&self, size: Block<u32>, offset: Block<u64>, checksum: C) -> Self::Scrub {
        let inner = Arc::clone(&self.inner);
        let f = move || {
            let futures: Vec<_> = inner
                .vdevs
                .iter()
                .map(|disk| {
                    disk.read(size, offset, checksum.clone())
                        .wrap_unfailable_result()
                }).collect();
            let mut data = None;
            let mut failed_disks = Vec::new();
            for (idx, result) in await!(join_all(futures))?.into_iter().enumerate() {
                match result {
                    Ok(x) => data = Some(x),
                    Err(_) => failed_disks.push(idx),
                }
            }
            Ok(ReadResult {
                data,
                failed_disks,
                inner,
            })
        };
        FutureObj::new(Box::new(handle_repair(size, offset, from_generator(f))))
    }

    fn read_raw(&self, size: Block<u32>, offset: Block<u64>) -> Self::ReadRaw {
        let inner = Arc::clone(&self.inner);
        let futures: Vec<_> = self
            .inner
            .vdevs
            .iter()
            .map(|disk| {
                let data = alloc_uninitialized(size.to_bytes() as usize);
                VdevLeafRead::<Box<[u8]>>::read_raw(disk, data, offset).wrap_unfailable_result()
            }).collect();
        Box::new(join_all(futures).then(move |result| {
            let mut v = Vec::new();
            for r in result.unwrap() {
                if let Ok(x) = r {
                    v.push(x);
                }
            }
            if v.is_empty() {
                bail!(ErrorKind::ReadError(inner.id.clone()))
            } else {
                Ok(v)
            }
        }))
    }
}

impl<V: VdevLeafWrite> VdevWrite for Mirror<V> {
    type Write = FutureObj<'static, Result<(), Error>>;
    type WriteRaw = UnfailableJoinAll<V::WriteRaw, FailedWriteUpdateStats<V>>;

    fn write(&self, data: Box<[u8]>, offset: Block<u64>) -> Self::Write {
        let inner = Arc::clone(&self.inner);
        let f = move || {
            let size = Block::from_bytes(data.len() as u32);
            let data = SplittableBuffer::new(data);
            inner
                .stats
                .written
                .fetch_add(size.as_u64(), Ordering::Relaxed);
            let futures: Vec<_> = inner
                .vdevs
                .iter()
                .map(|disk| {
                    disk.write_raw(data.clone(), offset, false)
                        .wrap_unfailable_result()
                }).collect();
            let results = await!(join_all(futures))?;
            let total_writes = results.len();
            let mut failed_writes = 0;
            for result in results {
                failed_writes += result.is_err() as usize;
            }
            if failed_writes < total_writes {
                Ok(())
            } else {
                inner
                    .stats
                    .failed_writes
                    .fetch_add(size.as_u64(), Ordering::Relaxed);
                bail!(ErrorKind::WriteError(inner.id.clone()))
            }
        };
        FutureObj::new(Box::new(from_generator(f)))
    }

    fn flush(&self) -> Result<(), Error> {
        for vdev in self.inner.vdevs.iter() {
            vdev.flush()?;
        }
        Ok(())
    }

    fn write_raw(&self, data: Box<[u8]>, offset: Block<u64>) -> Self::WriteRaw {
        let data = SplittableBuffer::new(data);
        let futures = self
            .inner
            .vdevs
            .iter()
            .map(|disk| {
                disk.write_raw(data.clone(), offset, false)
                    .wrap_unfailable_result()
            }).collect();
        UnfailableJoinAll::new(
            futures,
            FailedWriteUpdateStats {
                inner: Arc::clone(&self.inner),
                size: Block::from_bytes(data.len() as u32),
            },
        )
    }
}

pub struct FailedWriteUpdateStats<V> {
    inner: Arc<Inner<V>>,
    size: Block<u32>,
}

impl<V> Failed for FailedWriteUpdateStats<V> {
    fn failed(self) {
        self.inner
            .stats
            .failed_writes
            .fetch_add(self.size.as_u64(), Ordering::Relaxed);
    }
}

impl<V: Vdev> Vdev for Mirror<V> {
    fn actual_size(&self, size: Block<u32>) -> Block<u32> {
        // Only correct for leaf vdevs
        size
    }

    fn size(&self) -> Block<u64> {
        self.inner.vdevs.iter().map(Vdev::size).min().unwrap()
    }

    fn num_disks(&self) -> usize {
        self.inner.vdevs.len()
    }

    fn effective_free_size(&self, free_size: Block<u64>) -> Block<u64> {
        // Only correct for leaf vdevs
        free_size
    }
    fn id(&self) -> &str {
        &self.inner.id
    }

    fn stats(&self) -> Statistics {
        self.inner.stats.as_stats()
    }

    fn for_each_child(&self, f: &mut FnMut(&Vdev)) {
        for vdev in self.inner.vdevs.iter() {
            f(vdev);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Mirror;
    use checksum::{Builder, Checksum, State, XxHashBuilder};
    use futures::executor::block_on;
    use quickcheck::TestResult;
    use vdev::test::{generate_data, test_writes_are_persistent, FailingLeafVdev, FailureMode};
    use vdev::{Block, Vdev, VdevRead, VdevWrite};

    fn build_mirror_vdev(
        disk_size: Block<u32>,
        num_disks: u8,
    ) -> Result<Mirror<FailingLeafVdev>, TestResult> {
        if num_disks < 2 || disk_size == Block(0) {
            return Err(TestResult::discard());
        }
        let disks: Vec<_> = (0..num_disks)
            .map(|id| FailingLeafVdev::new(disk_size, format!("{}", id)))
            .collect();
        let vdev = Mirror::new(disks.into_boxed_slice(), String::from("mirror"));
        Ok(vdev)
    }

    #[quickcheck]
    fn size(disk_size: u8, num_disks: u8) -> TestResult {
        let disk_size = Block(disk_size as u32);
        let vdev = try_ret!(build_mirror_vdev(disk_size, num_disks));
        TestResult::from_bool(vdev.size() == Block::<u64>::from(disk_size))
    }

    #[quickcheck]
    fn effective_free_size(disk_size: u8, num_disks: u8) -> TestResult {
        let disk_size = Block(disk_size as u32);
        let vdev = try_ret!(build_mirror_vdev(disk_size, num_disks));
        TestResult::from_bool(
            vdev.effective_free_size(vdev.size()) == Block::<u64>::from(disk_size),
        )
    }

    #[quickcheck]
    fn actual_size(block_size: u8, num_disks: u8) -> TestResult {
        let vdev = try_ret!(build_mirror_vdev(Block(10), num_disks));

        let block_size = Block(block_size as u32);
        TestResult::from_bool(vdev.actual_size(block_size) == block_size)
    }

    #[quickcheck]
    fn writes_without_failure(writes: Vec<(u8, u8)>, num_disks: u8) -> TestResult {
        let vdev = try_ret!(build_mirror_vdev(Block(256), num_disks));
        test_writes_are_persistent(&writes, &vdev);
        TestResult::passed()
    }

    #[quickcheck]
    fn writes_with_failing_write(
        writes: Vec<(u8, u8)>,
        num_disks: u8,
        non_failing_disk_idx: u8,
        failure_mode: FailureMode,
    ) -> TestResult {
        if num_disks < 2 || failure_mode == FailureMode::NoFail {
            return TestResult::discard();
        }
        let non_failing_disk_idx = non_failing_disk_idx % num_disks;
        let disks: Vec<_> = (0..num_disks)
            .map(|id| FailingLeafVdev::new(Block(256), format!("{}", id)))
            .collect();
        for (idx, disk) in disks.iter().enumerate() {
            if idx != non_failing_disk_idx as usize {
                disk.fail_writes(failure_mode);
            }
        }
        let vdev = Mirror::new(disks.into_boxed_slice(), String::from("mirror"));
        test_writes_are_persistent(&writes, &vdev);
        TestResult::passed()
    }

    #[quickcheck]
    fn writes_with_failing_read(
        writes: Vec<(u8, u8)>,
        num_disks: u8,
        non_failing_disk_idx: u8,
        failure_mode: FailureMode,
    ) -> TestResult {
        if num_disks < 2 || failure_mode == FailureMode::NoFail {
            return TestResult::discard();
        }
        let non_failing_disk_idx = non_failing_disk_idx % num_disks;
        let disks: Vec<_> = (0..num_disks)
            .map(|id| FailingLeafVdev::new(Block(256), format!("{}", id)))
            .collect();
        for (idx, disk) in disks.iter().enumerate() {
            if idx != non_failing_disk_idx as usize {
                disk.fail_reads(failure_mode);
            }
        }
        let vdev = Mirror::new(disks.into_boxed_slice(), String::from("mirror"));
        test_writes_are_persistent(&writes, &vdev);
        TestResult::passed()
    }

    #[quickcheck]
    fn writes_with_failing_read_and_write(
        writes: Vec<(u8, u8)>,
        num_disks: u8,
        non_failing_disk_idx: u8,
        failure_mode: FailureMode,
    ) -> TestResult {
        if num_disks < 3 || failure_mode == FailureMode::NoFail {
            return TestResult::discard();
        }
        let non_failing_disk_idx = non_failing_disk_idx % num_disks;
        let disks: Vec<_> = (0..num_disks)
            .map(|id| FailingLeafVdev::new(Block(256), format!("{}", id)))
            .collect();
        for (idx, disk) in disks.iter().enumerate() {
            if idx != non_failing_disk_idx as usize {
                disk.fail_reads(failure_mode);
                disk.fail_writes(failure_mode);
            }
        }
        let vdev = Mirror::new(disks.into_boxed_slice(), String::from("mirror"));
        test_writes_are_persistent(&writes, &vdev);
        TestResult::passed()
    }

    #[test]
    fn writes_fail_with_all_failing_disks() {
        let disks: Vec<_> = (0..10)
            .map(|id| FailingLeafVdev::new(Block(256), format!("{}", id)))
            .collect();
        let data = vec![1; Block(1u32).to_bytes() as usize].into_boxed_slice();

        for disk in &disks {
            disk.fail_writes(FailureMode::FailOperation);
        }
        let vdev = Mirror::new(disks.into_boxed_slice(), String::from("mirror"));
        assert!(block_on(vdev.write(data, Block(0))).is_err());
    }

    #[quickcheck]
    fn scrub_detects_bad_data_and_repairs_data(
        writes: Vec<(u8, u8)>,
        num_disks: u8,
        write_non_failing_disk_idx: u8,
        write_failure_mode: FailureMode,
        read_non_failing_disk_idx: u8,
        read_failure_mode: FailureMode,
    ) -> TestResult {
        if num_disks < 3 || write_failure_mode == FailureMode::NoFail {
            return TestResult::discard();
        }
        let write_non_failing_disk_idx = (write_non_failing_disk_idx % num_disks) as usize;
        let read_non_failing_disk_idx = (read_non_failing_disk_idx % num_disks) as usize;

        let disks: Vec<_> = (0..num_disks)
            .map(|id| FailingLeafVdev::new(Block(256), format!("{}", id)))
            .collect();
        let vdev = Mirror::new(disks.into_boxed_slice(), String::from("mirror"));

        for (idx, &(offset, size)) in writes.iter().enumerate() {
            let offset = Block(offset as u64);
            let size = Block(size as u32);

            for idx in 0..num_disks as usize {
                if idx != write_non_failing_disk_idx {
                    vdev.inner.vdevs[idx].fail_writes(write_failure_mode);
                }
            }
            let data = generate_data(idx, offset, size);
            let checksum = {
                let mut state = XxHashBuilder.build();
                state.ingest(&data);
                state.finish()
            };
            assert!(block_on(vdev.write(data, offset)).is_ok());

            let scrub_result = block_on(vdev.scrub(size, offset, checksum)).unwrap();
            assert!(checksum.verify(&scrub_result.data).is_ok());
            let faulted_blocks = scrub_result.faulted;
            if write_failure_mode == FailureMode::FailOperation {
                assert_eq!(scrub_result.repaired, Block(0));
            }

            block_on(vdev.read(size, offset, checksum)).unwrap();

            for idx in 0..num_disks as usize {
                vdev.inner.vdevs[idx].fail_writes(FailureMode::NoFail);
            }

            let scrub_result = block_on(vdev.scrub(size, offset, checksum)).unwrap();
            assert!(checksum.verify(&scrub_result.data).is_ok());
            assert_eq!(scrub_result.faulted, faulted_blocks);
            assert_eq!(scrub_result.repaired, faulted_blocks);

            for idx in 0..num_disks as usize {
                if idx != read_non_failing_disk_idx {
                    vdev.inner.vdevs[idx].fail_reads(read_failure_mode);
                }
            }

            block_on(vdev.read(size, offset, checksum)).unwrap();

            for idx in 0..num_disks as usize {
                vdev.inner.vdevs[idx].fail_reads(FailureMode::NoFail);
            }
        }
        TestResult::passed()
    }
}

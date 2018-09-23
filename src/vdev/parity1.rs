use super::errors::*;
use super::util::*;
use super::{
    AtomicStatistics, Block, ScrubResult, Statistics, Vdev, VdevLeafRead, VdevLeafWrite, VdevRead,
    VdevWrite, BLOCK_SIZE,
};
use buffer::{new_buffer, SplittableBuffer, SplittableMutBuffer};
use checksum::Checksum;
use futures::future::{join_all, Future};
use futures::prelude::*;
use std::iter::{once, repeat};
use std::sync::atomic::Ordering;
use std::sync::Arc;

/// This `vdev` will generate parity data and stripe all data to its child
/// vdevs.
pub struct Parity1<V> {
    inner: Arc<Inner<V>>,
}

impl<V> Parity1<V> {
    /// Constructs a new `Parity1` vdev with the given child `vdevs` and `id`.
    /// Note: `vdevs.len()` must be at least 3.
    pub fn new(vdevs: Box<[V]>, id: String) -> Self {
        assert!(vdevs.len() >= 3);
        Parity1 {
            inner: Arc::new(Inner {
                vdevs,
                id,
                stats: Default::default(),
            }),
        }
    }
}

struct Inner<V> {
    vdevs: Box<[V]>,
    id: String,
    stats: AtomicStatistics,
}

impl<V> Inner<V> {
    /// The length in blocks of the long columns for a given request.
    fn long_col_len(&self, size: Block<u32>) -> Block<u32> {
        if size == Block(0) {
            Block(0)
        } else {
            (size - 1) / (self.vdevs.len() as u32 - 1) + 1
        }
    }
    /// The number of the long columns for a given request.
    fn long_col_cnt(&self, size: Block<u32>) -> usize {
        let disk_cnt = self.vdevs.len();
        if size == Block(0) {
            disk_cnt - 1
        } else {
            let cnt = size.as_u64() as usize % (disk_cnt - 1);
            if cnt == 0 {
                disk_cnt - 1
            } else {
                cnt
            }
        }
    }
}

impl<V: Vdev + VdevLeafRead<SplittableMutBuffer> + VdevLeafWrite> Vdev for Parity1<V> {
    fn actual_size(&self, size: Block<u32>) -> Block<u32> {
        size + self.inner.long_col_len(size)
    }

    fn num_disks(&self) -> usize {
        self.inner.vdevs.len()
    }

    fn size(&self) -> Block<u64> {
        let min_size = self.inner.vdevs.iter().map(Vdev::size).min().unwrap();
        min_size * (self.inner.vdevs.len() as u64)
    }

    fn effective_free_size(&self, free_size: Block<u64>) -> Block<u64> {
        let cnt = self.inner.vdevs.len() as u64;
        free_size * (cnt - 1) / cnt
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

impl<
        V: VdevLeafRead<SplittableMutBuffer> + VdevLeafRead<Box<[u8]>> + VdevLeafWrite,
        C: Checksum,
    > VdevRead<C> for Parity1<V>
{
    type Read = Box<Future<Item = Box<[u8]>, Error = Error> + Send>;
    type Scrub = Box<Future<Item = ScrubResult, Error = Error> + Send>;
    type ReadRaw = Box<Future<Item = Vec<Box<[u8]>>, Error = Error> + Send>;

    fn read(&self, size: Block<u32>, offset: Block<u64>, checksum: C) -> Self::Read {
        read(Arc::clone(&self.inner), size, offset, checksum, false)
    }

    fn scrub(&self, size: Block<u32>, offset: Block<u64>, checksum: C) -> Self::Scrub {
        read(Arc::clone(&self.inner), size, offset, checksum, true)
    }

    fn read_raw(&self, size: Block<u32>, offset: Block<u64>) -> Self::ReadRaw {
        let inner = Arc::clone(&self.inner);
        let futures: Vec<_> = self
            .inner
            .vdevs
            .iter()
            .map(|disk| {
                let data = alloc_uninitialized(size.to_bytes() as usize);
                disk.read_raw(data, offset).wrap_unfailable_result()
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

fn read<V, R, C>(
    inner: Arc<Inner<V>>,
    size: Block<u32>,
    offset: Block<u64>,
    checksum: C,
    scrub: bool,
) -> Result<R, Error>
where
    V: VdevLeafRead<SplittableMutBuffer> + VdevLeafRead<Box<[u8]>> + VdevLeafWrite,
    R: From<ScrubResult>,
    C: Checksum,
{
    let disk_cnt = inner.vdevs.len();

    inner.stats.read.fetch_add(size.as_u64(), Ordering::Relaxed);

    let long_col_len = inner.long_col_len(size);
    let long_col_cnt = inner.long_col_cnt(size);
    let parity_disk_offset = offset / (disk_cnt as u64);
    let parity_disk_idx = (offset.as_u64() % (disk_cnt as u64)) as usize;

    let (buf_handle, mut buf) = new_buffer(vec![0; size.to_bytes() as usize].into_boxed_slice());

    let mut reads = Vec::with_capacity(disk_cnt - 1);
    {
        for ((disk, disk_offset), col_length) in disk_iter(
            &inner.vdevs,
            parity_disk_idx,
            parity_disk_offset,
        ).zip(col_length_sequence(long_col_len, long_col_cnt, disk_cnt))
        {
            if col_length == Block(0) {
                break;
            }
            reads.push(
                disk.read_raw(buf.split_off(col_length.to_bytes() as usize), disk_offset)
                    .wrap_unfailable_result(),
            );
        }
        drop(buf);
    }
    let mut failed_idx = None;
    let mut faulted = Block(0);
    for (idx, result) in await!(join_all(reads))?.into_iter().enumerate() {
        if result.is_err() {
            faulted += calc_col_length(idx, long_col_len, long_col_cnt);
            if failed_idx.is_none() {
                failed_idx = Some(idx);
            } else {
                inner
                    .stats
                    .failed_reads
                    .fetch_add(size.as_u64(), Ordering::Relaxed);
                bail!(ErrorKind::ReadError(inner.id.clone()));
            }
        }
    }
    let mut data = match buf_handle.into_inner() {
        Ok(data) => data,
        Err(_) => unreachable!("Failed to reacquire buffer handle"),
    };
    if failed_idx.is_none() && !scrub && checksum.verify(&data).is_ok() {
        return Ok(ScrubResult {
            data,
            faulted: Block(0),
            repaired: Block(0),
        }.into());
    }

    let parity_future = inner.vdevs[parity_disk_idx].read_raw(
        vec![0; long_col_len.to_bytes() as usize].into_boxed_slice(),
        parity_disk_offset,
    );
    let parity_block = match await!(parity_future) {
        Ok(data) => data,
        Err(_) => if failed_idx.is_some() {
            inner
                .stats
                .failed_reads
                .fetch_add(size.as_u64(), Ordering::Relaxed);
            bail!(ErrorKind::ReadError(inner.id.clone()));
        } else {
            vec![0; long_col_len.to_bytes() as usize].into_boxed_slice()
        },
    };

    if let Some(failed_idx) = failed_idx {
        // Exactly one device failed. Rebuild its data with the parity.

        let disk_cnt = inner.vdevs.len();
        let bad_disk_idx = (parity_disk_idx + 1 + failed_idx) % disk_cnt;
        let bad_block_len = calc_col_length(failed_idx, long_col_len, long_col_cnt);
        let mut repaired_block = Box::from(&parity_block[..bad_block_len.to_bytes() as usize]);
        for (idx, block) in block_iter(long_col_len, long_col_cnt, disk_cnt, &data).enumerate() {
            if idx != failed_idx {
                xor(&mut repaired_block, block);
            }
        }
        let col_off = calc_col_offset(failed_idx, long_col_len, long_col_cnt);
        let start = col_off.to_bytes() as usize;
        let end = start + bad_block_len.to_bytes() as usize;
        data[start..end].copy_from_slice(&repaired_block);

        // We give up if the checksum does not match after rebuild.
        if checksum.verify(&data).is_err() {
            inner
                .stats
                .failed_reads
                .fetch_add(size.as_u64(), Ordering::Relaxed);
            inner
                .stats
                .checksum_errors
                .fetch_add(size.as_u64(), Ordering::Relaxed);
            bail!(ErrorKind::ReadError(inner.id.clone()));
        }

        // Otherwise we try to rewrite the defective data.
        let bad_disk_offset = if parity_disk_idx + 1 + failed_idx < inner.vdevs.len() {
            parity_disk_offset
        } else {
            parity_disk_offset + 1
        };

        let repaired = match await!(inner.vdevs[bad_disk_idx].write_raw(
            repaired_block,
            bad_disk_offset,
            true
        )) {
            Ok(()) => faulted,
            Err(_) => Block(0),
        };
        Ok(ScrubResult {
            data,
            faulted,
            repaired,
        }.into())
    } else if scrub && checksum.verify(&data).is_ok() {
        // We are scrubbing and the data is ok.
        // Verify parity.
        let mut new_parity_block = vec![0; long_col_len.to_bytes() as usize].into_boxed_slice();
        for block in block_iter(long_col_len, long_col_cnt, disk_cnt, &data) {
            xor(&mut new_parity_block, block)
        }
        if new_parity_block == parity_block {
            Ok(ScrubResult {
                data,
                faulted: Block(0),
                repaired: Block(0),
            }.into())
        } else {
            // Rewrite bad parity block.
            let repaired = match await!(inner.vdevs[parity_disk_idx].write_raw(
                new_parity_block,
                parity_disk_offset,
                true
            )) {
                Ok(()) => faulted,
                Err(_) => Block(0),
            };
            Ok(ScrubResult {
                data,
                faulted,
                repaired,
            }.into())
        }
    } else {
        // Every disk returned some data, but the checksum does not match.
        // Let's try combinatorial reconstruction
        let all_xored = {
            let mut all_xored = parity_block.clone();
            for block in block_iter(long_col_len, long_col_cnt, disk_cnt, &data) {
                xor(&mut all_xored, block);
            }
            all_xored
        };
        let mut repaired_block = vec![0; long_col_len.to_bytes() as usize];

        let mut bad_block_idx = None;
        for (idx, block) in block_iter(long_col_len, long_col_cnt, disk_cnt, &data).enumerate() {
            repaired_block.copy_from_slice(&all_xored);
            xor(&mut repaired_block, block);

            let iter1 = block_iter(long_col_len, long_col_cnt, disk_cnt, &data);
            let iter2 = block_iter(long_col_len, long_col_cnt, disk_cnt, &data);
            let iter = iter1
                .take(idx)
                .chain(once(&repaired_block[..block.len()]))
                .chain(iter2.skip(idx + 1));
            if checksum.verify_buffer(iter).is_ok() {
                bad_block_idx = Some(idx);
                break;
            }
        }
        let bad_block_idx = match bad_block_idx {
            None => {
                inner
                    .stats
                    .failed_reads
                    .fetch_add(size.as_u64(), Ordering::Relaxed);
                inner
                    .stats
                    .checksum_errors
                    .fetch_add(size.as_u64(), Ordering::Relaxed);
                bail!(ErrorKind::ReadError(inner.id.clone()))
            }
            Some(bad_block_idx) => bad_block_idx,
        };
        let bad_block_len = calc_col_length(bad_block_idx, long_col_len, long_col_cnt);
        repaired_block.truncate(bad_block_len.to_bytes() as usize);
        let bad_disk_idx = (parity_disk_idx + 1 + bad_block_idx) % inner.vdevs.len();
        let bad_disk_offset = if parity_disk_idx + 1 + bad_block_idx < inner.vdevs.len() {
            parity_disk_offset
        } else {
            parity_disk_offset + 1
        };
        let col_off = calc_col_offset(bad_block_idx, long_col_len, long_col_cnt);
        let start = col_off.to_bytes() as usize;
        let end = start + bad_block_len.to_bytes() as usize;
        data[start..end].copy_from_slice(&repaired_block);

        VdevLeafRead::<SplittableMutBuffer>::checksum_error_occurred(
            &inner.vdevs[bad_disk_idx],
            size,
        );
        let repaired = match await!(inner.vdevs[bad_disk_idx].write_raw(
            repaired_block,
            bad_disk_offset,
            true
        )) {
            Ok(()) => faulted,
            Err(_) => Block(0),
        };
        Ok(ScrubResult {
            data,
            faulted,
            repaired,
        }.into())
    }
}

impl<V: VdevLeafWrite> VdevWrite for Parity1<V> {
    type Write = UnfailableJoinAllPlusOne<V::WriteRaw, FailedWriteUpdateStats<V>>;
    type WriteRaw = UnfailableJoinAll<V::WriteRaw, FailedWriteUpdateStats<V>>;

    fn write(&self, data: Box<[u8]>, offset: Block<u64>) -> Self::Write {
        assert!(data.len() % BLOCK_SIZE == 0);
        let mut data = SplittableBuffer::new(data);
        let size = Block::from_bytes(data.len() as u32);
        let long_col_len = self.inner.long_col_len(size);
        let disk_cnt = self.inner.vdevs.len();
        let long_col_cnt = self.inner.long_col_cnt(size);
        let parity_block = build_parity(&data[..], long_col_len, long_col_cnt, disk_cnt);
        let parity_disk_offset = offset / (disk_cnt as u64);
        let parity_disk_idx = (offset.as_u64() % (disk_cnt as u64)) as usize;
        let mut writes = Vec::with_capacity(disk_cnt);

        writes.push(
            self.inner.vdevs[parity_disk_idx]
                .write_raw(
                    SplittableBuffer::from(parity_block),
                    parity_disk_offset,
                    false,
                ).wrap_unfailable_result(),
        );

        for ((disk, disk_offset), col_length) in
            disk_iter(&self.inner.vdevs, parity_disk_idx, parity_disk_offset)
                .zip(col_length_sequence(long_col_len, long_col_cnt, disk_cnt))
        {
            if col_length == Block(0) {
                break;
            }
            writes.push(
                disk.write_raw(
                    data.split_off(col_length.to_bytes() as usize),
                    disk_offset,
                    false,
                ).wrap_unfailable_result(),
            );
        }
        UnfailableJoinAllPlusOne::new(
            writes,
            FailedWriteUpdateStats {
                inner: Arc::clone(&self.inner),
                size,
            },
        )
    }

    fn flush(&self) -> Result<(), Error> {
        for vdev in self.inner.vdevs.iter() {
            vdev.flush()?;
        }
        Ok(())
    }

    fn write_raw(&self, data: Box<[u8]>, offset: Block<u64>) -> Self::WriteRaw {
        let futures = self
            .inner
            .vdevs
            .iter()
            .map(|v| v.write(data.clone(), offset).wrap_unfailable_result())
            .collect();
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

fn build_parity(
    mut data: &[u8],
    long_col_len: Block<u32>,
    long_col_cnt: usize,
    disk_cnt: usize,
) -> Box<[u8]> {
    let mut parity_block = vec![0; long_col_len.to_bytes() as usize].into_boxed_slice();

    for col_len in col_length_sequence(long_col_len, long_col_cnt, disk_cnt) {
        if col_len == Block(0) {
            break;
        }
        let (cur_col, rest) = data.split_at(col_len.to_bytes() as usize);
        xor(&mut parity_block, cur_col);
        data = rest;
    }
    parity_block
}

fn xor(d: &mut [u8], s: &[u8]) {
    for (d_b, &s_b) in d.iter_mut().zip(s) {
        *d_b ^= s_b;
    }
}

fn calc_col_length(col_idx: usize, long_col_len: Block<u32>, long_col_cnt: usize) -> Block<u32> {
    if col_idx < long_col_cnt {
        long_col_len
    } else {
        long_col_len - 1
    }
}

fn calc_col_offset(col_idx: usize, long_col_len: Block<u32>, long_col_cnt: usize) -> Block<u32> {
    if col_idx <= long_col_cnt {
        long_col_len * (col_idx as u32)
    } else {
        long_col_len * (long_col_cnt as u32)
            + (long_col_len - 1) * ((col_idx - long_col_cnt) as u32)
    }
}

fn disk_iter<'a, V: 'a>(
    disks: &'a [V],
    parity_idx: usize,
    parity_disk_offset: Block<u64>,
) -> impl Iterator<Item = (&'a V, Block<u64>)> + 'a {
    disks[parity_idx..]
        .iter()
        .skip(1)
        .zip(repeat(parity_disk_offset))
        .chain(
            disks[..parity_idx]
                .iter()
                .zip(repeat(parity_disk_offset + 1)),
        )
}

fn col_length_sequence(
    long_col_len: Block<u32>,
    long_col_cnt: usize,
    disk_cnt: usize,
) -> impl Iterator<Item = Block<u32>> {
    repeat(long_col_len)
        .take(long_col_cnt)
        .chain(repeat(if long_col_len == Block(0) {
            Block(0)
        } else {
            long_col_len - 1
        })).take(disk_cnt - 1)
}

fn block_iter(
    long_col_len: Block<u32>,
    long_col_cnt: usize,
    disk_cnt: usize,
    data: &[u8],
) -> impl Iterator<Item = &[u8]> {
    struct BlockIter<'a, I> {
        iter: I,
        data: &'a [u8],
    }
    impl<'a, I> Iterator for BlockIter<'a, I>
    where
        I: Iterator<Item = Block<u32>>,
    {
        type Item = &'a [u8];
        fn next(&mut self) -> Option<&'a [u8]> {
            match self.iter.next() {
                None => None,
                Some(col_length) => {
                    let (cur, rest) = self.data.split_at(col_length.to_bytes() as usize);
                    self.data = rest;
                    Some(cur)
                }
            }
        }
    }
    BlockIter {
        iter: col_length_sequence(long_col_len, long_col_cnt, disk_cnt),
        data,
    }
}

#[cfg(test)]
mod tests {
    use super::Parity1;
    use checksum::{Builder, Checksum, State, XxHashBuilder};
    use futures::executor::block_on;
    use quickcheck::TestResult;
    use vdev::test::{generate_data, test_writes_are_persistent, FailingLeafVdev, FailureMode};
    use vdev::{Block, Vdev, VdevRead, VdevWrite};

    fn build_parity_vdev(
        disk_size: Block<u32>,
        num_disks: u8,
    ) -> Result<Parity1<FailingLeafVdev>, TestResult> {
        if num_disks < 3 || disk_size == Block(0) {
            return Err(TestResult::discard());
        }
        let disks: Vec<_> = (0..num_disks)
            .map(|id| FailingLeafVdev::new(disk_size, format!("{}", id)))
            .collect();
        let vdev = Parity1::new(disks.into_boxed_slice(), String::from("parity1"));
        Ok(vdev)
    }

    #[quickcheck]
    fn size(disk_size: u8, num_disks: u8) -> TestResult {
        let disk_size = Block(disk_size as u32);
        let vdev = try_ret!(build_parity_vdev(disk_size, num_disks));
        TestResult::from_bool(vdev.size() == Block::<u64>::from(disk_size) * num_disks as u64)
    }

    #[quickcheck]
    fn effective_free_size(disk_size: u8, num_disks: u8) -> TestResult {
        let disk_size = Block(disk_size as u32);
        let vdev = try_ret!(build_parity_vdev(disk_size, num_disks));
        TestResult::from_bool(
            vdev.effective_free_size(vdev.size())
                == Block::<u64>::from(disk_size) * (num_disks as u64 - 1),
        )
    }

    #[quickcheck]
    fn actual_size(block_size: u8, num_disks: u8) -> TestResult {
        let vdev = try_ret!(build_parity_vdev(Block(10), num_disks));

        let parity_block_cnt = if block_size > 0 {
            let mut block_size = block_size;
            let mut parity_block_cnt = Block(1);
            while block_size > num_disks - 1 {
                parity_block_cnt += 1;
                block_size -= num_disks - 1;
            }
            parity_block_cnt
        } else {
            Block(0)
        };
        let block_size = Block(block_size as u32);
        TestResult::from_bool(vdev.actual_size(block_size) == block_size + parity_block_cnt)
    }

    #[quickcheck]
    fn writes_without_failure(writes: Vec<(u8, u8)>, num_disks: u8) -> TestResult {
        let vdev = try_ret!(build_parity_vdev(Block(256), num_disks));
        test_writes_are_persistent(&writes, &vdev);
        TestResult::passed()
    }

    #[quickcheck]
    fn writes_with_failing_write(
        writes: Vec<(u8, u8)>,
        num_disks: u8,
        failing_disk_idx: u8,
        failure_mode: FailureMode,
    ) -> TestResult {
        if num_disks < 3 || failure_mode == FailureMode::NoFail {
            return TestResult::discard();
        }
        let failing_disk_idx = failing_disk_idx % num_disks;
        let disks: Vec<_> = (0..num_disks)
            .map(|id| FailingLeafVdev::new(Block(256), format!("{}", id)))
            .collect();
        disks[failing_disk_idx as usize].fail_writes(failure_mode);
        let vdev = Parity1::new(disks.into_boxed_slice(), String::from("parity1"));
        test_writes_are_persistent(&writes, &vdev);
        TestResult::passed()
    }

    #[quickcheck]
    fn writes_with_failing_read(
        writes: Vec<(u8, u8)>,
        num_disks: u8,
        failing_disk_idx: u8,
        failure_mode: FailureMode,
    ) -> TestResult {
        if num_disks < 3 || failure_mode == FailureMode::NoFail {
            return TestResult::discard();
        }
        let failing_disk_idx = failing_disk_idx % num_disks;
        let disks: Vec<_> = (0..num_disks)
            .map(|id| FailingLeafVdev::new(Block(256), format!("{}", id)))
            .collect();
        disks[failing_disk_idx as usize].fail_reads(failure_mode);
        let vdev = Parity1::new(disks.into_boxed_slice(), String::from("parity1"));
        test_writes_are_persistent(&writes, &vdev);
        TestResult::passed()
    }

    #[quickcheck]
    fn writes_with_failing_read_and_write(
        writes: Vec<(u8, u8)>,
        num_disks: u8,
        failing_disk_idx: u8,
        failure_mode: FailureMode,
    ) -> TestResult {
        if num_disks < 3 || failure_mode == FailureMode::NoFail {
            return TestResult::discard();
        }
        let failing_disk_idx = failing_disk_idx % num_disks;
        let disks: Vec<_> = (0..num_disks)
            .map(|id| FailingLeafVdev::new(Block(256), format!("{}", id)))
            .collect();
        disks[failing_disk_idx as usize].fail_reads(failure_mode);
        disks[failing_disk_idx as usize].fail_writes(failure_mode);
        let vdev = Parity1::new(disks.into_boxed_slice(), String::from("parity1"));
        test_writes_are_persistent(&writes, &vdev);
        TestResult::passed()
    }

    #[test]
    fn writes_fail_with_two_failing_disks() {
        let disks: Vec<_> = (0..10)
            .map(|id| FailingLeafVdev::new(Block(256), format!("{}", id)))
            .collect();
        let data = vec![1; Block(1u32).to_bytes() as usize].into_boxed_slice();

        disks[0].fail_writes(FailureMode::FailOperation);
        disks[1].fail_writes(FailureMode::FailOperation);
        let vdev = Parity1::new(disks.into_boxed_slice(), String::from("parity1"));
        assert!(block_on(vdev.write(data, Block(0))).is_err());
    }

    #[quickcheck]
    fn scrub_detects_bad_data_and_repairs_data(
        writes: Vec<(u8, u8)>,
        num_disks: u8,
        write_failing_disk_idx: u8,
        write_failure_mode: FailureMode,
        read_failing_disk_idx: u8,
        read_failure_mode: FailureMode,
    ) -> TestResult {
        if num_disks < 3 || write_failure_mode == FailureMode::NoFail {
            return TestResult::discard();
        }
        let write_failing_disk_idx = (write_failing_disk_idx % num_disks) as usize;
        let read_failing_disk_idx = (read_failing_disk_idx % num_disks) as usize;

        let disks: Vec<_> = (0..num_disks)
            .map(|id| FailingLeafVdev::new(Block(256), format!("{}", id)))
            .collect();
        let vdev = Parity1::new(disks.into_boxed_slice(), String::from("parity1"));

        for (idx, &(offset, size)) in writes.iter().enumerate() {
            let offset = Block(offset as u64);
            let size = Block(size as u32);

            vdev.inner.vdevs[write_failing_disk_idx].fail_writes(write_failure_mode);
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

            vdev.inner.vdevs[write_failing_disk_idx].fail_writes(FailureMode::NoFail);

            let scrub_result = block_on(vdev.scrub(size, offset, checksum)).unwrap();
            assert!(checksum.verify(&scrub_result.data).is_ok());
            assert_eq!(scrub_result.faulted, faulted_blocks);
            assert_eq!(scrub_result.repaired, faulted_blocks);

            vdev.inner.vdevs[read_failing_disk_idx].fail_reads(read_failure_mode);

            block_on(vdev.read(size, offset, checksum)).unwrap();

            vdev.inner.vdevs[read_failing_disk_idx].fail_reads(FailureMode::NoFail);
        }
        TestResult::passed()
    }
}

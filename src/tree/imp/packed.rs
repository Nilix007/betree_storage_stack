use super::leaf::LeafNode;
use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};
use cow_bytes::{CowBytes, SlicedCowBytes};
use size::Size;
use std::cmp;
use std::io::{self, Write};
use std::mem::{align_of, size_of};
use std::slice::{from_raw_parts, from_raw_parts_mut};

/// Layout:
///     header: Header,
///     keys: [Data; header.entry_count],
///     values: [Data; header.entry_count],
///     data: [u8],
/// Header:
///     entry_count: u32,
/// Data:
///     pos: u32,
///     len: u32,
#[derive(Debug)]
pub struct PackedMap {
    data: CowBytes,
    entry_count: u32,
}

#[derive(Debug, Copy, Clone)]
#[repr(packed)]
struct Header {
    entry_count: u32,
}

#[derive(Debug, Copy, Clone)]
#[repr(packed)]
struct Data {
    pos: u32,
    len: u32,
}

fn ref_u32_from_bytes_mut(p: &mut [u8]) -> &mut [u32] {
    assert!(p.len() % size_of::<u32>() == 0);
    assert!(p.as_ptr() as usize % align_of::<[u32; 1]>() == 0);
    unsafe { from_raw_parts_mut(p.as_mut_ptr() as *mut u32, p.len() / 4) }
}

fn prefix_size(entry_count: u32) -> usize {
    size_of::<Header>() + 2 * size_of::<Data>() * entry_count as usize
}

impl PackedMap {
    pub fn new(mut data: Vec<u8>) -> Self {
        assert!(data.len() >= 4);
        let entry_count = LittleEndian::read_u32(&data[..4]);
        let total_bytes = prefix_size(entry_count);
        {
            let s = ref_u32_from_bytes_mut(&mut data[..total_bytes]);
            LittleEndian::from_slice_u32(s);
        }
        PackedMap {
            data: data.into(),
            entry_count,
        }
    }

    fn keys(&self) -> &[Data] {
        unsafe {
            let ptr = self.data.as_ptr();
            let start = ptr.offset(size_of::<Header>() as isize) as *const Data;
            from_raw_parts(start, self.entry_count as usize)
        }
    }

    fn values(&self) -> &[Data] {
        unsafe {
            let ptr = self.data.as_ptr();
            let start = ptr.offset(
                (size_of::<Header>() + size_of::<Data>() * self.entry_count as usize) as isize,
            ) as *const Data;
            from_raw_parts(start, self.entry_count as usize)
        }
    }

    fn get_slice(&self, data: Data) -> &[u8] {
        let pos = data.pos as usize;
        let len = data.len as usize;
        &self.data[pos..pos + len]
    }

    fn get_slice_cow(&self, data: Data) -> SlicedCowBytes {
        self.data.clone().slice(data.pos, data.len)
    }

    pub fn get(&self, key: &[u8]) -> Option<SlicedCowBytes> {
        let result = binary_search_by(self.keys(), |&data| self.get_slice(data).cmp(key));
        let idx = match result {
            Err(_) => return None,
            Ok(idx) => idx,
        };

        Some(self.get_slice_cow(self.values()[idx]))
    }

    pub fn get_all<'a>(&'a self) -> Box<Iterator<Item = (&'a [u8], SlicedCowBytes)> + 'a> {
        Box::new(
            self.keys()
                .iter()
                .zip(self.values())
                .map(move |(&key, &value)| (self.get_slice(key), self.get_slice_cow(value))),
        )
    }

    pub(super) fn unpack_leaf(&self) -> LeafNode {
        self.get_all().collect()
    }

    pub(super) fn pack<W: Write>(leaf: &LeafNode, mut writer: W) -> io::Result<()> {
        let entries = leaf.entries();
        let entries_cnt = entries.len() as u32;
        writer.write_u32::<LittleEndian>(entries_cnt)?;
        let mut pos = prefix_size(entries_cnt) as u32;
        for key in entries.keys() {
            writer.write_u32::<LittleEndian>(pos)?;
            let len = key.len() as u32;
            writer.write_u32::<LittleEndian>(len)?;
            pos += len;
        }

        for value in entries.values() {
            writer.write_u32::<LittleEndian>(pos)?;
            let len = value.len() as u32;
            writer.write_u32::<LittleEndian>(len)?;
            pos += len;
        }
        for key in entries.keys() {
            writer.write_all(key)?;
        }
        for value in entries.values() {
            writer.write_all(value)?;
        }
        Ok(())
    }

    pub(super) fn inner(&self) -> &CowBytes {
        &self.data
    }
}

fn binary_search_by<'a, T, F>(s: &'a [T], mut f: F) -> Result<usize, usize>
where
    F: FnMut(&'a T) -> cmp::Ordering,
{
    use std::cmp::Ordering::*;
    let mut size = s.len();
    if size == 0 {
        return Err(0);
    }
    let mut base = 0usize;
    while size > 1 {
        let half = size / 2;
        let mid = base + half;
        let cmp = f(&s[mid]);
        base = if cmp == Greater { base } else { mid };
        size -= half;
    }
    let cmp = f(&s[base]);
    if cmp == Equal {
        Ok(base)
    } else {
        Err(base + (cmp == Less) as usize)
    }
}

impl Size for PackedMap {
    fn size(&self) -> usize {
        self.data.len()
    }
}

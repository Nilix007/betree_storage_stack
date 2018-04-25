use super::errors::*;
use bincode::{deserialize, serialize_into};
use checksum::{Builder, State, XxHash, XxHashBuilder};
use serde::Serialize;
use serde::de::DeserializeOwned;
use size::StaticSize;
use storage_pool::StoragePoolLayer;
use vdev::{Block, BLOCK_SIZE};

static MAGIC: &[u8] = b"HEAFSv3\0\n";

#[derive(Serialize, Deserialize)]
pub struct Superblock<P> {
    magic: [u8; 9],
    root_ptr: P,
}

fn checksum(b: &[u8]) -> XxHash {
    let mut state = XxHashBuilder.build();
    state.ingest(b);
    state.finish()
}

impl<P: DeserializeOwned> Superblock<P> {
    pub fn unpack(b: &[u8]) -> Result<P> {
        let checksum_size = XxHash::size();
        let correct_checksum = checksum(&b[..b.len() - checksum_size]);
        let actual_checksum = deserialize(&b[b.len() - checksum_size..])?;
        if correct_checksum != actual_checksum {
            bail!("Invalid checksum");
        }
        let this: Self = deserialize(b)?;
        if this.magic != MAGIC {
            bail!("Invalid magic");
        }
        Ok(this.root_ptr)
    }
}

impl Superblock<super::ObjectPointer> {
    pub fn fetch_superblocks<S: StoragePoolLayer>(pool: &S) -> Option<super::ObjectPointer> {
        let v1 = pool.read_raw(Block(1), Block(0));
        let v2 = pool.read_raw(Block(1), Block(1));
        v1.into_iter()
            .chain(v2)
            .filter_map(|sb_data| Self::unpack(&sb_data).ok())
            .max_by_key(|ptr| ptr.generation())
    }

    pub fn write_superblock<S: StoragePoolLayer>(
        pool: &S,
        ptr: &super::ObjectPointer,
    ) -> Result<()> {
        let sb_data = Self::pack(ptr)?;
        let sb_offset = if ptr.generation().0 & 1 == 0 {
            Block(0)
        } else {
            Block(1)
        };
        pool.write_raw(sb_data, sb_offset)?;
        Ok(())
    }

    pub fn clear_superblock<S: StoragePoolLayer>(pool: &S) -> Result<()> {
        let empty_data = vec![0; BLOCK_SIZE].into_boxed_slice();
        pool.write_raw(empty_data.clone(), Block(0))?;
        pool.write_raw(empty_data, Block(1))?;
        Ok(())
    }
}

impl<P: Serialize> Superblock<P> {
    pub fn pack(p: &P) -> Result<Box<[u8]>> {
        let mut data = Vec::with_capacity(BLOCK_SIZE);
        {
            let mut this = Superblock {
                magic: [0; 9],
                root_ptr: p,
            };
            this.magic.copy_from_slice(MAGIC);
            serialize_into(&mut data, &this)?;
        }
        let checksum_size = XxHash::size();
        data.resize(BLOCK_SIZE - checksum_size, 0);
        let checksum = checksum(&data);
        serialize_into(&mut data, &checksum)?;
        assert!(data.len() == BLOCK_SIZE);
        Ok(data.into_boxed_slice())
    }
}

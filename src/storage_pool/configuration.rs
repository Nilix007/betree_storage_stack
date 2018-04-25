//! Storage Pool configuration.
use checksum::Checksum;
use futures_cpupool::CpuPool;
use itertools::Itertools;
use libc;
use ref_slice::ref_slice;
use std::fmt;
use std::fmt::Write;
use std::fs::OpenOptions;
use std::io;
use std::iter::FromIterator;
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use vdev::{self, Vdev as VdevTrait, VdevBoxed};

/// `Configuration` type for `StoragePoolUnit`.
#[derive(Debug, Serialize, Deserialize)]
pub struct Configuration {
    top_level_vdevs: Vec<Vdev>,
}

/// Represents a top-level vdev.
#[derive(Debug, Serialize, Deserialize)]
pub enum Vdev {
    /// This vdev is a leaf vdev.
    Leaf(LeafVdev),
    /// This vdev is a mirror vdev.
    Mirror(Vec<LeafVdev>),
    /// Parity1 aka RAID5.
    Parity1(Vec<LeafVdev>),
}

/// Represents a leaf vdev.
#[derive(Debug, Serialize, Deserialize)]
pub struct LeafVdev(PathBuf);

error_chain! {
    errors {
        #[allow(missing_docs)]
        InvalidKeyword
    }
}

impl Configuration {
    /// Returns a new `Configuration` based on the given top-level vdevs.
    pub fn new(top_level_vdevs: Vec<Vdev>) -> Self {
        Configuration { top_level_vdevs }
    }
    /// Opens file and devices and constructs a `Vec<Vdev>`.
    pub fn build<C: Checksum>(&self) -> io::Result<Vec<Box<VdevBoxed<C>>>> {
        self.top_level_vdevs
            .iter()
            .enumerate()
            .map(|(n, v)| v.build(n))
            .collect()
    }

    /// Parses the configuration from a ZFS-like representation.
    ///
    /// This representation is a sequence of top-level vdevs.
    /// The keywords `mirror` and `parity1` signal
    /// that all immediate following devices shall be grouped in such a vdev.
    ///
    /// # Example
    /// `/dev/sda mirror /dev/sdb /dev/sdc parity1 /dev/sdd /dev/sde /dev/sdf`
    /// results in three top-level vdevs: `/dev/sda`, a mirror vdev, and a
    /// parity1 vdev. The mirror vdev contains `/dev/sdb` and `/dev/sdc`.
    /// The parity1 vdev contains `/dev/sdd`, `/dev/sde`, and `/dev/sdf`.
    pub fn parse_zfs_like<I, S>(iter: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut iter = iter.into_iter().peekable();
        let mut v = Vec::new();
        while let Some(s) = iter.next() {
            let s = s.as_ref();
            if is_path(s) {
                v.push(Vdev::Leaf(LeafVdev::from(s)));
                continue;
            }
            let f = match s {
                "mirror" => Vdev::Mirror,
                "parity" | "parity1" => Vdev::Parity1,
                _ => bail!(ErrorKind::InvalidKeyword),
            };
            let leaves = iter.peeking_take_while(is_path)
                .map(|s| LeafVdev::from(s.as_ref()))
                .collect();
            v.push(f(leaves));
        }
        Ok(Configuration { top_level_vdevs: v })
    }

    /// Returns the configuration in a ZFS-like string representation.
    ///
    /// See `parse_zfs_like` for more information.
    pub fn zfs_like(&self) -> String {
        let mut s = String::new();
        for vdev in &self.top_level_vdevs {
            let (keyword, leaves) = match *vdev {
                Vdev::Leaf(ref leaf) => ("", ref_slice(leaf)),
                Vdev::Mirror(ref leaves) => ("mirror ", &leaves[..]),
                Vdev::Parity1(ref leaves) => ("parity1 ", &leaves[..]),
            };
            s.push_str(keyword);
            for leaf in leaves {
                write!(s, "{} ", leaf.0.display()).unwrap();
            }
        }
        s.pop();
        s
    }
}

fn is_path<S: AsRef<str> + ?Sized>(s: &S) -> bool {
    match s.as_ref().chars().next() {
        Some('.') | Some('/') => true,
        _ => false,
    }
}

impl FromIterator<Vdev> for Configuration {
    fn from_iter<T: IntoIterator<Item = Vdev>>(iter: T) -> Self {
        Configuration {
            top_level_vdevs: iter.into_iter().collect(),
        }
    }
}

impl Vdev {
    /// Opens file and devices and constructs a `Vdev`.
    fn build<C: Checksum>(&self, n: usize) -> io::Result<Box<VdevBoxed<C>>> {
        let pool = CpuPool::new(4);
        match *self {
            Vdev::Mirror(ref vec) => {
                let leaves: io::Result<Vec<_>> = vec.iter().map(|leaf| leaf.build(&pool)).collect();
                let leaves = leaves?.into_boxed_slice();
                Ok(Box::new(vdev::Mirror::new(leaves, format!("mirror-{}", n))))
            }
            Vdev::Parity1(ref vec) => {
                let leaves: io::Result<Vec<_>> = vec.iter().map(|leaf| leaf.build(&pool)).collect();
                let leaves = leaves?.into_boxed_slice();
                Ok(Box::new(vdev::Parity1::new(
                    leaves,
                    format!("parity-{}", n),
                )))
            }
            Vdev::Leaf(ref leaf) => leaf.build(&pool).map(VdevTrait::boxed),
        }
    }
}

impl LeafVdev {
    fn build(&self, pool: &CpuPool) -> io::Result<vdev::File> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            // TODO needs some work
            // .custom_flags(libc::O_DIRECT)
            .open(&self.0)?;
        if unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_RANDOM) } != 0 {
            return Err(io::Error::last_os_error());
        }

        let pool = pool.clone();
        Ok(vdev::File::new(
            file,
            pool,
            self.0.to_string_lossy().into_owned(),
        )?)
    }
}

impl<'a> From<&'a str> for LeafVdev {
    fn from(s: &'a str) -> Self {
        LeafVdev(PathBuf::from(s))
    }
}

impl fmt::Display for Configuration {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for vdev in &self.top_level_vdevs {
            vdev.display(0, f)?;
        }
        Ok(())
    }
}

impl Vdev {
    fn display(&self, indent: usize, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Vdev::Leaf(ref leaf) => leaf.display(indent, f),
            Vdev::Mirror(ref mirror) => {
                writeln!(f, "{:indent$}mirror", "", indent = indent)?;
                for vdev in mirror {
                    vdev.display(indent + 4, f)?;
                }
                Ok(())
            }
            Vdev::Parity1(ref parity) => {
                writeln!(f, "{:indent$}parity1", "", indent = indent)?;
                for vdev in parity {
                    vdev.display(indent + 4, f)?;
                }
                Ok(())
            }
        }
    }
}

impl LeafVdev {
    fn display(&self, indent: usize, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "{:indent$}{}", "", &self.0.display(), indent = indent)
    }
}

//! This module provides `CowBytes` which is a Copy-on-Write smart pointer
//! similar to `std::borrow::Cow`.

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use size::Size;
use stable_deref_trait::StableDeref;
use std::borrow::Borrow;
use std::cmp;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

/// Copy-on-Write smart pointer which supports cheap cloning as it is
/// reference-counted.
#[derive(Debug, Clone, Eq, Ord, Default)]
pub struct CowBytes {
    // TODO Replace by own implementation
    pub(super) inner: Arc<Vec<u8>>,
}

impl<T: AsRef<[u8]>> PartialEq<T> for CowBytes {
    fn eq(&self, other: &T) -> bool {
        &**self == other.as_ref()
    }
}

impl<T: AsRef<[u8]>> PartialOrd<T> for CowBytes {
    fn partial_cmp(&self, other: &T) -> Option<cmp::Ordering> {
        (**self).partial_cmp(other.as_ref())
    }
}

impl Serialize for CowBytes {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(self)
    }
}

impl<'de> Deserialize<'de> for CowBytes {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<CowBytes, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::{Error, Visitor};
        use std::fmt;
        struct CowBytesVisitor;

        impl<'de> Visitor<'de> for CowBytesVisitor {
            type Value = CowBytes;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("byte array")
            }

            #[inline]
            fn visit_bytes<E>(self, v: &[u8]) -> Result<CowBytes, E>
            where
                E: Error,
            {
                Ok(CowBytes::from(v))
            }

            #[inline]
            fn visit_str<E>(self, v: &str) -> Result<CowBytes, E>
            where
                E: Error,
            {
                self.visit_bytes(v.as_ref())
            }
        }
        deserializer.deserialize_bytes(CowBytesVisitor)
    }
}

impl Size for CowBytes {
    fn size(&self) -> usize {
        8 + self.inner.len()
    }
}

impl<'a> From<&'a [u8]> for CowBytes {
    fn from(x: &'a [u8]) -> Self {
        CowBytes {
            inner: Arc::new(x.to_vec()),
        }
    }
}

impl From<Box<[u8]>> for CowBytes {
    fn from(x: Box<[u8]>) -> Self {
        CowBytes {
            inner: Arc::new(x.into_vec()),
        }
    }
}

impl From<Vec<u8>> for CowBytes {
    fn from(x: Vec<u8>) -> Self {
        CowBytes { inner: Arc::new(x) }
    }
}

impl Borrow<[u8]> for CowBytes {
    fn borrow(&self) -> &[u8] {
        &*self
    }
}

impl AsRef<[u8]> for CowBytes {
    fn as_ref(&self) -> &[u8] {
        &*self
    }
}

unsafe impl StableDeref for CowBytes {}

impl Deref for CowBytes {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for CowBytes {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut Arc::make_mut(&mut self.inner)[..]
    }
}

impl<'a> IntoIterator for &'a CowBytes {
    type Item = &'a u8;
    type IntoIter = ::std::slice::Iter<'a, u8>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl CowBytes {
    /// Constructs a new, empty `CowBytes`.
    #[inline]
    pub fn new() -> Self {
        CowBytes::default()
    }
    /// Returns the length of the byte buffer.
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns whether this buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Create a new, empty `CowBytes` with the given capacity.
    #[inline]
    pub fn with_capacity(cap: usize) -> Self {
        CowBytes {
            inner: Arc::new(Vec::with_capacity(cap)),
        }
    }

    /// Pushes a byte slice onto the end of the byte buffer.
    #[inline]
    pub fn push_slice(&mut self, v: &[u8]) {
        Arc::make_mut(&mut self.inner).extend_from_slice(v)
    }

    /// Fills the buffer with zeros up to `size`.
    #[inline]
    pub fn fill_zeros_up_to(&mut self, size: usize) {
        if self.len() < size {
            let fill_up = size - self.len();
            let byte = 0;
            self.extend((0..fill_up).map(|_| &byte));
        }
    }

    /// Returns the size (number of bytes) that this object would have
    /// if serialized using `bincode`.
    pub fn size(&self) -> usize {
        8 + self.inner.len()
    }

    /// Returns the underlying data as `Vec<u8>`.
    /// If this object is the only reference to the data,
    /// this functions avoids copying the underlying data.
    pub fn into_vec(self) -> Vec<u8> {
        match Arc::try_unwrap(self.inner) {
            Ok(v) => v,
            Err(this) => Vec::clone(&this),
        }
    }

    /// Returns a `SlicedCowBytes` which points to `self[pos..pos+len]`.
    pub fn slice(self, pos: u32, len: u32) -> SlicedCowBytes {
        SlicedCowBytes::from(self).subslice(pos, len)
    }
}

impl<'a> Extend<&'a u8> for CowBytes {
    fn extend<T: IntoIterator<Item = &'a u8>>(&mut self, iter: T) {
        Arc::make_mut(&mut self.inner).extend(iter)
    }
}

/// Reference-counted pointer which points to a subslice of the referenced data.
#[derive(Debug, Default, Clone)]
pub struct SlicedCowBytes {
    pub(super) data: CowBytes,
    pos: u32,
    len: u32,
}

impl PartialEq for SlicedCowBytes {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

impl Eq for SlicedCowBytes {}

impl Serialize for SlicedCowBytes {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(&**self)
    }
}

impl<'de> Deserialize<'de> for SlicedCowBytes {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        CowBytes::deserialize(deserializer).map(Self::from)
    }
}

impl Size for SlicedCowBytes {
    fn size(&self) -> usize {
        8 + self.len as usize
    }
}

impl SlicedCowBytes {
    /// Returns a new subslice which points to `self[pos..pos+len]`.
    pub fn subslice(self, pos: u32, len: u32) -> Self {
        let pos = self.pos + pos;
        assert!(pos + len <= self.len);
        SlicedCowBytes {
            data: self.data,
            pos,
            len,
        }
    }
}

impl From<CowBytes> for SlicedCowBytes {
    fn from(data: CowBytes) -> Self {
        SlicedCowBytes {
            pos: 0,
            len: data.len() as u32,
            data,
        }
    }
}

impl Deref for SlicedCowBytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        let start = self.pos as usize;
        let end = start + self.len as usize;
        &self.data[start..end]
    }
}

#[cfg(test)]
mod tests {
    use super::{Arc, CowBytes};
    use quickcheck::{Arbitrary, Gen};

    impl Arbitrary for CowBytes {
        fn arbitrary<G: Gen>(g: &mut G) -> Self {
            let len = g.gen_range(0, 128);
            CowBytes {
                inner: Arc::new(g.gen_iter().take(len).collect()),
            }
        }

        fn shrink(&self) -> Box<Iterator<Item = Self>> {
            Box::new(self.inner.shrink().map(|inner| CowBytes { inner }))
        }
    }
}

//! This module provides `Size`, `SizeMut`, and `StaticSize`.
//!
//! These traits are used for serializable objects that knows their serialized
//! size when [`bincode`](../../bincode/index.html) is used.

use parking_lot::RwLock;
use std::collections::BTreeMap;

/// A trait which represents an serializable object
/// that can quickly calculate the size of it's
/// [`bincode`](../../bincode/index.html) representation.
pub trait Size {
    /// Returns the size (number of bytes) that this object would have
    /// if serialized using [`bincode`](../../bincode/index.html).
    fn size(&self) -> usize;
}

/// A trait which represents an serializable object
/// that can quickly calculate the size of it's
/// [`bincode`](../../bincode/index.html) representation.
pub trait SizeMut {
    /// Returns the size (number of bytes) that this object would have
    /// if serialized using [`bincode`](../../bincode/index.html).
    fn size(&mut self) -> usize;
}

/// A trait which represents an serializable object
/// that knows the size of it's
/// [`bincode`](../../bincode/index.html) representation.
pub trait StaticSize {
    /// Returns the size (number of bytes) that an object would have
    /// if serialized using [`bincode`](../../bincode/index.html).
    fn size() -> usize;
}

impl StaticSize for () {
    fn size() -> usize {
        0
    }
}

impl<T: Size> Size for Vec<T> {
    fn size(&self) -> usize {
        8 + self.iter().map(Size::size).sum::<usize>()
    }
}

impl<K: Size, V: Size> Size for BTreeMap<K, V> {
    fn size(&self) -> usize {
        8 + self
            .iter()
            .map(|(key, value)| key.size() + value.size())
            .sum::<usize>()
    }
}

impl<T: Size> SizeMut for T {
    fn size(&mut self) -> usize {
        Size::size(self)
    }
}

impl<T: StaticSize> Size for T {
    fn size(&self) -> usize {
        T::size()
    }
}

impl<T: SizeMut> SizeMut for RwLock<T> {
    fn size(&mut self) -> usize {
        self.get_mut().size()
    }
}

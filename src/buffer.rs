//! This module provides reference counted buffers that can be splitted.

use owning_ref::OwningRef;
use std::mem::replace;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

/// Returns a new splittable, mutable buffer that holds `data`
/// and a `Handle` to recover the data later on.
pub fn new_buffer(data: Box<[u8]>) -> (Handle, SplittableMutBuffer) {
    let x = Arc::new(());
    let data = Box::into_raw(data);

    let handle = Handle {
        x: Arc::clone(&x),
        data,
    };
    let buffer = SplittableMutBuffer {
        x,
        data: unsafe { &mut *data },
    };
    (handle, buffer)
}

/// A handle to `SplittableMutBuffer`s.  Can be used to regain access to the
/// underlying data.
pub struct Handle {
    x: Arc<()>,
    data: *mut [u8],
}

unsafe impl Send for Handle {}

impl Handle {
    /// Tries to recover the inner data.
    /// Fails if there are active `SplittableMutBuffer`s that reference this
    /// data.
    pub fn into_inner(mut self) -> Result<Box<[u8]>, Self> {
        if Arc::get_mut(&mut self.x).is_none() {
            return Err(self);
        }
        unsafe { Ok(Box::from_raw(self.data)) }
    }
}

/// A splittable buffer with mutable access.
pub struct SplittableMutBuffer {
    x: Arc<()>,
    data: &'static mut [u8],
}

impl SplittableMutBuffer {
    /// Splits the buffer at `mid`.
    /// The returned `SplittableMutBuffer` will contain all indices from `[0,
    /// mid)` and this Buffer will contain `[mid, len)`.
    pub fn split_off(&mut self, mid: usize) -> Self {
        let (left, right) = replace(&mut self.data, &mut []).split_at_mut(mid);
        self.data = right;
        SplittableMutBuffer {
            x: Arc::clone(&self.x),
            data: left,
        }
    }
}

impl Deref for SplittableMutBuffer {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        self.data
    }
}

impl DerefMut for SplittableMutBuffer {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }
}

impl AsMut<[u8]> for SplittableMutBuffer {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut *self
    }
}

/// Read-only buffer of bytes that can be split into disjoint parts.
#[derive(Clone)]
pub struct SplittableBuffer {
    inner: OwningRef<Arc<Box<[u8]>>, [u8]>,
}

impl SplittableBuffer {
    /// Constructs a new `SplittableBuffer` which holds the given data.
    pub fn new(b: Box<[u8]>) -> Self {
        Self::from(b)
    }

    /// Splits the buffer into two parts. Afterwards, `self` contains the
    /// elements `[0, at)`
    /// and the returned buffer contains the elements `[at, len)`.
    pub fn split_to(&mut self, at: usize) -> Self {
        let left_part = self.inner.clone().map(|x| &x[..at]);
        let other = replace(&mut self.inner, left_part);
        SplittableBuffer {
            inner: other.map(|x| &x[at..]),
        }
    }

    /// Splits the buffer into two parts. Afterwards, `self` contains the
    /// elements `[at, len)`
    /// and the returned buffer contains the elements `[0, at)`.
    pub fn split_off(&mut self, at: usize) -> Self {
        let left_part = self.inner.clone().map(|x| &x[at..]);
        let other = replace(&mut self.inner, left_part);
        SplittableBuffer {
            inner: other.map(|x| &x[..at]),
        }
    }

    /// Returns the underlying buffer if it has exactly one reference.
    pub fn try_unwrap(self) -> Result<Box<[u8]>, Arc<Box<[u8]>>> {
        Arc::try_unwrap(self.inner.into_owner())
    }
}

impl From<Box<[u8]>> for SplittableBuffer {
    fn from(b: Box<[u8]>) -> Self {
        SplittableBuffer {
            inner: OwningRef::new(Arc::new(b)).map(|x| &x[..]),
        }
    }
}

impl Deref for SplittableBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl AsRef<[u8]> for SplittableBuffer {
    fn as_ref(&self) -> &[u8] {
        &*self
    }
}

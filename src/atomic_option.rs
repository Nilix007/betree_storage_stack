//! This module provides `AtomicOption`, an `Option` which is `Send + Sync`.
//!
//! `AtomicOption` is intialized to the `None` state and can be set to
//! `Some(_)` exactly once.

use parking_lot::Mutex;
use std::cell::UnsafeCell;
use std::mem::{uninitialized, ManuallyDrop};
use std::ptr::write;
use std::sync::atomic::{AtomicBool, Ordering};

/// `AtomicOption` is an `Option` which is `Send + Sync`.
/// It is intialized to the `None` state and can be set to `Some(_)` exactly
/// once.
pub struct AtomicOption<T> {
    initialized: AtomicBool,
    lock: Mutex<()>,
    data: UnsafeCell<ManuallyDrop<T>>,
}

unsafe impl<T: Send + Sync> Send for AtomicOption<T> {}
unsafe impl<T: Send + Sync> Sync for AtomicOption<T> {}

impl<T> Default for AtomicOption<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> AtomicOption<T> {
    /// Returns a new `AtomicOption` which is initialized to `None`.
    pub fn new() -> Self {
        AtomicOption {
            initialized: AtomicBool::new(false),
            lock: Mutex::new(()),
            data: unsafe { UnsafeCell::new(ManuallyDrop::new(uninitialized())) },
        }
    }

    /// Returns an reference to the inner object or `None`.
    pub fn get(&self) -> Option<&T> {
        if self.initialized.load(Ordering::Relaxed) {
            unsafe { Some(&*self.data.get()) }
        } else {
            None
        }
    }

    /// Sets the inner `Option` to `Some(x)`.
    /// Note that this function will panic if called more than once.
    pub fn set(&self, x: T) {
        if self.initialized.load(Ordering::Relaxed) {
            panic!();
        }
        let _guard = self.lock.lock();
        if self.initialized.load(Ordering::Relaxed) {
            panic!();
        }
        unsafe {
            write(&mut **self.data.get(), x);
        }
        self.initialized.store(true, Ordering::Relaxed);
    }
}

impl<T> Drop for AtomicOption<T> {
    fn drop(&mut self) {
        if self.initialized.load(Ordering::Relaxed) {
            unsafe { ManuallyDrop::drop(&mut *self.data.get()) }
        }
    }
}

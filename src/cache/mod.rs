//! This module provides a cache interface and a CLOCK cache implementation.

use stable_deref_trait::StableDeref;
use std::fmt::{Debug, Display};
use std::hash::Hash;

/// Error type of `Cache::change_key`.
pub enum ChangeKeyError<E> {
    /// The requested cache entry was not present in the cache.
    NotPresent,
    /// The requested cache entry was pinned.
    Pinned,
    /// The callback returned an error.
    CallbackError(E),
}

impl<E> From<E> for ChangeKeyError<E> {
    fn from(e: E) -> Self {
        ChangeKeyError::CallbackError(e)
    }
}

/// Error type of `Cache::remove`.
pub enum RemoveError {
    /// The requested cache entry was not present in the cache.
    NotPresent,
    /// The requested cache entry was pinned.
    Pinned,
}

/// Cache that supports
///
/// - pinned entries (short-lived only)
/// - variable sized entries
/// - entries may change their size
///
pub trait Cache: Send + Sync {
    /// The key type for the cache.
    type Key: Eq + Hash;
    /// The cache entry type for the cache.
    type Value;

    /// Constructs a new instance with the given `capacity` in bytes.
    fn new(capacity: usize) -> Self;

    /// The value returned by `get`. Holds a reference to the actual cache
    /// entry.
    type ValueRef: AddSize + StableDeref<Target = Self::Value>;

    /// Returns whether a cache entry is present.
    fn contains_key(&self, key: &Self::Key) -> bool;

    /// Returns a cache entry if present.
    /// The cache entry will be pinned while the return value is in scope.
    /// See `Self::ValueRef` for more information.
    fn get(&self, key: &Self::Key, count_miss: bool) -> Option<Self::ValueRef>;

    /// Removes a cache entry if present and not pinned.
    /// `f` shall return the size of the cache entry in bytes.
    fn remove<F>(&mut self, key: &Self::Key, f: F) -> Result<Self::Value, RemoveError>
    where
        F: FnOnce(&mut Self::Value) -> usize;

    /// Removes a cache entry.
    /// Will forcefully remove entry if pinned.
    /// Returns true iff cache entry was present.
    fn force_remove(&mut self, key: &Self::Key, size: usize) -> bool;

    /// Changes the key of a cache entry if present and not pinned.
    /// `f` provides the new key.
    /// The key will be changed if `f` does not fail.
    /// Returns whether the key was present.
    fn change_key<E, F>(&mut self, key: &Self::Key, f: F) -> Result<(), ChangeKeyError<E>>
    where
        F: FnOnce(&Self::Key, &mut Self::Value, &Fn(&Self::Key) -> bool) -> Result<Self::Key, E>;

    /// Changes the key of a cache entry if present.
    /// Returns whether the key was present.
    fn force_change_key(&mut self, key: &Self::Key, new_key: Self::Key) -> bool;

    /// Evicts a cache entry.
    /// `f` may return `None` if an entry cannot be evicted.
    fn evict<F>(&mut self, f: F) -> Option<(Self::Key, Self::Value)>
    where
        F: FnMut(&Self::Key, &mut Self::Value, &Fn(&Self::Key) -> bool) -> Option<usize>;

    /// Inserts a new cache entry.
    /// The given `key` must not be present beforehand.
    /// This function ignores the capacity of the cache.
    ///
    /// Call `evict` with `size` prior to this
    /// if the cache should not grow beyond the capacity bound.
    fn insert(&mut self, key: Self::Key, value: Self::Value, size: usize);

    /// Returns an iterator that iterates over the cache entry keys in order
    /// from old to new.
    fn iter<'a>(&'a self) -> Box<Iterator<Item = &'a Self::Key> + 'a>;

    /// Returns the total size of all cache entries.
    fn size(&self) -> usize;

    /// Returns the capacity.
    fn capacity(&self) -> usize;

    /// The value returned by `stats`.
    type Stats: Stats;

    /// Returns a struct that holds access statistics.
    fn stats(&self) -> Self::Stats;
}

/// Corresponds to a pinned cache entry.
/// You should use `add_size` if the size of the cache entry changed.
pub trait AddSize: Sized {
    /// Adds the given `size_delta` to the cache's size.
    fn add_size(&self, size_delta: isize);

    /// Convenience wrapper around `add_size` that consumes `self`.
    fn finish(self, size_delta: isize) {
        self.add_size(size_delta);
    }
}

/// Interface to cache statistics.
pub trait Stats: Display + Debug {
    /// Returns the capacity of the cache in bytes.
    fn capacity(&self) -> usize;
    /// Returns the size of the cache in bytes.
    fn size(&self) -> usize;
    /// Returns the number of elements in the cache.
    fn len(&self) -> usize;
    /// Returns the number of cache hits.
    fn hits(&self) -> u64;
    /// Returns the number of cache misses.
    fn misses(&self) -> u64;
    /// Returns the number of insertions.
    fn insertions(&self) -> u64;
    /// Returns the number of evictions.
    fn evictions(&self) -> u64;
    /// Returns the number of removals.
    fn removals(&self) -> u64;
}

mod clock;
mod clock_cache;
pub use self::clock_cache::ClockCache;

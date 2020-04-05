//! This module provides a CLOCK cache implementation.
//!
//! CLOCK is a more efficient implementation of Second Chance which is a 1-bit
//! approximation of LRU.
//! The benefit compared to LRU is the much lower overhead for cache hits
//! as CLOCK does not have to move the cache entry to the MRU position like LRU
//! does.

use super::clock::Clock;
use super::{AddSize, Cache, ChangeKeyError, RemoveError, Stats};
use stable_deref_trait::StableDeref;
use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

/// A clock cache. (1-bit approximation of LRU)
pub struct ClockCache<K, V> {
    map: HashMap<K, Arc<CacheEntry<V>>>,
    clock: Clock<K>,
    capacity: usize,
    // Let's leak it
    size: &'static AtomicUsize,
    hits: AtomicU64,
    misses: AtomicU64,
    insertions: u64,
    evictions: u64,
    removals: u64,
}

struct CacheEntry<V> {
    value: V,
    referenced: AtomicBool,
}

/// Pinned cache entry
pub struct PinnedEntry<V: 'static> {
    size: &'static AtomicUsize,
    entry: Arc<CacheEntry<V>>,
}

impl<V> Deref for PinnedEntry<V> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &self.entry.value
    }
}

unsafe impl<V> StableDeref for PinnedEntry<V> {}

/// Cache statistics
#[derive(Debug, Clone, Copy)]
pub struct CacheStats {
    capacity: usize,
    size: usize,
    len: usize,
    hits: u64,
    misses: u64,
    insertions: u64,
    evictions: u64,
    removals: u64,
}

impl fmt::Display for CacheStats {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let total = self.hits + self.misses;
        write!(
            f,
            r"
STATISTICS:
===
              Size: {s}/{c} ({s_p:.2}% filled)
        Clock size: {c_s:>6}
Average entry size: {avg_e:.2}

  Hits: {h:>8} ({h_p:>6.2}%)
Misses: {m:>8} ({m_p:>6.2}%)

Insertions: {i:>8}
 Evictions: {e:>8}
  Removals: {r:>8}",
            s = self.size,
            c = self.capacity,
            s_p = 100.0 * self.size as f32 / self.capacity as f32,
            c_s = self.len,
            avg_e = self.size as f32 / self.len as f32,
            h = self.hits,
            h_p = 100.0 * self.hits as f32 / total as f32,
            m = self.misses,
            m_p = 100.0 * self.misses as f32 / total as f32,
            i = self.insertions,
            e = self.evictions,
            r = self.removals
        )
    }
}

impl Stats for CacheStats {
    fn capacity(&self) -> usize {
        self.capacity
    }

    fn size(&self) -> usize {
        self.size
    }

    fn len(&self) -> usize {
        self.len
    }

    fn hits(&self) -> u64 {
        self.hits
    }

    fn misses(&self) -> u64 {
        self.misses
    }
    fn insertions(&self) -> u64 {
        self.insertions
    }

    fn evictions(&self) -> u64 {
        self.evictions
    }

    fn removals(&self) -> u64 {
        self.removals
    }
}

impl<V> AddSize for PinnedEntry<V> {
    fn add_size(&self, size_delta: isize) {
        if size_delta >= 0 {
            self.size.fetch_add(size_delta as usize, Ordering::Relaxed);
        } else {
            self.size.fetch_sub(-size_delta as usize, Ordering::Relaxed);
        }
    }
}

fn leak<T>(x: Box<T>) -> &'static T {
    let p = Box::into_raw(x);
    unsafe { &*p }
}

impl<K: Hash + Eq, V> ClockCache<K, V> {
    /// Returns a new cache instance with the given `capacity`.
    pub fn new(capacity: usize) -> Self {
        ClockCache {
            map: Default::default(),
            clock: Default::default(),
            size: leak(Default::default()),
            hits: Default::default(),
            misses: Default::default(),
            capacity,
            insertions: 0,
            evictions: 0,
            removals: 0,
        }
    }
}

impl<K: Clone + Eq + Hash + Sync + Send, V: Sync + Send + 'static> Cache for ClockCache<K, V> {
    type Key = K;
    type Value = V;
    type ValueRef = PinnedEntry<V>;
    type Stats = CacheStats;

    fn new(capacity: usize) -> Self {
        Self::new(capacity)
    }

    fn contains_key(&self, key: &K) -> bool {
        self.map.contains_key(key)
    }

    fn get(&self, key: &K, count_miss: bool) -> Option<Self::ValueRef> {
        if let Some(entry) = self.map.get(key).cloned() {
            self.hits.fetch_add(1, Ordering::Relaxed);
            entry.referenced.store(true, Ordering::Relaxed);
            Some(PinnedEntry {
                size: self.size,
                entry,
            })
        } else {
            if count_miss {
                self.misses.fetch_add(1, Ordering::Relaxed);
            }
            None
        }
    }

    fn remove<F>(&mut self, key: &K, f: F) -> Result<V, RemoveError>
    where
        F: FnOnce(&mut V) -> usize,
    {
        {
            let entry = self.map.get_mut(key).ok_or(RemoveError::NotPresent)?;
            Arc::get_mut(entry).ok_or(RemoveError::Pinned)?;
        }
        self.clock.retain(|entry| entry != key);
        let entry = self.map.remove(key).unwrap();
        let mut value = Arc::try_unwrap(entry).ok().unwrap().value;
        let size = f(&mut value);
        self.removals += 1;
        self.size.fetch_sub(size, Ordering::Relaxed);
        Ok(value)
    }

    fn force_remove(&mut self, key: &Self::Key, size: usize) -> bool {
        self.clock.retain(|entry| entry != key);
        if self.map.remove(key).is_none() {
            return false;
        }
        self.removals += 1;
        self.size.fetch_sub(size, Ordering::Relaxed);
        true
    }

    fn change_key<E, F>(&mut self, key: &K, f: F) -> Result<(), ChangeKeyError<E>>
    where
        F: FnOnce(&K, &mut V, &dyn Fn(&K) -> bool) -> Result<K, E>,
    {
        let new_key = {
            let second_ref: &Self = unsafe { &*(self as *mut _) };
            let entry = self.map.get_mut(key).ok_or(ChangeKeyError::NotPresent)?;
            let entry = Arc::get_mut(entry).ok_or(ChangeKeyError::Pinned)?;
            f(key, &mut entry.value, &|k| second_ref.contains_key(k))?
        };
        let entry = self.map.remove(key).unwrap();
        self.map.insert(new_key.clone(), entry);
        if let Some(entry) = self.clock.iter_mut().find(|entry| *entry == key) {
            *entry = new_key;
        }
        Ok(())
    }

    fn force_change_key(&mut self, key: &Self::Key, new_key: Self::Key) -> bool {
        let entry = match self.map.remove(key) {
            None => return false,
            Some(entry) => entry,
        };
        self.map.insert(new_key.clone(), entry);
        if let Some(entry) = self.clock.iter_mut().find(|entry| *entry == key) {
            *entry = new_key;
        }
        true
    }

    fn evict<F>(&mut self, mut f: F) -> Option<(K, V)>
    where
        F: FnMut(&K, &mut V, &dyn Fn(&K) -> bool) -> Option<usize>,
    {
        let len = self.clock.len();
        let mut cnt = 0;
        loop {
            let eviction_successful = {
                let key = match self.clock.peek_front().cloned() {
                    None => {
                        warn!("Clock size mismatch");
                        break None;
                    }
                    Some(key) => key,
                };

                let second_ref: &Self = unsafe { &*(self as *mut _) };
                let entry = self.map.get_mut(&key).unwrap();

                // An entry will be evicted if the following three conditions are satisfied:
                // - The cache entry is not pinned
                // - The referenced bit of the cache entry is false
                // - The eviction callback signals a successful eviction.

                if let Some(entry) = Arc::get_mut(entry) {
                    // reset reference bit
                    let was_referenced = *entry.referenced.get_mut();
                    *entry.referenced.get_mut() = false;
                    if was_referenced {
                        None
                    } else {
                        f(&key, &mut entry.value, &|k| second_ref.contains_key(k))
                    }
                } else {
                    None
                }
            };
            if let Some(size) = eviction_successful {
                let key = self.clock.pop_front().unwrap();
                let entry = self.map.remove(&key).unwrap();
                self.evictions += 1;
                self.size.fetch_sub(size, Ordering::Relaxed);
                let value = Arc::try_unwrap(entry).ok().unwrap().value;
                break Some((key, value));
            }

            self.clock.next();
            cnt += 1;
            if cnt == 2 * len {
                warn!("Clock eviction failed");
                break None;
            }
        }
    }

    fn insert(&mut self, key: K, value: V, size: usize) {
        let old_value = self.map.insert(
            key.clone(),
            Arc::new(CacheEntry {
                value,
                referenced: AtomicBool::new(false),
            }),
        );
        assert!(old_value.is_none());
        self.clock.push_back(key);
        self.insertions += 1;
        self.size.fetch_add(size, Ordering::Relaxed);
    }

    fn stats(&self) -> Self::Stats {
        CacheStats {
            capacity: self.capacity,
            size: self.size.load(Ordering::Relaxed),
            len: self.map.len(),
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            insertions: self.insertions,
            evictions: self.evictions,
            removals: self.removals,
        }
    }

    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = &'a K> + 'a> {
        Box::new(self.clock.iter())
    }

    fn size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    fn capacity(&self) -> usize {
        self.capacity
    }
}

#[cfg(test)]
mod tests {
    use super::{Cache, ClockCache};
    use bencher::{black_box, Bencher};

    fn get_and_pin(b: &mut Bencher) {
        let mut c = ClockCache::new(5);
        c.insert(5, 5, 1);
        b.iter(|| {
            black_box(c.get(&5, true));
        });
    }

    benchmark_group!(benches, get_and_pin);
    benchmark_main!(benches);
}

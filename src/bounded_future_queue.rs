//! This module provides a bounded queue of futures which are identified by a
//! key.

use futures::stream::FuturesUnordered;
use futures::Stream;
use futures::{Async, Future, IntoFuture, Poll};
use std::borrow::Borrow;
use std::collections::HashSet;
use std::hash::Hash;

struct Helper<K, F> {
    key: Option<K>,
    future: F,
}

impl<K, F> Future for Helper<K, F>
where
    F: Future<Item = ()>,
{
    type Item = K;
    type Error = F::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.future.poll() {
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Ok(Async::Ready(())) => Ok(Async::Ready(self.key.take().unwrap())),
            Err(e) => Err(e),
        }
    }
}

/// A bounded queue of `F`s which are identified by a key `K`.
pub struct BoundedFutureQueue<K, F> {
    map: HashSet<K>,
    queue: FuturesUnordered<Helper<K, F>>,
    limit: usize,
}

impl<K: Eq + Hash, F: Future<Item = ()>> BoundedFutureQueue<K, F> {
    /// Creates a new queue with the given `limit`.
    pub fn new(limit: usize) -> Self {
        BoundedFutureQueue {
            map: HashSet::new(),
            queue: FuturesUnordered::new(),
            limit: limit,
        }
    }

    /// Enqueues a new `Future`. This function will block if the queue is full.
    pub fn enqueue<I>(&mut self, key: K, f: I) -> Result<(), F::Error>
    where
        I: IntoFuture<Future = F, Item = (), Error = F::Error>,
        K: Clone,
    {
        self.wait(&key)?;
        let future = f.into_future();
        self.queue.push(Helper {
            future,
            key: Some(key.clone()),
        });
        self.map.insert(key);
        let limit = self.limit;
        self.drain_while_above_limit(limit)?;
        Ok(())
    }

    /// Returns false if the queue does not contain `key`.
    pub fn may_contains_key(&self, key: &K) -> bool {
        self.map.contains(key)
    }

    /// Waits asynchronously for the given `key`.
    /// May flush the whole queue if a future returned an error beforehand.
    pub fn wait_async<'a, Q: Borrow<K> + 'a>(
        &'a mut self,
        key: Q,
    ) -> impl Future<Item = (), Error = F::Error> + 'a {
        // If `map` does not contain `key`,
        // `key` is not in the queue. By setting found to false,
        // we don't actually wait for any futures in the queue.
        let mut found = !self.map.contains(key.borrow());
        self.drain_while(move |finished_key| {
            if found {
                false
            } else {
                found = finished_key == key.borrow();
                true
            }
        })
    }

    /// Waits for the given `key`.
    /// May flush the whole queue if a future returned an error beforehand.
    pub fn wait(&mut self, key: &K) -> Result<(), F::Error> {
        self.wait_async(key).wait()
    }

    /// Flushes the queue.
    pub fn flush(&mut self) -> Result<(), F::Error> {
        self.map.clear();
        self.queue.by_ref().map(|_| ()).collect().wait().map(|_| ())
    }

    /// Returns true iff the queue's size is at the limit.
    pub fn full(&self) -> bool {
        self.map.len() >= self.limit
    }

    fn drain_while_above_limit(&mut self, limit: usize) -> Result<(), F::Error> {
        if self.map.len() > limit {
            let amount = self.map.len() - limit;
            let result = self.queue.by_ref().take(amount as u64).collect().wait();
            let keys = result?;
            for key in keys {
                self.map.remove(&key);
            }
        }
        Ok(())
    }

    fn drain_while<'a, G: 'a>(
        &'a mut self,
        mut f: G,
    ) -> impl Future<Item = (), Error = F::Error> + 'a
    where
        G: FnMut(&K) -> bool,
    {
        let map = &mut self.map;
        self.queue
            .by_ref()
            .take_while(move |key| Ok(f(key)))
            .for_each(move |key| {
                map.remove(&key);
                Ok(())
            })
    }
}

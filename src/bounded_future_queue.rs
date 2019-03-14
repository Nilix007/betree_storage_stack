//! This module provides a bounded queue of futures which are identified by a
//! key.

use futures::executor::block_on;
use futures::future::{ok, ready, IntoFuture};
use futures::prelude::*;
use futures::stream::FuturesUnordered;
use futures::task::Waker;
use futures::{try_ready, Poll, TryFuture};
use std::borrow::Borrow;
use std::collections::HashSet;
use std::hash::Hash;
use std::pin::Pin;

struct Helper<K, F> {
    key: Option<K>,
    future: F,
}

impl<K, F> TryFuture for Helper<K, F>
where
    F: TryFuture<Ok = ()>,
{
    type Ok = K;
    type Error = F::Error;

    fn try_poll(self: Pin<&mut Self>, waker: &Waker) -> Poll<Result<Self::Ok, Self::Error>> {
        let this = unsafe { Pin::get_unchecked_mut(self) };
        let f = unsafe { Pin::new_unchecked(&mut this.future) };
        try_ready!(f.try_poll(waker));
        Poll::Ready(Ok(this.key.take().unwrap()))
    }
}

/// A bounded queue of `F`s which are identified by a key `K`.
pub struct BoundedFutureQueue<K, F> {
    map: HashSet<K>,
    queue: FuturesUnordered<IntoFuture<Helper<K, F>>>,
    limit: usize,
}

impl<K: Eq + Hash, F: TryFuture<Ok = ()>> BoundedFutureQueue<K, F> {
    /// Creates a new queue with the given `limit`.
    pub fn new(limit: usize) -> Self {
        BoundedFutureQueue {
            map: HashSet::new(),
            queue: FuturesUnordered::new(),
            limit,
        }
    }

    /// Enqueues a new `Future`. This function will block if the queue is full.
    pub fn enqueue(&mut self, key: K, future: F) -> Result<(), F::Error>
    where
        K: Clone,
    {
        self.wait(&key)?;
        self.queue.push(
            Helper {
                future,
                key: Some(key.clone()),
            }
            .into_future(),
        );
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
    ) -> impl TryFuture<Ok = (), Error = F::Error> + 'a {
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
        block_on(self.wait_async(key).into_future())
    }

    /// Flushes the queue.
    pub fn flush(&mut self) -> Result<(), F::Error> {
        self.map.clear();
        block_on(self.queue.by_ref().map_ok(|_| ()).try_collect::<Vec<()>>()).map(|_| ())
    }

    /// Returns true iff the queue's size is at the limit.
    pub fn full(&self) -> bool {
        self.map.len() >= self.limit
    }

    fn drain_while_above_limit(&mut self, limit: usize) -> Result<(), F::Error> {
        if self.map.len() > limit {
            let amount = self.map.len() - limit;
            let keys: Vec<_> = block_on(self.queue.by_ref().take(amount as u64).try_collect())?;
            for key in keys {
                self.map.remove(&key);
            }
        }
        Ok(())
    }

    fn drain_while<'a, G: 'a>(
        &'a mut self,
        mut f: G,
    ) -> impl TryFuture<Ok = (), Error = F::Error> + 'a
    where
        G: FnMut(&K) -> bool,
    {
        let map = &mut self.map;
        self.queue
            .by_ref()
            .take_while(move |key| {
                ready(match key {
                    Ok(key) => f(key),
                    Err(_) => false,
                })
            })
            .try_for_each(move |key| {
                map.remove(&key);
                ok(())
            })
    }
}

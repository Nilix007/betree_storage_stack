use futures::future::{join_all, Future, JoinAll, Then};
use futures::{Async, Poll};
use std::ops::{Generator, GeneratorState};

type WrapUnfailableResultFn<T> = fn(T) -> Result<T, !>;
pub type UnfailableFuture<F> = Then<
    F,
    Result<Result<<F as Future>::Item, <F as Future>::Error>, !>,
    WrapUnfailableResultFn<Result<<F as Future>::Item, <F as Future>::Error>>,
>;

pub trait UnfailableFutureExt: Future + Sized {
    fn wrap_unfailable_result(self) -> UnfailableFuture<Self>;
}

impl<F: Future> UnfailableFutureExt for F {
    fn wrap_unfailable_result(self) -> UnfailableFuture<Self> {
        self.then(Ok as WrapUnfailableResultFn<_>)
    }
}

pub struct UnfailableJoinAll<F: Future, G: Failed> {
    future: JoinAll<Vec<UnfailableFuture<F>>>,
    fail: Option<G>,
}

impl<F: Future, G: Failed> UnfailableJoinAll<F, G> {
    pub(super) fn new(futures: Vec<UnfailableFuture<F>>, fail: G) -> Self {
        UnfailableJoinAll {
            future: join_all(futures),
            fail: Some(fail),
        }
    }
}

impl<F: Future<Item = ()>, G: Failed> Future for UnfailableJoinAll<F, G> {
    type Item = ();
    type Error = F::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let results = match self.future.poll() {
            Ok(Async::NotReady) => return Ok(Async::NotReady),
            Ok(Async::Ready(results)) => results,
        };
        for result in results {
            if let Err(e) = result {
                self.fail.take().unwrap().failed();
                bail!(e);
            }
        }
        Ok(Async::Ready(()))
    }
}

pub struct UnfailableJoinAllPlusOne<F: Future, G: Failed> {
    future: JoinAll<Vec<UnfailableFuture<F>>>,
    fail: Option<G>,
}

impl<F: Future, G: Failed> UnfailableJoinAllPlusOne<F, G> {
    pub(super) fn new(futures: Vec<UnfailableFuture<F>>, fail: G) -> Self {
        UnfailableJoinAllPlusOne {
            future: join_all(futures),
            fail: Some(fail),
        }
    }
}

impl<F: Future<Item = ()>, G: Failed> Future for UnfailableJoinAllPlusOne<F, G> {
    type Item = ();
    type Error = F::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let results = match self.future.poll() {
            Ok(Async::NotReady) => return Ok(Async::NotReady),
            Ok(Async::Ready(results)) => results,
        };
        let mut error_occurred = false;
        for result in results {
            if let Err(e) = result {
                if !error_occurred {
                    error_occurred = true;
                } else {
                    self.fail.take().unwrap().failed();
                    bail!(e)
                }
            }
        }
        Ok(Async::Ready(()))
    }
}

pub trait Failed {
    fn failed(self);
}

// TODO immovable generator!
pub struct GeneratorFuture<T>(T);

impl<T> GeneratorFuture<T> {
    pub fn new(g: T) -> Self {
        GeneratorFuture(g)
    }
}

impl<G, T, E> Future for GeneratorFuture<G>
where
    G: Generator<Yield = (), Return = Result<T, E>>,
{
    type Item = T;
    type Error = E;

    fn poll(&mut self) -> Poll<T, E> {
        match unsafe { self.0.resume() } {
            GeneratorState::Yielded(()) => Ok(Async::NotReady),
            GeneratorState::Complete(r) => r.map(Async::Ready),
        }
    }
}

macro_rules! await {
    ($e:expr) => {{
        let mut f = $e;
        loop {
            if let $crate::futures::Async::Ready(t) = f.poll()? {
                break t;
            }
            yield ();
        }
    }};
}

pub fn alloc_uninitialized(size: usize) -> Box<[u8]> {
    let mut v = Vec::new();
    v.reserve(size);
    unsafe {
        v.set_len(size);
    }
    v.into_boxed_slice()
}

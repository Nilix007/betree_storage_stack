use futures::future::{join_all, JoinAll, Then};
use futures::prelude::*;
use futures::task::Context;

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
    future: JoinAll<UnfailableFuture<F>>,
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

    fn poll(&mut self, cx: &mut Context) -> Poll<Self::Item, Self::Error> {
        let results = match self.future.poll(cx) {
            Ok(Async::Pending) => return Ok(Async::Pending),
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
    future: JoinAll<UnfailableFuture<F>>,
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

    fn poll(&mut self, cx: &mut Context) -> Poll<Self::Item, Self::Error> {
        let results = match self.future.poll(cx) {
            Ok(Async::Pending) => return Ok(Async::Pending),
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

pub fn alloc_uninitialized(size: usize) -> Box<[u8]> {
    let mut v = Vec::new();
    v.reserve(size);
    unsafe {
        v.set_len(size);
    }
    v.into_boxed_slice()
}

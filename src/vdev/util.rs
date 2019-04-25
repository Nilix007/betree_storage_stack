use futures::prelude::*;
use futures::ready;
use futures::stream::{Collect, FuturesUnordered};
use futures::task::{Context, Poll};
use std::pin::Pin;

pub struct UnfailableJoinAll<F: Future, G: Failed> {
    future: Collect<FuturesUnordered<F>, Vec<F::Output>>,
    fail: Option<G>,
}

impl<F: Future, G: Failed> UnfailableJoinAll<F, G> {
    // XXX use IntoIter instead of Vec<_>
    pub(super) fn new(futures: Vec<F>, fail: G) -> Self {
        UnfailableJoinAll {
            future: futures
                .into_iter()
                .collect::<FuturesUnordered<_>>()
                .collect(),
            fail: Some(fail),
        }
    }
}

impl<F: Future<Output = Result<(), E>>, G: Failed, E> TryFuture for UnfailableJoinAll<F, G> {
    type Ok = ();
    type Error = E;

    fn try_poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Result<Self::Ok, Self::Error>> {
        let this = unsafe { Pin::get_unchecked_mut(self) };
        let f = unsafe { Pin::new_unchecked(&mut this.future) };
        let results = ready!(f.poll(ctx));
        for result in results {
            if let Err(e) = result {
                this.fail.take().unwrap().failed();
                return Poll::Ready(Err(e));
            }
        }
        Poll::Ready(Ok(()))
    }
}

pub struct UnfailableJoinAllPlusOne<F: Future, G: Failed> {
    future: Collect<FuturesUnordered<F>, Vec<F::Output>>,
    fail: Option<G>,
}

impl<F: Future, G: Failed> UnfailableJoinAllPlusOne<F, G> {
    // XXX use IntoIter instead of Vec<_>
    pub(super) fn new(futures: Vec<F>, fail: G) -> Self {
        UnfailableJoinAllPlusOne {
            future: futures
                .into_iter()
                .collect::<FuturesUnordered<_>>()
                .collect(),
            fail: Some(fail),
        }
    }
}

impl<F: Future<Output = Result<(), E>>, G: Failed, E> TryFuture for UnfailableJoinAllPlusOne<F, G> {
    type Ok = ();
    type Error = E;

    fn try_poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Result<Self::Ok, Self::Error>> {
        let this = unsafe { Pin::get_unchecked_mut(self) };
        let f = unsafe { Pin::new_unchecked(&mut this.future) };
        let results = ready!(f.poll(ctx));
        let mut error_occurred = false;
        for result in results {
            if let Err(e) = result {
                if !error_occurred {
                    error_occurred = true;
                } else {
                    this.fail.take().unwrap().failed();
                    return Poll::Ready(Err(e));
                }
            }
        }
        Poll::Ready(Ok(()))
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

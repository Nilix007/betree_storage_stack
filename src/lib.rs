//! Storage stack with key-value store interface on top of B<sup>e</sup>-Trees.
#![warn(missing_docs)]
#![feature(exhaustive_patterns)]
#![feature(integer_atomics)]
#![feature(collections_range)]
#![feature(unboxed_closures)]
#![feature(generator_trait, generators)]
#![feature(specialization)]
#![feature(ptr_internals)]
#![feature(never_type)]
#![feature(pin)]
#![feature(proc_macro_non_items)]
#![cfg_attr(any(test, feature = "clippy"), feature(plugin))]
#![cfg_attr(feature = "clippy", plugin(clippy))]
#![cfg_attr(test, plugin(quickcheck_macros))]
#![cfg_attr(test, feature(test))]

extern crate bincode;
extern crate byteorder;
extern crate core;
#[macro_use]
extern crate error_chain;
extern crate futures;
extern crate itertools;
extern crate libc;
#[macro_use]
extern crate log;
extern crate lz4;
extern crate owning_ref;
extern crate parking_lot;
#[cfg(test)]
extern crate quickcheck;
#[cfg(test)]
extern crate rand;
extern crate ref_slice;
extern crate seqlock;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate stable_deref_trait;
#[cfg(test)]
extern crate test;
extern crate twox_hash;

pub mod allocator;
pub mod atomic_option;
pub mod bounded_future_queue;
pub mod buffer;
pub mod c_interface;
pub mod cache;
pub mod checksum;
pub mod compression;
pub mod cow_bytes;
pub mod data_management;
pub mod database;
pub mod size;
pub mod storage_pool;
pub mod tree;
pub mod vdev;

pub use self::database::{Database, Dataset, Error, Snapshot};
pub use self::storage_pool::Configuration;

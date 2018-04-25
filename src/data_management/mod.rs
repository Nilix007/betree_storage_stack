//! This module provides the Data Management Layer
//! which handles user-defined objects and incldues caching and write back.

use allocator::{Action, SegmentAllocator, SegmentId};
use cache::AddSize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use size::{Size, StaticSize};
use stable_deref_trait::StableDeref;
use std::error;
use std::fmt::Debug;
use std::hash::Hash;
use std::io::{self, Write};
use std::ops::DerefMut;
use storage_pool::DiskOffset;
use vdev::Block;

/// Marker trait for plain old data types
pub trait PodType:
    Serialize + DeserializeOwned + Debug + Hash + Eq + Copy + StaticSize + Send + Sync + 'static
{
}
impl<
        T: Serialize
            + DeserializeOwned
            + Debug
            + Hash
            + Eq
            + Copy
            + StaticSize
            + Send
            + Sync
            + 'static,
    > PodType for T
{
}

/// An reference to an object managed by a `Dml`.
pub trait ObjectRef: Serialize + DeserializeOwned + StaticSize + 'static {
    /// The ObjectPointer for this ObjectRef.
    type ObjectPointer;
    /// Return a reference to an `Self::ObjectPointer`
    /// if this object reference is in the unmodified state.
    fn get_unmodified(&self) -> Option<&Self::ObjectPointer>;
}

/// Defines some base types for a `Dml`.
pub trait DmlBase: Sized {
    /// A reference to an object managed by this `Dmu`.
    type ObjectRef: ObjectRef<ObjectPointer = Self::ObjectPointer>;
    /// The pointer type to an on-disk object.
    type ObjectPointer: Serialize + DeserializeOwned;
    /// The info type which is tagged to each object.
    type Info: PodType;
}

/// Defines some base types for a `Handler`.
pub trait HandlerTypes: Sized {
    /// The generation of an object.
    type Generation: PodType;
    /// The info tagged to an object.
    type Info: PodType;
}

/// An object managed by a `Dml`.
pub trait Object<R>: Size + Sized {
    /// Packs the object into the given `writer`.
    fn pack<W: Write>(&self, writer: W) -> Result<(), io::Error>;
    /// Unpacks the object from the given `data`.
    fn unpack(data: Box<[u8]>) -> Result<Self, io::Error>;

    /// Returns debug information about an object.
    fn debug_info(&self) -> String;

    /// Calls a closure on each child `ObjectRef` of this object.
    ///
    /// This method is short-circuiting on `Err(_)`.
    fn for_each_child<E, F>(&mut self, f: F) -> Result<(), E>
    where
        F: FnMut(&mut R) -> Result<(), E>;
}

/// A `Dml` for a specific `Handler`.
pub trait HandlerDml: DmlBase {
    /// The object type managed by this Dml.
    type Object: Object<Self::ObjectRef>;

    /// A reference to a cached object.
    type CacheValueRef: StableDeref<Target = Self::Object> + AddSize + 'static;

    /// A mutable reference to a cached object.
    type CacheValueRefMut: StableDeref<Target = Self::Object> + DerefMut + AddSize + 'static;

    /// Provides immutable access to the object identified by the given
    /// `ObjectRef`.  Fails if the object was modified and has been evicted.
    fn try_get(&self, or: &Self::ObjectRef) -> Option<Self::CacheValueRef>;

    /// Provides immutable access to the object identified by the given
    /// `ObjectRef`.
    fn get(&self, or: &mut Self::ObjectRef) -> Result<Self::CacheValueRef, Error>;

    /// Provides mutable access to the object identified by the given
    /// `ObjectRef`.
    ///
    /// If the object is not mutable, it will be `CoW`ed and `info` will be
    /// attached to the object.
    fn get_mut(
        &self,
        or: &mut Self::ObjectRef,
        info: Self::Info,
    ) -> Result<Self::CacheValueRefMut, Error>;

    /// Provides mutable access to the object
    /// if this object is already mutable.
    fn try_get_mut(&self, or: &Self::ObjectRef) -> Option<Self::CacheValueRefMut>;

    /// Inserts a new mutable `object` into the cache.
    fn insert(&self, object: Self::Object, info: Self::Info) -> Self::ObjectRef;

    /// Inserts a new mutable `object` into the cache.
    fn insert_and_get_mut(
        &self,
        object: Self::Object,
        info: Self::Info,
    ) -> (Self::CacheValueRefMut, Self::ObjectRef);

    /// Removes the object referenced by `or`.
    fn remove(&self, or: Self::ObjectRef);

    /// Removes the object referenced by `or` and returns it.
    fn get_and_remove(&self, or: Self::ObjectRef) -> Result<Self::Object, Error>;

    /// Evicts excessive cache entries.
    fn evict(&self) -> Result<(), Error>;

    /// Turns an ObjectPointer into an ObjectReference.
    fn ref_from_ptr(r: Self::ObjectPointer) -> Self::ObjectRef;
}

/// Handler for a `Dml`.
pub trait Handler<R: ObjectRef>: HandlerTypes {
    /// The object type managed by this handler.
    type Object: Object<R>;
    /// The error type of this handler.
    type Error: error::Error + Send;

    /// Returns the current generation.
    fn current_generation(&self) -> Self::Generation;

    /// Updates the allocation bitmap due to an allocation or deallocation.
    fn update_allocation_bitmap<X>(
        &self,
        offset: DiskOffset,
        size: Block<u32>,
        action: Action,
        dmu: &X,
    ) -> Result<(), Self::Error>
    where
        X: HandlerDml<
            Object = Self::Object,
            ObjectRef = R,
            ObjectPointer = R::ObjectPointer,
            Info = Self::Info,
        >,
        R::ObjectPointer: Serialize + DeserializeOwned;

    /// Retrieves the allocation bitmap for specific segment.
    fn get_allocation_bitmap<X>(
        &self,
        id: SegmentId,
        dmu: &X,
    ) -> Result<SegmentAllocator, Self::Error>
    where
        X: HandlerDml<
            Object = Self::Object,
            ObjectRef = R,
            ObjectPointer = R::ObjectPointer,
            Info = Self::Info,
        >,
        R::ObjectPointer: Serialize + DeserializeOwned;

    /// Returns the amount of free space (in blocks) for a given top-level vdev.
    fn get_free_space(&self, disk_id: u16) -> Block<u64>;

    /// Will be called when an object has been made mutable.
    /// May be used to mark the data blocks for delayed deallocation.
    fn copy_on_write(
        &self,
        offset: DiskOffset,
        size: Block<u32>,
        generation: Self::Generation,
        info: Self::Info,
    );
}

/// The Data Mangement Layer
pub trait Dml: HandlerDml {
    /// Writes back an object and all its dependencies.
    /// `acquire_or_lock` shall return a lock guard
    /// that provides mutable access to the object reference.
    fn write_back<F>(&self, acquire_or_lock: F) -> Result<Self::ObjectPointer, Error>
    where
        F: FnMut<()>,
        F::Output: DerefMut<Target = Self::ObjectRef>;

    /// Prefetch session type.
    type Prefetch;

    /// Prefetches the on-disk object identified by `or`.
    /// Will return `None` if object is in cache.
    fn prefetch(&self, or: &Self::ObjectRef) -> Result<Option<Self::Prefetch>, Error>;

    /// Finishes the prefetching.
    fn finish_prefetch(&self, p: Self::Prefetch) -> Result<(), Error>;

    /// Drops the cache entries.
    fn drop_cache(&self);
}

mod delegation;
mod errors;
pub(crate) mod impls;
// mod handler_test;

pub use self::errors::{Error, ErrorKind};
pub use self::impls::Dmu;

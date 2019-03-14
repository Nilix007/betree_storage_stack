use super::MessageAction;
use crate::cow_bytes::{CowBytes, SlicedCowBytes};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Borrow;
use std::ops::RangeBounds;

use super::errors::*;

// TODO
// - ro transaction
// - how to do range delete with ro transaction?

/// Basic Tree Layer interface.
pub trait TreeBaseLayer<M: MessageAction> {
    /// Inserts a new message with the given `key`.
    fn insert<K: Borrow<[u8]> + Into<CowBytes>>(
        &self,
        key: K,
        msg: SlicedCowBytes,
    ) -> Result<(), Error>;

    /// Gets the entry for the given `key` if it exists.
    fn get<K: Borrow<[u8]>>(&self, key: K) -> Result<Option<SlicedCowBytes>, Error>;

    /// Returns the depth of the tree.
    fn depth(&self) -> Result<u32, Error>;
}

/// Tree Layer interface.
pub trait TreeLayer<M: MessageAction>: TreeBaseLayer<M> {
    /// The range query iterator.
    type Range: Iterator<Item = Result<(CowBytes, SlicedCowBytes), Error>>;
    /// Issues a range query for the given key `range`.
    /// Returns an iterator that will iterate over the entries in that range.
    fn range<K, R>(&self, range: R) -> Result<Self::Range, Error>
    where
        R: RangeBounds<K>,
        K: Borrow<[u8]> + Into<CowBytes>,
        Self: Clone;

    /// Deletes all entries in the given key `range`.
    fn range_delete<K, R>(&self, range: R) -> Result<(), Error>
    where
        R: RangeBounds<K>,
        K: Borrow<[u8]> + Into<Box<[u8]>>;

    /// Tree pointer type that represents a synced tree.
    type Pointer: Serialize + DeserializeOwned;

    /// Sync the tree to disk.
    fn sync(&self) -> Result<Self::Pointer, Error>;
}

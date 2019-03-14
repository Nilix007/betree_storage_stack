//! This module provides the `Compression` trait for compressing and
//! decompressing data.
//! `None` and `Lz4` are provided as implementation.

use crate::size::Size;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;
use std::io::{self, Write};

/// Trait for compressing and decompressing data.
pub trait Compression:
    Debug + Serialize + DeserializeOwned + Size + Clone + Send + Sync + 'static
{
    /// Returned by `compress`.
    type Compress: Compress;
    /// Decompresses data from the given `buffer`.
    fn decompress(&self, buffer: Box<[u8]>) -> io::Result<Box<[u8]>>;
    /// Returns an object for compressing data into a `Box<[u8]>`.
    fn compress(&self) -> Self::Compress;
}

/// Trait for the object that compresses data.
pub trait Compress: Write {
    /// Finishes the compression stream and returns a buffer that contains the
    /// compressed data.
    fn finish(self) -> Box<[u8]>;
}

mod none;
pub use self::none::None;

mod lz4;
pub use self::lz4::Lz4;

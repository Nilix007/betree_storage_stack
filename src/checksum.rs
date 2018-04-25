//! This module provides a `Checksum` trait for verifying data integrity.

use serde::Serialize;
use serde::de::DeserializeOwned;
use size::{Size, StaticSize};
use std::error::Error;
use std::fmt;
use std::hash::Hasher;
use std::iter::once;
use twox_hash;

/// A checksum to verify data integrity.
pub trait Checksum:
    Serialize + DeserializeOwned + Size + Clone + Send + Sync + fmt::Debug + 'static
{
    /// Builds a new `Checksum`.
    type Builder: Builder<Self>;

    /// Verifies the contents of the given buffer which consists of multiple
    /// `u8` slices.
    fn verify_buffer<I: IntoIterator<Item = T>, T: AsRef<[u8]>>(
        &self,
        data: I,
    ) -> Result<(), ChecksumError>;

    /// Verifies the contents of the given buffer.
    fn verify(&self, data: &[u8]) -> Result<(), ChecksumError> {
        self.verify_buffer(once(data))
    }
}

/// A checksum builder
pub trait Builder<C: Checksum>:
    Serialize + DeserializeOwned + Clone + Send + Sync + fmt::Debug + 'static
{
    /// The internal state of the checksum.
    type State: State<Checksum = C>;

    /// Create a new state to build a checksum.
    fn build(&self) -> Self::State;
}

/// Holds a state for building a new `Checksum`.
pub trait State {
    /// The resulting `Checksum`.
    type Checksum: Checksum;

    /// Ingests the given data into the state.
    fn ingest(&mut self, data: &[u8]);

    /// Builds the actual `Checksum`.
    fn finish(self) -> Self::Checksum;
}

/// This is the error that will be returned when a `Checksum` does not match.
#[derive(Debug)]
pub struct ChecksumError;

impl fmt::Display for ChecksumError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "Failed to verify the integrity")
    }
}

impl Error for ChecksumError {
    fn description(&self) -> &str {
        "a checksum error occurred"
    }
}

/// `XxHash` contains a digest of `xxHash`
/// which is an "extremely fast non-cryptographic hash algorithm"
/// (<https://github.com/Cyan4973/xxHash>)
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub struct XxHash(u64);

impl StaticSize for XxHash {
    fn size() -> usize {
        8
    }
}

impl Checksum for XxHash {
    type Builder = XxHashBuilder;

    fn verify_buffer<I: IntoIterator<Item = T>, T: AsRef<[u8]>>(
        &self,
        data: I,
    ) -> Result<(), ChecksumError> {
        let mut state = XxHashBuilder.build();
        for x in data {
            state.ingest(x.as_ref());
        }
        let other = state.finish();
        if *self == other {
            Ok(())
        } else {
            Err(ChecksumError)
        }
    }
}

/// The corresponding `Builder` for `XxHash`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct XxHashBuilder;

impl Builder<XxHash> for XxHashBuilder {
    type State = XxHashState;

    fn build(&self) -> Self::State {
        XxHashState(twox_hash::XxHash::with_seed(0))
    }
}

/// The internal state of `XxHash`.
pub struct XxHashState(twox_hash::XxHash);

impl State for XxHashState {
    type Checksum = XxHash;

    fn ingest(&mut self, data: &[u8]) {
        self.0.write(data);
    }

    fn finish(self) -> Self::Checksum {
        XxHash(self.0.finish())
    }
}

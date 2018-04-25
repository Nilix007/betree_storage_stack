//! This module provides a B<sup>e</sup>-Tree on top of the Data Management
//! Layer.

mod errors;
mod imp;
mod layer;
mod message_action;

pub use self::errors::{Error, ErrorKind};
pub use self::imp::{Inner, Node, RangeIterator, Tree};
pub use self::layer::{TreeBaseLayer, TreeLayer};
pub use self::message_action::{DefaultMessageAction, MessageAction};

//! This module provides `MessageAction` which can be used to override the
//! default message type to build custom message actions.
//! These can have a custom payload and may perform arbitrary
//! computation on message application.

use crate::cow_bytes::{CowBytes, SlicedCowBytes};
use bincode::{deserialize, serialize_into};
use std::fmt::Debug;
use std::ops::Deref;

/// Defines the action of a message.
pub trait MessageAction: Debug + Send + Sync {
    /// Applies the message `msg`. `data` holds the current data.
    fn apply(&self, key: &[u8], msg: &SlicedCowBytes, data: &mut Option<SlicedCowBytes>);

    /// Applies the message `msg` to a leaf entry. `data` holds the current
    /// data.
    fn apply_to_leaf(&self, key: &[u8], msg: SlicedCowBytes, data: &mut Option<SlicedCowBytes>) {
        self.apply(key, &msg, data)
    }

    /// Merges two messages.
    fn merge(
        &self,
        key: &[u8],
        upper_msg: SlicedCowBytes,
        lower_msg: SlicedCowBytes,
    ) -> SlicedCowBytes;
}

impl<T: Deref + Debug + Send + Sync> MessageAction for T
where
    T::Target: MessageAction,
{
    fn apply(&self, key: &[u8], msg: &SlicedCowBytes, data: &mut Option<SlicedCowBytes>) {
        (**self).apply(key, msg, data)
    }
    fn apply_to_leaf(&self, key: &[u8], msg: SlicedCowBytes, data: &mut Option<SlicedCowBytes>) {
        (**self).apply_to_leaf(key, msg, data)
    }
    fn merge(
        &self,
        key: &[u8],
        upper_msg: SlicedCowBytes,
        lower_msg: SlicedCowBytes,
    ) -> SlicedCowBytes {
        (**self).merge(key, upper_msg, lower_msg)
    }
}

/// This is the default message action. It supports inserts, deletes, and
/// upserts.
#[derive(Debug, Copy, Clone)]
pub struct DefaultMessageAction;

#[derive(Serialize, Deserialize)]
struct Upsert<'a> {
    offset: u32,
    data: &'a [u8],
}

enum MsgType<'a> {
    Overwrite(Option<SlicedCowBytes>),
    Upsert(Vec<Upsert<'a>>),
}

impl<'a> MsgType<'a> {
    fn unpack(msg: &SlicedCowBytes) -> MsgType {
        let t = msg[0];
        match t {
            0 => MsgType::Overwrite(Some(msg.clone().subslice(1, msg.len() as u32 - 1))),
            1 => MsgType::Overwrite(None),
            2 => MsgType::Upsert(deserialize(&msg[1..]).unwrap()),
            _ => unreachable!(),
        }
    }

    fn as_upsert(msg: &SlicedCowBytes) -> Option<Vec<Upsert>> {
        if msg[0] == 2 {
            Some(deserialize(&msg[1..]).unwrap())
        } else {
            None
        }
    }
}

impl DefaultMessageAction {
    fn apply_upsert(upsert: &[Upsert], msg_data: &mut Option<SlicedCowBytes>) {
        if upsert.is_empty() {
            return;
        }

        let mut data = msg_data
            .as_ref()
            .map(|b| CowBytes::from(&b[..]))
            .unwrap_or_default();
        for &Upsert {
            offset,
            data: new_data,
        } in upsert
        {
            let end_offset = offset as usize + new_data.len();

            if data.len() <= offset as usize {
                data.fill_zeros_up_to(offset as usize);
                data.push_slice(new_data);
            } else {
                data.fill_zeros_up_to(end_offset);

                let slice = &mut data[offset as usize..end_offset as usize];
                slice.copy_from_slice(new_data);
            }
        }
        *msg_data = Some(data.into());
    }

    fn build_overwrite_msg(data: Option<&[u8]>) -> SlicedCowBytes {
        let mut v = Vec::with_capacity(1 + data.map_or(0, |b| b.len()));
        v.push(if data.is_some() { 0 } else { 1 });
        if let Some(b) = data {
            v.extend_from_slice(b);
        }
        CowBytes::from(v).into()
    }

    fn build_upsert_msg(upsert: &[Upsert]) -> SlicedCowBytes {
        let mut v = Vec::new();
        v.push(2);
        serialize_into(&mut v, upsert).unwrap();
        CowBytes::from(v).into()
    }

    /// Return a new messages which unconditionally inserts the given `data`.
    pub fn insert_msg(data: &[u8]) -> SlicedCowBytes {
        Self::build_overwrite_msg(Some(data))
    }

    /// Return a new messages which deletes data.
    pub fn delete_msg() -> SlicedCowBytes {
        Self::build_overwrite_msg(None)
    }

    /// Return a new messages which will update or insert the given `data` at
    /// `offset`.
    pub fn upsert_msg(offset: u32, data: &[u8]) -> SlicedCowBytes {
        Self::build_upsert_msg(&[Upsert { offset, data }])
    }
}

impl MessageAction for DefaultMessageAction {
    fn apply(&self, _key: &[u8], msg: &SlicedCowBytes, data: &mut Option<SlicedCowBytes>) {
        match MsgType::unpack(msg) {
            MsgType::Overwrite(new_data) => *data = new_data,
            MsgType::Upsert(upsert) => Self::apply_upsert(&upsert, data),
        }
    }

    fn merge(
        &self,
        _key: &[u8],
        upper_msg: SlicedCowBytes,
        lower_msg: SlicedCowBytes,
    ) -> SlicedCowBytes {
        let upper_upsert = match MsgType::as_upsert(&upper_msg) {
            // TODO
            None => return upper_msg.clone(),
            Some(upsert) => upsert,
        };
        match MsgType::unpack(&lower_msg) {
            MsgType::Overwrite(mut data) => {
                Self::apply_upsert(&upper_upsert, &mut data);
                Self::build_overwrite_msg(data.as_ref().map(|b| &b[..]))
            }
            MsgType::Upsert(mut lower_upsert) => {
                lower_upsert.extend(upper_upsert);
                Self::build_upsert_msg(&lower_upsert)
            }
        }
    }
}

#[cfg(test)]
pub use self::tests::DefaultMessageActionMsg;

#[cfg(test)]
mod tests {
    use super::{DefaultMessageAction, Upsert};
    use crate::cow_bytes::SlicedCowBytes;
    use quickcheck::{Arbitrary, Gen};
    use rand::Rng;

    #[derive(Debug, Clone)]
    pub struct DefaultMessageActionMsg(pub SlicedCowBytes);

    impl Arbitrary for DefaultMessageActionMsg {
        fn arbitrary<G: Gen>(g: &mut G) -> Self {
            let b = g.gen_range(0, 3);
            match b {
                2 => {
                    // upsert
                    // TODO multiple?
                    let offset = g.gen_range(0, 10);
                    let data: Vec<_> = Arbitrary::arbitrary(g);
                    DefaultMessageActionMsg(DefaultMessageAction::build_upsert_msg(&[Upsert {
                        offset,
                        data: &data,
                    }]))
                }
                1 => {
                    // delete
                    DefaultMessageActionMsg(DefaultMessageAction::delete_msg())
                }
                0 => {
                    // overwrite with data
                    let data: Vec<_> = Arbitrary::arbitrary(g);
                    DefaultMessageActionMsg(DefaultMessageAction::insert_msg(&data))
                }
                _ => unreachable!(),
            }
        }
    }
}

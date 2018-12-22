use cow_bytes::{CowBytes, SlicedCowBytes};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use size::{Size, StaticSize};
use std::borrow::Borrow;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, Bound};
use std::mem::replace;
use tree::MessageAction;

/// A buffer for messages that belong to a child of a tree node.
#[derive(Debug, Serialize, Deserialize)]
#[serde(bound(serialize = "N: Serialize", deserialize = "N: Deserialize<'de>"))]
pub(super) struct ChildBuffer<N: 'static> {
    buffer_entries_size: usize,
    buffer: BTreeMap<CowBytes, SlicedCowBytes>,
    #[serde(with = "ser_np")]
    pub(super) node_pointer: RwLock<N>,
}

mod ser_np {
    use super::RwLock;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<N, S>(np: &RwLock<N>, serializer: S) -> Result<S::Ok, S::Error>
    where
        N: Serialize,
        S: Serializer,
    {
        np.read().serialize(serializer)
    }

    pub fn deserialize<'de, N, D>(deserializer: D) -> Result<RwLock<N>, D::Error>
    where
        N: Deserialize<'de>,
        D: Deserializer<'de>,
    {
        N::deserialize(deserializer).map(RwLock::new)
    }

}

impl<N: StaticSize> Size for ChildBuffer<N> {
    fn size(&self) -> usize {
        Self::static_size() + self.buffer_entries_size + N::size()
    }
}

impl<N: StaticSize> ChildBuffer<N> {
    pub fn actual_size(&self) -> usize {
        Self::static_size()
            + N::size()
            + self
                .buffer
                .iter()
                .map(|(key, msgs)| key.size() + msgs.size())
                .sum::<usize>()
    }
}

impl<N> ChildBuffer<N> {
    pub fn static_size() -> usize {
        16
    }

    pub fn buffer_size(&self) -> usize {
        self.buffer_entries_size
    }

    /// Returns whether there is no message in this buffer for the given `key`.
    pub fn is_empty(&self, key: &[u8]) -> bool {
        !self.buffer.contains_key(key)
    }

    pub fn get(&self, key: &[u8]) -> Option<&SlicedCowBytes> {
        self.buffer.get(key)
    }

    /// Returns an iterator over all messages.
    pub fn get_all_messages<'a>(
        &'a self,
    ) -> impl Iterator<Item = (&'a CowBytes, &'a SlicedCowBytes)> + 'a {
        self.buffer.iter().map(|(key, msg)| (key, msg))
    }

    /// Takes the message buffer out this `ChildBuffer`,
    /// leaving an empty one in its place.
    pub fn take(&mut self) -> (BTreeMap<CowBytes, SlicedCowBytes>, usize) {
        (
            replace(&mut self.buffer, BTreeMap::new()),
            replace(&mut self.buffer_entries_size, 0),
        )
    }

    pub fn append(&mut self, other: &mut Self) {
        self.buffer.append(&mut other.buffer);
        self.buffer_entries_size += other.buffer_entries_size;
        other.buffer_entries_size = 0;
    }

    /// Splits this `ChildBuffer` at `pivot`
    /// so that `self` contains all entries up to (and including) `pivot_key`
    /// and the returned `Self` contains the other entries and `node_pointer`.
    pub fn split_at(&mut self, pivot: &CowBytes, node_pointer: N) -> Self {
        let (buffer, buffer_entries_size) = self.split_off(pivot);
        ChildBuffer {
            buffer,
            buffer_entries_size,
            node_pointer: RwLock::new(node_pointer),
        }
    }
    fn split_off(&mut self, pivot: &CowBytes) -> (BTreeMap<CowBytes, SlicedCowBytes>, usize) {
        // `split_off` puts the split-key into the right buffer.
        let mut next_key = pivot.to_vec();
        next_key.push(0);
        let right_buffer = self.buffer.split_off(&next_key[..]);

        let right_entry_size = right_buffer
            .iter()
            .map(|(key, value)| key.size() + value.size())
            .sum();
        self.buffer_entries_size -= right_entry_size;
        (right_buffer, right_entry_size)
    }

    pub fn rebalance(&mut self, right_sibling: &mut Self, new_pivot_key: &CowBytes) {
        self.append(right_sibling);
        let (buffer, buffer_entries_size) = self.split_off(new_pivot_key);
        right_sibling.buffer = buffer;
        right_sibling.buffer_entries_size = buffer_entries_size;
    }

    /// Inserts a message to this buffer for the given `key`.
    pub fn insert<Q, M>(&mut self, key: Q, msg: SlicedCowBytes, msg_action: M) -> isize
    where
        Q: Borrow<[u8]> + Into<CowBytes>,
        M: MessageAction,
    {
        let key = key.into();
        let key_size = key.size();
        // TODO
        match self.buffer.entry(key.clone()) {
            Entry::Vacant(e) => {
                let size_delta = key_size + msg.size();
                e.insert(msg);
                self.buffer_entries_size += size_delta;
                size_delta as isize
            }
            Entry::Occupied(mut e) => {
                let lower = e.get_mut().clone();
                let lower_size = lower.size();
                let merged_msg = msg_action.merge(&key, msg, lower);
                let merged_msg_size = merged_msg.size();
                *e.get_mut() = merged_msg;
                self.buffer_entries_size -= lower_size;
                self.buffer_entries_size += merged_msg_size;
                merged_msg_size as isize - lower_size as isize
            }
        }
    }

    /// Constructs a new, empty buffer.
    pub fn new(node_pointer: N) -> Self {
        ChildBuffer {
            buffer: BTreeMap::new(),
            buffer_entries_size: 0,
            node_pointer: RwLock::new(node_pointer),
        }
    }
}

impl<N> ChildBuffer<N> {
    pub fn range_delete(&mut self, start: &[u8], end: Option<&[u8]>) -> usize {
        // TODO https://github.com/rust-lang/rust/issues/42849
        let mut size_delta = 0;
        let range = (
            Bound::Included(start),
            end.map_or(Bound::Unbounded, Bound::Excluded),
        );
        let mut keys = Vec::new();
        for (key, msg) in self.buffer.range_mut::<[u8], _>(range) {
            size_delta += key.size() + msg.size();
            keys.push(key.clone());
        }
        for key in keys {
            self.buffer.remove(&key);
        }
        self.buffer_entries_size -= size_delta;
        size_delta
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bincode::serialized_size;
    use quickcheck::{Arbitrary, Gen};
    use rand::Rng;
    use tree::message_action::DefaultMessageActionMsg;

    impl<N: Clone> Clone for ChildBuffer<N> {
        fn clone(&self) -> Self {
            ChildBuffer {
                buffer_entries_size: self.buffer_entries_size,
                buffer: self.buffer.clone(),
                node_pointer: RwLock::new(self.node_pointer.read().clone()),
            }
        }
    }

    impl<N: PartialEq> PartialEq for ChildBuffer<N> {
        fn eq(&self, other: &Self) -> bool {
            self.buffer_entries_size == other.buffer_entries_size
                && self.buffer == other.buffer
                && *self.node_pointer.read() == *other.node_pointer.read()
        }
    }

    impl<N: Arbitrary> Arbitrary for ChildBuffer<N> {
        fn arbitrary<G: Gen>(g: &mut G) -> Self {
            let entries_cnt = g.gen_range(0, 20);
            let buffer: BTreeMap<CowBytes, SlicedCowBytes> = (0..entries_cnt)
                .map(|_| {
                    (
                        CowBytes::arbitrary(g),
                        DefaultMessageActionMsg::arbitrary(g).0,
                    )
                })
                .collect();
            ChildBuffer {
                buffer_entries_size: buffer
                    .iter()
                    .map(|(key, value)| key.size() + value.size())
                    .sum::<usize>(),
                buffer,
                node_pointer: RwLock::new(Arbitrary::arbitrary(g)),
            }
        }
    }

    #[quickcheck]
    fn check_serialize_size(child_buffer: ChildBuffer<()>) {
        assert_eq!(
            child_buffer.size(),
            serialized_size(&child_buffer).unwrap() as usize
        );
    }

    #[quickcheck]
    fn check_size_split_at(mut child_buffer: ChildBuffer<()>, pivot_key: CowBytes) {
        let size_before = child_buffer.size();
        let sibling = child_buffer.split_at(&pivot_key, ());
        assert_eq!(
            child_buffer.size(),
            serialized_size(&child_buffer).unwrap() as usize
        );
        assert_eq!(sibling.size(), serialized_size(&sibling).unwrap() as usize);
        assert_eq!(
            child_buffer.size() + sibling.buffer_entries_size,
            size_before
        );
    }

    #[quickcheck]
    fn check_split_at(mut child_buffer: ChildBuffer<()>, pivot_key: CowBytes) {
        let this = child_buffer.clone();
        let mut sibling = child_buffer.split_at(&pivot_key, ());
        assert!(child_buffer
            .buffer
            .iter()
            .next_back()
            .map_or(true, |(key, value)| key.clone() <= pivot_key));
        assert!(sibling
            .buffer
            .iter()
            .next()
            .map_or(true, |(key, value)| key.clone() > pivot_key));
        let (mut buffer, _) = child_buffer.take();
        buffer.append(&mut sibling.take().0);
        assert_eq!(this.buffer, buffer);
    }
}

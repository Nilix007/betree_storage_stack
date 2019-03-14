use super::FillUpResult;
use crate::cow_bytes::{CowBytes, SlicedCowBytes};
use crate::size::Size;
use crate::tree::MessageAction;
use std::borrow::Borrow;
use std::collections::{BTreeMap, Bound};
use std::iter::FromIterator;

/// A leaf node of the tree holds pairs of keys values which are plain data.
#[derive(Debug, Clone)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub(super) struct LeafNode {
    entries_size: usize,
    entries: BTreeMap<CowBytes, SlicedCowBytes>,
}

impl Size for LeafNode {
    fn size(&self) -> usize {
        4 + self.entries_size
    }
}
impl LeafNode {
    pub(super) fn actual_size(&self) -> usize {
        4 + self
            .entries
            .iter()
            .map(|(key, value)| key.size() + value.size())
            .sum::<usize>()
    }
}

impl<'a> FromIterator<(&'a [u8], SlicedCowBytes)> for LeafNode {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = (&'a [u8], SlicedCowBytes)>,
    {
        let mut entries_size = 0;
        let entries = iter
            .into_iter()
            .map(|(key, value)| {
                entries_size += 16 + key.len() + value.len();
                (key.into(), value)
            })
            .collect();
        LeafNode {
            entries_size,
            entries,
        }
    }
}

impl LeafNode {
    /// Constructs a new, empty `LeafNode`.
    pub fn new() -> Self {
        LeafNode {
            entries_size: 0,
            entries: BTreeMap::new(),
        }
    }

    /// Returns the value for the given key.
    pub fn get(&self, key: &[u8]) -> Option<SlicedCowBytes> {
        self.entries.get(key).cloned()
    }

    pub(in crate::tree) fn entries(&self) -> &BTreeMap<CowBytes, SlicedCowBytes> {
        &self.entries
    }

    fn do_split_off(
        &mut self,
        right_sibling: &mut Self,
        min_size: usize,
        max_size: usize,
    ) -> (CowBytes, isize) {
        assert!(self.size() > max_size);
        assert!(right_sibling.entries_size == 0);

        let mut sibling_size = 0;
        let mut split_key = None;
        for (k, v) in self.entries.iter().rev() {
            sibling_size += k.size() + v.size();
            if 4 + sibling_size >= min_size {
                split_key = Some(k.clone());
                break;
            }
        }
        let split_key = split_key.unwrap();

        right_sibling.entries = self.entries.split_off(&split_key);
        self.entries_size -= sibling_size;
        right_sibling.entries_size = sibling_size;

        let size_delta = -(sibling_size as isize);

        let pivot_key = self.entries.keys().next_back().cloned().unwrap();
        (pivot_key, size_delta)
    }

    /// Inserts a new message as leaf entry.
    pub fn insert<Q, M>(&mut self, key: Q, msg: SlicedCowBytes, msg_action: M) -> isize
    where
        Q: Borrow<[u8]> + Into<CowBytes>,
        M: MessageAction,
    {
        let size_before = self.entries_size as isize;
        let key_size = 8 + key.borrow().len();
        let mut data = self.get(key.borrow());
        msg_action.apply_to_leaf(key.borrow(), msg, &mut data);

        if let Some(data) = data {
            self.entries_size += data.size();

            if let Some(old_data) = self.entries.insert(key.into(), data) {
                self.entries_size -= old_data.size();
            } else {
                self.entries_size += key_size;
            }
        } else if let Some(old_data) = self.entries.remove(key.borrow()) {
            self.entries_size -= key_size;
            self.entries_size -= old_data.size();
        }
        self.entries_size as isize - size_before
    }

    /// Inserts messages as leaf entries.
    pub fn insert_msg_buffer<M, I>(&mut self, msg_buffer: I, msg_action: M) -> isize
    where
        M: MessageAction,
        I: IntoIterator<Item = (CowBytes, SlicedCowBytes)>,
    {
        let mut size_delta = 0;
        for (key, msg) in msg_buffer {
            size_delta += self.insert(key, msg, &msg_action);
        }
        size_delta
    }

    /// Splits this `LeafNode` into to two nodes.
    /// Returns a new right sibling, the corresponding pivot key,
    /// and the size delta of this node.
    pub fn split(&mut self, min_size: usize, max_size: usize) -> (Self, CowBytes, isize) {
        // assert!(self.size() > S::MAX);
        let mut sibling = LeafNode {
            entries_size: 0,
            entries: BTreeMap::new(),
        };

        let (pivot_key, size_delta) = self.do_split_off(&mut sibling, min_size, max_size);

        (sibling, pivot_key, size_delta)
    }

    pub fn merge(&mut self, right_sibling: &mut Self) -> isize {
        self.entries.append(&mut right_sibling.entries);
        let size_delta = right_sibling.entries_size;
        self.entries_size += right_sibling.entries_size;
        right_sibling.entries_size = 0;
        size_delta as isize
    }

    /// Rebalances `self` and `right_sibling`. Returns `Merged`
    /// if all entries of `right_sibling` have been merged into this node.
    /// Otherwise, returns a new pivot key.
    pub fn rebalance(
        &mut self,
        right_sibling: &mut Self,
        min_size: usize,
        max_size: usize,
    ) -> FillUpResult {
        // TODO size delta
        self.merge(right_sibling);
        if self.size() <= max_size {
            FillUpResult::Merged
        } else {
            let (pivot_key, _) = self.do_split_off(right_sibling, min_size, max_size);
            FillUpResult::Rebalanced(pivot_key)
        }
    }

    pub fn range_delete(&mut self, start: &[u8], end: Option<&[u8]>) -> usize {
        // https://github.com/rust-lang/rust/issues/42849
        let size_before = self.entries_size;
        let range = (
            Bound::Included(start),
            end.map_or(Bound::Unbounded, Bound::Excluded),
        );
        let mut keys = Vec::new();
        for (key, value) in self.entries.range_mut::<[u8], _>(range) {
            self.entries_size -= key.size() + value.size();
            keys.push(key.clone());
        }
        for key in keys {
            self.entries.remove(&key);
        }
        size_before - self.entries_size
    }
}

#[cfg(test)]
mod tests {
    use super::{BTreeMap, CowBytes, LeafNode, Size, SlicedCowBytes};
    use crate::tree::imp::packed::PackedMap;
    use crate::tree::message_action::DefaultMessageActionMsg;
    use crate::tree::DefaultMessageAction;
    use quickcheck::{Arbitrary, Gen, TestResult};
    use rand::Rng;

    impl Arbitrary for LeafNode {
        fn arbitrary<G: Gen>(g: &mut G) -> Self {
            let len = g.gen_range(0, 20);
            let entries = (0..len)
                .map(|_| {
                    (
                        Arbitrary::arbitrary(g),
                        DefaultMessageActionMsg::arbitrary(g),
                    )
                })
                .map(|(k, v)| (k, v.0))
                .collect::<BTreeMap<CowBytes, SlicedCowBytes>>();
            LeafNode {
                entries_size: entries
                    .iter()
                    .map(|(key, value)| key.size() + value.size())
                    .sum::<usize>(),
                entries,
            }
        }

        // fn shrink(&self) -> Box<Iterator<Item = Self>> {
        //     Box::new(self.entries.shrink().map(|entries| {
        //         LeafNode {
        //             entries_size: entries
        //                 .iter()
        //                 .map(|(key, value)| key.size() + value.size())
        //                 .sum::<usize>(),
        //             entries,
        //         }
        //     }))
        // }
    }

    fn serialized_size(leaf_node: &LeafNode) -> usize {
        let mut data = Vec::new();
        PackedMap::pack(&leaf_node, &mut data).unwrap();
        data.len()
    }

    #[quickcheck]
    fn check_serialize_size(leaf_node: LeafNode) {
        assert_eq!(leaf_node.size(), serialized_size(&leaf_node));
    }

    #[quickcheck]
    fn check_serialization(leaf_node: LeafNode) {
        let mut data = Vec::new();
        PackedMap::pack(&leaf_node, &mut data).unwrap();
        let twin = PackedMap::new(data).unpack_leaf();

        assert_eq!(leaf_node, twin);
    }

    #[quickcheck]
    fn check_size_insert(mut leaf_node: LeafNode, key: CowBytes, msg: DefaultMessageActionMsg) {
        let size_before = leaf_node.size();
        let size_delta = leaf_node.insert(key, msg.0, DefaultMessageAction);
        let size_after = leaf_node.size();
        assert_eq!((size_before as isize + size_delta) as usize, size_after);
        assert_eq!(serialized_size(&leaf_node) as usize, size_after);
    }

    const MIN_LEAF_SIZE: usize = 512;
    const MAX_LEAF_SIZE: usize = 2048;

    #[quickcheck]
    fn check_size_split(mut leaf_node: LeafNode) -> TestResult {
        let size_before = leaf_node.size();

        if size_before <= MAX_LEAF_SIZE {
            return TestResult::discard();
        }

        let (sibling, _, size_delta) = leaf_node.split(MIN_LEAF_SIZE, MAX_LEAF_SIZE);
        assert_eq!(serialized_size(&leaf_node) as usize, leaf_node.size());
        assert_eq!(serialized_size(&sibling) as usize, sibling.size());
        assert_eq!(
            (size_before as isize + size_delta) as usize,
            leaf_node.size()
        );
        assert!(sibling.size() <= MAX_LEAF_SIZE);
        assert!(sibling.size() >= MIN_LEAF_SIZE);
        assert!(leaf_node.size() >= MIN_LEAF_SIZE);
        TestResult::passed()
    }

    #[quickcheck]
    fn check_split_merge_idempotent(mut leaf_node: LeafNode) -> TestResult {
        if leaf_node.size() <= MAX_LEAF_SIZE {
            return TestResult::discard();
        }
        let this = leaf_node.clone();
        let (mut sibling, _, _) = leaf_node.split(MIN_LEAF_SIZE, MAX_LEAF_SIZE);
        leaf_node.merge(&mut sibling);
        assert_eq!(this, leaf_node);
        TestResult::passed()
    }

    // TODO
    // check size deltas for various operations (insert, delete, rebalance,
    // fill_up...)
    // check insert
    // check rebalance, fill_up

}

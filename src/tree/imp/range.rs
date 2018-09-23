use super::node::{GetRangeResult, Node};
use super::{Inner, Tree};
use cow_bytes::{CowBytes, SlicedCowBytes};
use data_management::{Dml, ObjectRef};
use std::borrow::Borrow;
use std::collections::{BTreeMap, Bound, VecDeque};
use std::mem::replace;
use std::ops::RangeBounds;
use tree::errors::*;
use tree::MessageAction;

fn next(v: &mut Vec<u8>) {
    v.push(0);
}

#[derive(Debug, Clone, Copy)]
enum MyBound<T> {
    Included(T),
    Excluded(T),
}

/// The range iterator of a tree.
pub struct RangeIterator<X: Dml, M, I: Borrow<Inner<X::ObjectRef, X::Info, M>>> {
    buffer: VecDeque<(CowBytes, SlicedCowBytes)>,
    min_key: MyBound<Vec<u8>>,
    /// Always inclusive
    max_key: Option<Vec<u8>>,
    tree: Tree<X, M, I>,
    finished: bool,
    prefetch: Option<X::Prefetch>,
}

impl<X, R, M, I> Iterator for RangeIterator<X, M, I>
where
    X: Dml<Object = Node<R>, ObjectRef = R>,
    R: ObjectRef<ObjectPointer = X::ObjectPointer>,
    M: MessageAction,
    I: Borrow<Inner<X::ObjectRef, X::Info, M>>,
{
    type Item = Result<(CowBytes, SlicedCowBytes), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(x) = self.buffer.pop_front() {
                return Some(Ok(x));
            } else if self.finished {
                return None;
            } else if let Err(e) = self.fill_buffer() {
                self.finished = true;
                return Some(Err(e));
            }
        }
    }
}

impl<X, R, M, I> RangeIterator<X, M, I>
where
    X: Dml<Object = Node<R>, ObjectRef = R>,
    R: ObjectRef<ObjectPointer = X::ObjectPointer>,
    M: MessageAction,
    I: Borrow<Inner<X::ObjectRef, X::Info, M>>,
{
    pub(super) fn new<K, T>(range: T, tree: Tree<X, M, I>) -> Self
    where
        T: RangeBounds<K>,
        K: Borrow<[u8]>,
    {
        let min_key = match range.start() {
            Bound::Unbounded => MyBound::Included(Vec::new()),
            Bound::Included(x) => MyBound::Included(x.borrow().to_vec()),
            Bound::Excluded(x) => MyBound::Excluded(x.borrow().to_vec()),
        };
        let max_key = match range.end() {
            Bound::Unbounded => None,
            Bound::Included(x) => Some(x.borrow().to_vec()),
            Bound::Excluded(x) => {
                let mut v = x.borrow().to_vec();
                next(&mut v);
                Some(v)
            }
        };

        RangeIterator {
            min_key,
            max_key,
            tree,
            finished: false,
            buffer: VecDeque::new(),
            prefetch: None,
        }
    }

    fn fill_buffer(&mut self) -> Result<(), Error> {
        let pivot_option = {
            let min_key = match self.min_key {
                MyBound::Included(ref x) | MyBound::Excluded(ref x) => x,
            };
            self.tree
                .leaf_range_query(min_key, &mut self.buffer, &mut self.prefetch)?
        };

        // Strip entries which are out of bounds from the buffer.
        while self
            .buffer
            .front()
            .map(|&(ref key, _)| match self.min_key {
                MyBound::Included(ref min_key) => key < min_key,
                MyBound::Excluded(ref min_key) => key <= min_key,
            }).unwrap_or_default()
        {
            self.buffer.pop_front().unwrap();
        }
        if let Some(ref max_key) = self.max_key {
            while self
                .buffer
                .back()
                .map(|&(ref key, _)| key > max_key)
                .unwrap_or_default()
            {
                self.buffer.pop_back().unwrap();
            }
        }
        // TODO clean up
        if let Some(pivot) = pivot_option {
            if self.max_key.is_some() && Some(&pivot[..]) >= self.max_key.as_ref().map(|v| &v[..]) {
                // If the pivot key is equal or greater to our max key then this was the last
                // leaf
                // that contains data for this range query.
                self.finished = true;
            } else {
                // Otherwise we will update our min key for the next fill_buffer call.
                let mut last_key = pivot.to_vec();
                // `last_key` is actually exact.
                // There are no values on this path we have not seen.
                next(&mut last_key);
                self.min_key = MyBound::Excluded(last_key);
            }
        } else {
            // If there is no pivot key then this was the right-most leaf in the tree.
            self.finished = true;
        }

        Ok(())
    }
}

impl<X, R, M, I> Tree<X, M, I>
where
    X: Dml<Object = Node<R>, ObjectRef = R>,
    R: ObjectRef<ObjectPointer = X::ObjectPointer>,
    M: MessageAction,
    I: Borrow<Inner<X::ObjectRef, X::Info, M>>,
{
    fn leaf_range_query(
        &self,
        key: &[u8],
        data: &mut VecDeque<(CowBytes, SlicedCowBytes)>,
        prefetch: &mut Option<X::Prefetch>,
    ) -> Result<Option<CowBytes>, Error> {
        let result = {
            let mut left_pivot_key = None;
            let mut right_pivot_key = None;
            let mut messages = BTreeMap::new();

            // First, we gather all messages for the given key and its value in the leaf.
            let mut node = self.get_root_node()?;

            loop {
                let next_node = match node.get_range(
                    key,
                    &mut left_pivot_key,
                    &mut right_pivot_key,
                    &mut messages,
                ) {
                    GetRangeResult::NextNode {
                        prefetch_option,
                        np,
                    } => {
                        let previous_prefetch = if let Some(prefetch_np) = prefetch_option {
                            let f = self.dml.prefetch(&prefetch_np.read())?;
                            replace(prefetch, f)
                        } else {
                            prefetch.take()
                        };
                        if let Some(previous_prefetch) = previous_prefetch {
                            self.dml.finish_prefetch(previous_prefetch)?;
                        }
                        self.get_node(np)?
                    }
                    GetRangeResult::Data(leaf_entries) => {
                        self.apply_messages(
                            &left_pivot_key,
                            &right_pivot_key,
                            messages,
                            leaf_entries,
                            data,
                        );
                        break Ok(right_pivot_key);
                    }
                };
                node = next_node;
            }
        };

        if self.evict {
            self.dml.evict()?;
        }
        result
    }

    fn apply_messages<'a, J>(
        &self,
        left_pivot_key: &Option<CowBytes>,
        right_pivot_key: &Option<CowBytes>,
        messages: BTreeMap<CowBytes, Vec<SlicedCowBytes>>,
        leaf_entries: J,
        data: &mut VecDeque<(CowBytes, SlicedCowBytes)>,
    ) where
        J: Iterator<Item = (&'a [u8], SlicedCowBytes)>,
    {
        // disregard any messages with keys outside of
        // left_pivot_key..right_pivot_key.
        let msgs_iter = messages
            .into_iter()
            .skip_while(|&(ref key, _)| match *left_pivot_key {
                None => false,
                Some(ref min_key) => key < min_key,
            }).take_while(|&(ref key, _)| match *right_pivot_key {
                None => true,
                Some(ref max_key) => key <= max_key,
            });
        // TODO
        let leaf_entries = leaf_entries.map(|(k, v)| (CowBytes::from(k), v));

        for (key, msgs, mut value) in MergeByKeyIterator::new(msgs_iter, leaf_entries) {
            if let Some(msgs) = msgs {
                for msg in msgs.into_iter().rev() {
                    self.msg_action().apply(&key, &msg, &mut value);
                }
            }
            if let Some(value) = value {
                data.push_back((key, value));
            }
        }
    }
}

struct MergeByKeyIterator<I: Iterator, J: Iterator> {
    i: I,
    j: J,
    peeked_i: Option<I::Item>,
    peeked_j: Option<J::Item>,
}

impl<I: Iterator, J: Iterator> MergeByKeyIterator<I, J> {
    pub fn new<A, B>(i: A, j: B) -> Self
    where
        A: IntoIterator<Item = I::Item, IntoIter = I>,
        B: IntoIterator<Item = J::Item, IntoIter = J>,
    {
        MergeByKeyIterator {
            i: i.into_iter(),
            j: j.into_iter(),
            peeked_i: None,
            peeked_j: None,
        }
    }
}

impl<I, J, K, T, U> Iterator for MergeByKeyIterator<I, J>
where
    I: Iterator<Item = (K, T)>,
    J: Iterator<Item = (K, U)>,
    K: Ord,
{
    type Item = (K, Option<T>, Option<U>);

    fn next(&mut self) -> Option<Self::Item> {
        use std::cmp::Ordering;
        let i = self.peeked_i.take().or_else(|| self.i.next());
        let j = self.peeked_j.take().or_else(|| self.j.next());

        let (k_i, v_i, k_j, v_j) = match (i, j) {
            (None, None) => return None,
            (Some((k, v)), None) => return Some((k, Some(v), None)),
            (None, Some((k, v))) => return Some((k, None, Some(v))),
            (Some((k_i, v_i)), Some((k_j, v_j))) => (k_i, v_i, k_j, v_j),
        };

        Some(match k_i.cmp(&k_j) {
            Ordering::Equal => (k_i, Some(v_i), Some(v_j)),
            Ordering::Less => {
                self.peeked_j = Some((k_j, v_j));
                (k_i, Some(v_i), None)
            }
            Ordering::Greater => {
                self.peeked_i = Some((k_i, v_i));
                (k_j, None, Some(v_j))
            }
        })
    }
}

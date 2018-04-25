use super::child_buffer::ChildBuffer;
use cow_bytes::{CowBytes, SlicedCowBytes};
use parking_lot::RwLock;
use size::{Size, SizeMut, StaticSize};
use std::borrow::Borrow;
use std::cmp;
use std::collections::BTreeMap;
use std::mem::replace;
use tree::MessageAction;

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(test, derive(PartialEq))]
pub(super) struct InternalNode<T> {
    level: u32,
    entries_size: usize,
    pivot: Vec<CowBytes>,
    children: Vec<T>,
}

impl<T> Size for InternalNode<T> {
    fn size(&self) -> usize {
        28 + self.entries_size
    }
}

impl<N: StaticSize> InternalNode<ChildBuffer<N>> {
    pub(super) fn actual_size(&self) -> usize {
        28 + self.pivot.iter().map(Size::size).sum::<usize>()
            + self.children
                .iter()
                .map(|child| child.actual_size())
                .sum::<usize>()
    }
}

fn binary_search_by<'a, T, F>(s: &'a [T], mut f: F) -> Result<usize, usize>
where
    F: FnMut(&'a T) -> cmp::Ordering,
{
    use std::cmp::Ordering::*;
    let mut size = s.len();
    if size == 0 {
        return Err(0);
    }
    let mut base = 0usize;
    while size > 1 {
        let half = size / 2;
        let mid = base + half;
        let cmp = f(&s[mid]);
        base = if cmp == Greater { base } else { mid };
        size -= half;
    }
    let cmp = f(&s[base]);
    if cmp == Equal {
        Ok(base)
    } else {
        Err(base + (cmp == Less) as usize)
    }
}

impl<T> InternalNode<T> {
    pub fn new(left_child: T, right_child: T, pivot_key: CowBytes, level: u32) -> Self
    where
        T: Size,
    {
        InternalNode {
            level,
            entries_size: left_child.size() + right_child.size() + pivot_key.size(),
            pivot: vec![pivot_key],
            children: vec![left_child, right_child],
        }
    }

    /// Returns the number of children.
    pub fn fanout(&self) -> usize {
        self.children.len()
    }

    /// Returns the level of this node.
    pub fn level(&self) -> u32 {
        self.level
    }

    /// Returns the index of the child buffer
    /// corresponding to the given `key`.
    fn idx(&self, key: &[u8]) -> usize {
        match binary_search_by(&self.pivot, |pivot_key| pivot_key.as_ref().cmp(key)) {
            Ok(idx) | Err(idx) => idx,
        }
    }

    pub fn iter_mut<'a>(&'a mut self) -> impl Iterator<Item = &'a mut T> + 'a {
        self.children.iter_mut()
    }
}

impl<N> InternalNode<ChildBuffer<N>> {
    pub fn get(&self, key: &[u8]) -> (&RwLock<N>, Option<SlicedCowBytes>) {
        // TODO Merge range messages into msg stream
        let child = &self.children[self.idx(key)];

        let msg = child.get(key).cloned();
        (&child.node_pointer, msg)
    }

    pub fn get_range(
        &self,
        key: &[u8],
        left_pivot_key: &mut Option<CowBytes>,
        right_pivot_key: &mut Option<CowBytes>,
        all_msgs: &mut BTreeMap<CowBytes, Vec<SlicedCowBytes>>,
    ) -> &RwLock<N> {
        // TODO Merge range messages into msg stream
        let idx = self.idx(key);
        if idx > 0 {
            *left_pivot_key = Some(self.pivot[idx - 1].clone());
        }
        if idx < self.pivot.len() {
            *right_pivot_key = Some(self.pivot[idx].clone());
        }
        let child = &self.children[idx];
        for (key, msg) in child.get_all_messages() {
            all_msgs
                .entry(key.clone())
                .or_insert_with(Vec::new)
                .push(msg.clone());
        }

        &child.node_pointer
    }

    pub fn get_next_node(&self, key: &[u8]) -> Option<&RwLock<N>> {
        let idx = self.idx(key) + 1;
        self.children.get(idx).map(|child| &child.node_pointer)
    }

    pub fn insert<Q, M>(&mut self, key: Q, msg: SlicedCowBytes, msg_action: M) -> isize
    where
        Q: Borrow<[u8]> + Into<CowBytes>,
        M: MessageAction,
    {
        let idx = self.idx(key.borrow());
        let added_size = self.children[idx].insert(key, msg, msg_action);
        if added_size > 0 {
            self.entries_size += added_size as usize;
        } else {
            self.entries_size -= -added_size as usize;
        }
        added_size
    }

    pub fn insert_msg_buffer<I, M>(&mut self, iter: I, msg_action: M) -> isize
    where
        I: IntoIterator<Item = (CowBytes, SlicedCowBytes)>,
        M: MessageAction,
    {
        let mut added_size = 0;
        let mut iter = iter.into_iter();

        let mut current = match iter.next() {
            Some(x) => x,
            None => return 0,
        };
        let mut idx = self.idx(&current.0);
        loop {
            added_size += self.children[idx].insert(current.0, current.1, &msg_action);
            current = match iter.next() {
                Some(x) => x,
                None => break,
            };
            let new_idx = self.idx(&current.0);
            if idx != new_idx {
                idx = new_idx;
            }
        }
        if added_size > 0 {
            self.entries_size += added_size as usize;
        } else {
            self.entries_size -= -added_size as usize;
        }
        added_size
    }

    pub fn drain_children<'a>(&'a mut self) -> impl Iterator<Item = N> + 'a {
        self.children
            .drain(..)
            .map(|child| child.node_pointer.into_inner())
    }
}

impl<N: StaticSize> InternalNode<ChildBuffer<N>> {
    pub fn range_delete(
        &mut self,
        start: &[u8],
        end: Option<&[u8]>,
        dead: &mut Vec<N>,
    ) -> (usize, &mut N, Option<&mut N>) {
        let size_before = self.entries_size;
        let start_idx = self.idx(start);
        let end_idx = end.map_or(self.children.len() - 1, |i| self.idx(i));
        if start_idx == end_idx {
            let size_delta = self.children[start_idx].range_delete(start, end);
            return (
                size_delta,
                self.children[start_idx].node_pointer.get_mut(),
                None,
            );
        }
        // Skip children that may overlap.
        let dead_start_idx = start_idx + 1;
        let dead_end_idx = end_idx - end.is_some() as usize;
        if dead_start_idx <= dead_end_idx {
            for pivot_key in self.pivot.drain(dead_start_idx..dead_end_idx) {
                self.entries_size -= pivot_key.size();
            }
            let entries_size = &mut self.entries_size;
            dead.extend(
                self.children
                    .drain(dead_start_idx..dead_end_idx + 1)
                    .map(|child| {
                        *entries_size -= child.size();
                        child.node_pointer.into_inner()
                    }),
            );
        }

        let (left_child, mut right_child) = {
            let (left, right) = self.children.split_at_mut(start_idx + 1);
            (&mut left[start_idx], end.map(move |_| &mut right[0]))
        };
        self.entries_size -= left_child.range_delete(start, None);
        if let Some(ref mut child) = right_child {
            self.entries_size -= child.range_delete(start, end);
        }
        let size_delta = size_before - self.entries_size;

        (
            size_delta,
            left_child.node_pointer.get_mut(),
            right_child.map(|child| child.node_pointer.get_mut()),
        )
    }
}

impl<T: Size> InternalNode<T> {
    pub fn split(&mut self) -> (Self, CowBytes, isize) {
        let split_off_idx = self.fanout() / 2;
        let pivot = self.pivot.split_off(split_off_idx);
        let pivot_key = self.pivot.pop().unwrap();
        let mut children = self.children.split_off(split_off_idx);

        let entries_size = pivot.iter().map(Size::size).sum::<usize>()
            + children.iter_mut().map(SizeMut::size).sum::<usize>();

        let size_delta = entries_size + pivot_key.size();
        self.entries_size -= size_delta;

        let right_sibling = InternalNode {
            level: self.level,
            entries_size,
            pivot,
            children,
        };
        (right_sibling, pivot_key, -(size_delta as isize))
    }

    pub fn merge(&mut self, right_sibling: &mut Self, old_pivot_key: CowBytes) -> isize {
        let size_delta = right_sibling.entries_size + old_pivot_key.size();
        self.entries_size += size_delta;
        self.pivot.push(old_pivot_key);
        self.pivot.append(&mut right_sibling.pivot);
        self.children.append(&mut right_sibling.children);
        size_delta as isize
    }
}

impl<N> InternalNode<ChildBuffer<N>> {
    pub fn try_walk(&mut self, key: &[u8]) -> Option<TakeChildBuffer<ChildBuffer<N>>> {
        let child_idx = self.idx(key);
        if self.children[child_idx].is_empty(key) {
            Some(TakeChildBuffer {
                node: self,
                child_idx,
            })
        } else {
            None
        }
    }
    pub fn try_flush(
        &mut self,
        min_flush_size: usize,
        max_node_size: usize,
        min_fanout: usize,
    ) -> Option<TakeChildBuffer<ChildBuffer<N>>> {
        let child_idx = {
            let size = self.size();
            let fanout = self.fanout();
            let (child_idx, child) = self.children
                .iter()
                .enumerate()
                .max_by_key(|&(_, child)| child.buffer_size())
                .unwrap();

            debug!("Largest child's buffer size: {}", child.buffer_size());

            if child.buffer_size() >= min_flush_size
                && (size - child.buffer_size() <= max_node_size || fanout < 2 * min_fanout)
            {
                Some(child_idx)
            } else {
                None
            }
        };
        child_idx.map(move |child_idx| TakeChildBuffer {
            node: self,
            child_idx,
        })
    }
}

pub(super) struct TakeChildBuffer<'a, T: 'a> {
    node: &'a mut InternalNode<T>,
    child_idx: usize,
}

impl<'a, N: StaticSize> TakeChildBuffer<'a, ChildBuffer<N>> {
    pub(super) fn split_child(
        &mut self,
        sibling_np: N,
        pivot_key: CowBytes,
        select_right: bool,
    ) -> isize {
        let sibling = self.node.children[self.child_idx].split_at(&pivot_key, sibling_np);
        let size_delta = sibling.size() + pivot_key.size();
        self.node.children.insert(self.child_idx + 1, sibling);
        self.node.pivot.insert(self.child_idx, pivot_key);
        self.node.entries_size += size_delta;
        if select_right {
            self.child_idx += 1;
        }
        size_delta as isize
    }
}

impl<'a, T> TakeChildBuffer<'a, T> {
    pub(super) fn size(&self) -> usize {
        Size::size(&*self.node)
    }

    pub(super) fn prepare_merge(&mut self) -> PrepareMergeChild<T> {
        if self.child_idx + 1 < self.node.children.len() {
            PrepareMergeChild {
                node: self.node,
                pivot_key_idx: self.child_idx,
                other_child_idx: self.child_idx + 1,
            }
        } else {
            PrepareMergeChild {
                node: self.node,
                pivot_key_idx: self.child_idx - 1,
                other_child_idx: self.child_idx - 1,
            }
        }
    }
}

pub(super) struct PrepareMergeChild<'a, T: 'a> {
    node: &'a mut InternalNode<T>,
    pivot_key_idx: usize,
    other_child_idx: usize,
}

impl<'a, N> PrepareMergeChild<'a, ChildBuffer<N>> {
    pub(super) fn sibling_node_pointer(&mut self) -> &mut RwLock<N> {
        &mut self.node.children[self.other_child_idx].node_pointer
    }
    pub(super) fn is_right_sibling(&self) -> bool {
        self.pivot_key_idx != self.other_child_idx
    }
}
impl<'a, N: Size> PrepareMergeChild<'a, ChildBuffer<N>> {
    pub(super) fn merge_children(self) -> (CowBytes, N, isize) {
        let mut right_sibling = self.node.children.remove(self.pivot_key_idx + 1);
        let pivot_key = self.node.pivot.remove(self.pivot_key_idx);
        let size_delta =
            pivot_key.size() + ChildBuffer::<N>::static_size() + right_sibling.node_pointer.size();
        self.node.entries_size -= size_delta;

        let left_sibling = &mut self.node.children[self.pivot_key_idx];
        left_sibling.append(&mut right_sibling);

        (
            pivot_key,
            right_sibling.node_pointer.into_inner(),
            -(size_delta as isize),
        )
    }
}

impl<'a, N: Size> PrepareMergeChild<'a, ChildBuffer<N>> {
    fn get_children(&mut self) -> (&mut ChildBuffer<N>, &mut ChildBuffer<N>) {
        let (left, right) = self.node.children[self.pivot_key_idx..].split_at_mut(1);
        (&mut left[0], &mut right[0])
    }
    pub(super) fn rebalanced(&mut self, new_pivot_key: CowBytes) -> isize {
        {
            // Move messages around
            let (left_child, right_child) = self.get_children();
            left_child.rebalance(right_child, &new_pivot_key);
        }

        let mut size_delta = new_pivot_key.size() as isize;
        let old_pivot_key = replace(&mut self.node.pivot[self.pivot_key_idx], new_pivot_key);
        size_delta -= old_pivot_key.size() as isize;

        size_delta
    }
}

impl<'a, N: Size> TakeChildBuffer<'a, ChildBuffer<N>> {
    pub fn node_pointer_mut(&mut self) -> &mut RwLock<N> {
        &mut self.node.children[self.child_idx].node_pointer
    }
    pub fn take_buffer(&mut self) -> (BTreeMap<CowBytes, SlicedCowBytes>, isize) {
        let (buffer, size_delta) = self.node.children[self.child_idx].take();
        self.node.entries_size -= size_delta;
        (buffer, -(size_delta as isize))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bincode::serialized_size;
    use quickcheck::{Arbitrary, Gen, TestResult};
    use serde::Serialize;
    use tree::DefaultMessageAction;
    use tree::message_action::DefaultMessageActionMsg;

    impl<T: Clone> Clone for InternalNode<T> {
        fn clone(&self) -> Self {
            InternalNode {
                level: self.level,
                entries_size: self.entries_size,
                pivot: self.pivot.clone(),
                children: self.children.iter().cloned().collect(),
            }
        }
    }

    impl<T: Arbitrary + Size> Arbitrary for InternalNode<T> {
        fn arbitrary<G: Gen>(g: &mut G) -> Self {
            let pivot_key_cnt = g.gen_range(1, 20);
            let mut entries_size = 0;

            let mut pivot = Vec::with_capacity(pivot_key_cnt);
            for _ in 0..pivot_key_cnt {
                let pivot_key = CowBytes::arbitrary(g);
                entries_size += pivot_key.size();
                pivot.push(pivot_key);
            }

            let mut children = Vec::with_capacity(pivot_key_cnt + 1);
            for _ in 0..pivot_key_cnt + 1 {
                let child = T::arbitrary(g);
                entries_size += child.size();
                children.push(child);
            }

            InternalNode {
                pivot,
                children,
                entries_size,
                level: 1,
            }
        }
    }

    fn check_size<T: Serialize>(node: &mut InternalNode<T>) {
        assert_eq!(node.size() as u64, serialized_size(node).unwrap());
    }

    #[quickcheck]
    fn check_serialize_size(mut node: InternalNode<CowBytes>) {
        check_size(&mut node);
    }

    #[quickcheck]
    fn check_idx(node: InternalNode<()>, key: CowBytes) {
        let idx = node.idx(&key);

        if let Some(upper_key) = node.pivot.get(idx) {
            assert!(&key <= upper_key);
        }
        if idx > 0 {
            let lower_key = &node.pivot[idx - 1];
            assert!(lower_key < &key);
        }
    }

    #[quickcheck]
    fn check_size_insert(
        mut node: InternalNode<ChildBuffer<()>>,
        key: CowBytes,
        msg: DefaultMessageActionMsg,
    ) {
        let size_before = node.size() as isize;
        let added_size = node.insert(key, msg.0, DefaultMessageAction);
        assert_eq!(size_before + added_size, node.size() as isize);

        check_size(&mut node);
    }

    #[quickcheck]
    fn check_size_insert_msg_buffer(
        mut node: InternalNode<ChildBuffer<()>>,
        buffer: BTreeMap<CowBytes, DefaultMessageActionMsg>,
    ) {
        let size_before = node.size() as isize;
        let added_size = node.insert_msg_buffer(
            buffer.into_iter().map(|(key, msg)| (key, msg.0)),
            DefaultMessageAction,
        );
        assert_eq!(size_before + added_size, node.size() as isize);

        check_size(&mut node);
    }

    #[quickcheck]
    fn check_insert_msg_buffer(
        mut node: InternalNode<ChildBuffer<()>>,
        buffer: BTreeMap<CowBytes, DefaultMessageActionMsg>,
    ) {
        let mut node_twin = node.clone();
        let added_size = node.insert_msg_buffer(
            buffer.iter().map(|(key, msg)| (key.clone(), msg.0.clone())),
            DefaultMessageAction,
        );

        let mut added_size_twin = 0;
        for (key, msg) in buffer {
            let idx = node_twin.idx(&key);
            added_size_twin += node_twin.children[idx].insert(key, msg.0, DefaultMessageAction);
        }
        if added_size_twin > 0 {
            node_twin.entries_size += added_size_twin as usize;
        } else {
            node_twin.entries_size -= -added_size_twin as usize;
        }

        assert_eq!(node, node_twin);
    }

    fn check_size_split(mut node: InternalNode<ChildBuffer<()>>) -> TestResult {
        if node.fanout() < 2 {
            return TestResult::discard();
        }
        let size_before = node.size();
        let (mut right_sibling, pivot_key, size_delta) = node.split();
        assert_eq!(size_before as isize + size_delta, node.size() as isize);
        check_size(&mut node);
        check_size(&mut right_sibling);

        TestResult::passed()
    }

    #[quickcheck]
    fn check_split(mut node: InternalNode<ChildBuffer<()>>) -> TestResult {
        if node.fanout() < 4 {
            return TestResult::discard();
        }
        let twin = node.clone();
        let (mut right_sibling, pivot_key, size_delta) = node.split();

        assert!(node.fanout() >= 2);
        assert!(right_sibling.fanout() >= 2);

        node.entries_size += pivot_key.size() + right_sibling.entries_size;
        node.pivot.push(pivot_key);
        node.pivot.append(&mut right_sibling.pivot);
        node.children.append(&mut right_sibling.children);

        assert_eq!(node, twin);

        TestResult::passed()
    }

    // TODO tests
    // split
    // child split
    // flush buffer
    // get with max_msn

}

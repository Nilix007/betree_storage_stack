use self::Inner::*;
use super::child_buffer::ChildBuffer;
use super::internal::{InternalNode, TakeChildBuffer};
use super::leaf::LeafNode;
use super::packed::PackedMap;
use super::{
    FillUpResult, MAX_INTERNAL_NODE_SIZE, MAX_LEAF_NODE_SIZE, MIN_FANOUT, MIN_FLUSH_SIZE,
    MIN_LEAF_NODE_SIZE,
};
use crate::cow_bytes::{CowBytes, SlicedCowBytes};
use crate::data_management::{Object, ObjectRef};
use crate::size::{Size, SizeMut, StaticSize};
use crate::tree::MessageAction;
use bincode::{deserialize, serialize_into};
use parking_lot::RwLock;
use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::io::{self, Write};
use std::mem::replace;

/// The tree node type.
#[derive(Debug)]
pub struct Node<N: 'static>(Inner<N>);

#[derive(Debug)]
pub(super) enum Inner<N: 'static> {
    PackedLeaf(PackedMap),
    Leaf(LeafNode),
    Internal(InternalNode<ChildBuffer<N>>),
}

impl<R: ObjectRef> Object<R> for Node<R> {
    fn pack<W: Write>(&self, mut writer: W) -> Result<(), io::Error> {
        match self.0 {
            PackedLeaf(ref map) => writer.write_all(map.inner()),
            Leaf(ref leaf) => PackedMap::pack(leaf, writer),
            Internal(ref internal) => {
                writer.write_all(&[0xFFu8, 0xFF, 0xFF, 0xFF] as &[u8])?;
                serialize_into(writer, internal)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
            }
        }
    }

    fn unpack(data: Box<[u8]>) -> Result<Self, io::Error> {
        if data[..4] == [0xFFu8, 0xFF, 0xFF, 0xFF] {
            match deserialize(&data[4..]) {
                Ok(internal) => Ok(Node(Internal(internal))),
                Err(e) => Err(io::Error::new(io::ErrorKind::InvalidData, e)),
            }
        } else {
            Ok(Node(PackedLeaf(PackedMap::new(data.into_vec()))))
        }
    }

    fn debug_info(&self) -> String {
        format!(
            "{}: {:?}, {}, {}",
            self.kind(),
            self.fanout(),
            self.size(),
            self.actual_size()
        )
    }

    fn for_each_child<E, F>(&mut self, mut f: F) -> Result<(), E>
    where
        F: FnMut(&mut R) -> Result<(), E>,
    {
        if let Some(iter) = self.child_pointer_iter() {
            for np in iter {
                f(np)?;
            }
        }
        Ok(())
    }
}

impl<N> Size for Node<N> {
    fn size(&self) -> usize {
        match self.0 {
            PackedLeaf(ref map) => map.size(),
            Leaf(ref leaf) => leaf.size(),
            Internal(ref internal) => 4 + internal.size(),
        }
    }
}

impl<N: StaticSize> Node<N> {
    pub(super) fn try_walk(&mut self, key: &[u8]) -> Option<TakeChildBuffer<ChildBuffer<N>>> {
        match self.0 {
            Leaf(_) | PackedLeaf(_) => None,
            Internal(ref mut internal) => internal.try_walk(key),
        }
    }

    pub(super) fn try_flush(&mut self) -> Option<TakeChildBuffer<ChildBuffer<N>>> {
        match self.0 {
            Leaf(_) | PackedLeaf(_) => None,
            Internal(ref mut internal) => {
                internal.try_flush(MIN_FLUSH_SIZE, MAX_INTERNAL_NODE_SIZE, MIN_FANOUT)
            }
        }
    }
}

impl<N: StaticSize> Node<N> {
    pub(super) fn actual_size(&self) -> usize {
        match self.0 {
            PackedLeaf(ref map) => map.size(),
            Leaf(ref leaf) => leaf.actual_size(),
            Internal(ref internal) => internal.actual_size(),
        }
    }
}
impl<N> Node<N> {
    pub(super) fn kind(&self) -> &str {
        match self.0 {
            PackedLeaf(_) => "packed leaf",
            Leaf(_) => "leaf",
            Internal(_) => "internal",
        }
    }
    pub(super) fn fanout(&self) -> Option<usize> {
        match self.0 {
            Leaf(_) | PackedLeaf(_) => None,
            Internal(ref internal) => Some(internal.fanout()),
        }
    }

    fn ensure_unpacked(&mut self) {
        let leaf = if let PackedLeaf(ref mut map) = self.0 {
            map.unpack_leaf()
        } else {
            return;
        };
        *self = Node(Leaf(leaf));
    }

    fn take(&mut self) -> Self {
        replace(self, Self::empty_leaf())
    }

    pub(super) fn has_too_low_fanout(&self) -> bool {
        match self.0 {
            Leaf(_) | PackedLeaf(_) => false,
            Internal(ref internal) => internal.fanout() < MIN_FANOUT,
        }
    }

    pub(super) fn is_too_small_leaf(&self) -> bool {
        match self.0 {
            PackedLeaf(ref map) => map.size() < MIN_LEAF_NODE_SIZE,
            Leaf(ref leaf) => leaf.size() < MIN_LEAF_NODE_SIZE,
            Internal(_) => false,
        }
    }

    pub(super) fn is_too_large_leaf(&self) -> bool {
        match self.0 {
            PackedLeaf(ref map) => map.size() > MAX_LEAF_NODE_SIZE,
            Leaf(ref leaf) => leaf.size() > MAX_LEAF_NODE_SIZE,
            Internal(_) => false,
        }
    }

    pub(super) fn is_too_large(&self) -> bool {
        match self.0 {
            PackedLeaf(ref map) => map.size() > MAX_LEAF_NODE_SIZE,
            Leaf(ref leaf) => leaf.size() > MAX_LEAF_NODE_SIZE,
            Internal(ref internal) => internal.size() > MAX_INTERNAL_NODE_SIZE,
        }
    }

    pub(super) fn is_leaf(&self) -> bool {
        match self.0 {
            Leaf(_) | PackedLeaf(_) => true,
            Internal(_) => false,
        }
    }

    pub(super) fn empty_leaf() -> Self {
        Node(Leaf(LeafNode::new()))
    }

    pub(super) fn level(&self) -> u32 {
        match self.0 {
            Leaf(_) | PackedLeaf(_) => 0,
            Internal(ref internal) => internal.level(),
        }
    }

    pub(super) fn root_needs_merge(&self) -> bool {
        match self.0 {
            Leaf(_) | PackedLeaf(_) => false,
            Internal(ref internal) => internal.fanout() == 1,
        }
    }
}

impl<N: StaticSize> Node<N> {
    pub(super) fn split_root_mut<F>(&mut self, allocate_obj: F) -> isize
    where
        F: Fn(Self) -> N,
    {
        let size_before = self.size();
        self.ensure_unpacked();
        let mut left_sibling = self.take();
        let (right_sibling, pivot_key, cur_level) = match left_sibling.0 {
            PackedLeaf(_) => unreachable!(),
            Leaf(ref mut leaf) => {
                let (right_sibiling, pivot_key, _) =
                    leaf.split(MIN_LEAF_NODE_SIZE, MAX_LEAF_NODE_SIZE);
                (Node(Leaf(right_sibiling)), pivot_key, 0)
            }
            Internal(ref mut internal) => {
                let (right_sibiling, pivot_key, _) = internal.split();
                (Node(Internal(right_sibiling)), pivot_key, internal.level())
            }
        };
        debug!("Root split pivot key: {:?}", pivot_key);
        *self = Node(Internal(InternalNode::new(
            ChildBuffer::new(allocate_obj(left_sibling)),
            ChildBuffer::new(allocate_obj(right_sibling)),
            pivot_key,
            cur_level + 1,
        )));
        let size_after = self.size();
        size_after as isize - size_before as isize
    }
}

pub(super) enum GetResult<'a, N: 'a> {
    Data(Option<SlicedCowBytes>),
    NextNode(&'a RwLock<N>),
}

pub(super) enum GetRangeResult<'a, T, N: 'a> {
    Data(T),
    NextNode {
        np: &'a RwLock<N>,
        prefetch_option: Option<&'a RwLock<N>>,
    },
}

impl<N> Node<N> {
    pub(super) fn get(&self, key: &[u8], msgs: &mut Vec<SlicedCowBytes>) -> GetResult<N> {
        match self.0 {
            PackedLeaf(ref map) => GetResult::Data(map.get(key)),
            Leaf(ref leaf) => GetResult::Data(leaf.get(key)),
            Internal(ref internal) => {
                let (child_np, msg) = internal.get(key);
                if let Some(msg) = msg {
                    msgs.push(msg);
                }
                GetResult::NextNode(child_np)
            }
        }
    }

    pub(super) fn get_range<'a>(
        &'a self,
        key: &[u8],
        left_pivot_key: &mut Option<CowBytes>,
        right_pivot_key: &mut Option<CowBytes>,
        all_msgs: &mut BTreeMap<CowBytes, Vec<SlicedCowBytes>>,
    ) -> GetRangeResult<Box<Iterator<Item = (&'a [u8], SlicedCowBytes)> + 'a>, N> {
        match self.0 {
            PackedLeaf(ref map) => GetRangeResult::Data(map.get_all()),
            Leaf(ref leaf) => GetRangeResult::Data(Box::new(
                leaf.entries().iter().map(|(k, v)| (&k[..], v.clone())),
            )),
            Internal(ref internal) => {
                let prefetch_option = if internal.level() == 1 {
                    internal.get_next_node(key)
                } else {
                    None
                };
                let np = internal.get_range(key, left_pivot_key, right_pivot_key, all_msgs);
                GetRangeResult::NextNode {
                    prefetch_option,
                    np,
                }
            }
        }
    }
}

impl<N> Node<N> {
    pub(super) fn insert<K, M>(&mut self, key: K, msg: SlicedCowBytes, msg_action: M) -> isize
    where
        K: Borrow<[u8]> + Into<CowBytes>,
        M: MessageAction,
    {
        self.ensure_unpacked();
        match self.0 {
            PackedLeaf(_) => unreachable!(),
            Leaf(ref mut leaf) => leaf.insert(key, msg, msg_action),
            Internal(ref mut internal) => internal.insert(key, msg, msg_action),
        }
    }

    pub(super) fn insert_msg_buffer<I, M>(&mut self, msg_buffer: I, msg_action: M) -> isize
    where
        I: IntoIterator<Item = (CowBytes, SlicedCowBytes)>,
        M: MessageAction,
    {
        self.ensure_unpacked();
        match self.0 {
            PackedLeaf(_) => unreachable!(),
            Leaf(ref mut leaf) => leaf.insert_msg_buffer(msg_buffer, msg_action),
            Internal(ref mut internal) => {
                internal.insert_msg_buffer(msg_buffer, msg_action) as isize
            }
        }
    }
}

impl<N> Node<N> {
    fn child_pointer_iter<'a>(&'a mut self) -> Option<impl Iterator<Item = &'a mut N> + 'a> {
        match self.0 {
            Leaf(_) | PackedLeaf(_) => None,
            Internal(ref mut internal) => Some(
                internal
                    .iter_mut()
                    .map(|child| child.node_pointer.get_mut()),
            ),
        }
    }

    pub(super) fn drain_children<'a>(&'a mut self) -> Option<impl Iterator<Item = N> + 'a> {
        match self.0 {
            Leaf(_) | PackedLeaf(_) => None,
            Internal(ref mut internal) => Some(internal.drain_children()),
        }
    }
}

impl<N: StaticSize> Node<N> {
    pub(super) fn split(&mut self) -> (Self, CowBytes, isize) {
        self.ensure_unpacked();
        match self.0 {
            PackedLeaf(_) => unreachable!(),
            Leaf(ref mut leaf) => {
                let (node, pivot_key, size_delta) =
                    leaf.split(MIN_LEAF_NODE_SIZE, MAX_LEAF_NODE_SIZE);
                (Node(Leaf(node)), pivot_key, size_delta)
            }
            Internal(ref mut internal) => {
                assert!(
                    internal.fanout() >= 2 * MIN_FANOUT,
                    "internal split failed due to low fanout: {}, size: {}, actual_size: {}",
                    internal.fanout(),
                    internal.size(),
                    internal.actual_size()
                );
                let (node, pivot_key, size_delta) = internal.split();
                (Node(Internal(node)), pivot_key, size_delta)
            }
        }
    }

    pub(super) fn merge(&mut self, right_sibling: &mut Self, pivot_key: CowBytes) -> isize {
        self.ensure_unpacked();
        right_sibling.ensure_unpacked();
        match (&mut self.0, &mut right_sibling.0) {
            (&mut Leaf(ref mut left), &mut Leaf(ref mut right)) => left.merge(right),
            (&mut Internal(ref mut left), &mut Internal(ref mut right)) => {
                left.merge(right, pivot_key)
            }
            _ => unreachable!(),
        }
    }

    pub(super) fn leaf_rebalance(&mut self, right_sibling: &mut Self) -> FillUpResult {
        self.ensure_unpacked();
        right_sibling.ensure_unpacked();
        match (&mut self.0, &mut right_sibling.0) {
            (&mut Leaf(ref mut left), &mut Leaf(ref mut right)) => {
                left.rebalance(right, MIN_LEAF_NODE_SIZE, MAX_LEAF_NODE_SIZE)
            }
            _ => unreachable!(),
        }
    }

    pub(super) fn range_delete(
        &mut self,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> (isize, Option<(&mut N, Option<&mut N>, Vec<N>)>) {
        self.ensure_unpacked();
        match self.0 {
            PackedLeaf(_) => unreachable!(),
            Leaf(ref mut leaf) => {
                let size_delta = leaf.range_delete(start, end);
                (-(size_delta as isize), None)
            }
            Internal(ref mut internal) => {
                let mut dead = Vec::new();
                let (size_delta, l, r) = internal.range_delete(start, end, &mut dead);
                (-(size_delta as isize), Some((l, r, dead)))
            }
        }
    }
}

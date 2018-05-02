use self::flush::Ref;
use self::node::GetResult;
use super::errors::*;
use super::layer::{TreeBaseLayer, TreeLayer};
use cache::AddSize;
use cow_bytes::CowBytes;
use cow_bytes::SlicedCowBytes;
use data_management::{Dml, DmlBase, HandlerDml, ObjectRef};
use owning_ref::OwningRef;
use parking_lot::{RwLock, RwLockWriteGuard};
use std::borrow::Borrow;
use std::collections::range::RangeArgument;
use std::collections::Bound;
use std::marker::PhantomData;
use std::mem::replace;
use tree::MessageAction;

#[derive(Debug)]
enum FillUpResult {
    Rebalanced(CowBytes),
    Merged,
}

pub(super) const MAX_INTERNAL_NODE_SIZE: usize = 4 * 1024 * 1024;
const MIN_FLUSH_SIZE: usize = 256 * 1024;
const MIN_FANOUT: usize = 4;
const MIN_LEAF_NODE_SIZE: usize = 1 * 1024 * 1024;
const MAX_LEAF_NODE_SIZE: usize = MAX_INTERNAL_NODE_SIZE;

/// The actual tree type.
pub struct Tree<X: DmlBase, M, I: Borrow<Inner<X::ObjectRef, X::Info, M>>> {
    inner: I,
    dml: X,
    evict: bool,
    marker: PhantomData<M>,
}

impl<X: Clone + DmlBase, M, I: Clone + Borrow<Inner<X::ObjectRef, X::Info, M>>> Clone
    for Tree<X, M, I>
{
    fn clone(&self) -> Self {
        Tree {
            inner: self.inner.clone(),
            dml: self.dml.clone(),
            evict: self.evict,
            marker: PhantomData,
        }
    }
}

/// The inner tree type that does not contain the DML object.
pub struct Inner<R, I, M> {
    root_node: RwLock<R>,
    tree_id: Option<I>,
    msg_action: M,
}

impl<R, I, M> Inner<R, I, M> {
    fn new(tree_id: I, root_node: R, msg_action: M) -> Self {
        Inner {
            tree_id: Some(tree_id),
            root_node: RwLock::new(root_node),
            msg_action,
        }
    }

    /// Returns a new read-only tree.
    pub fn new_ro(root_node: R, msg_action: M) -> Self {
        Inner {
            tree_id: None,
            root_node: RwLock::new(root_node),
            msg_action,
        }
    }

    /// Sets a new root node.
    pub fn update_root_node(&mut self, root_node: R) {
        *self.root_node.get_mut() = root_node;
    }
}

impl<X, R, M, I> Tree<X, M, I>
where
    X: HandlerDml<Object = Node<R>, ObjectRef = R>,
    R: ObjectRef<ObjectPointer = X::ObjectPointer>,
    M: MessageAction,
    I: Borrow<Inner<X::ObjectRef, X::Info, M>> + From<Inner<X::ObjectRef, X::Info, M>>,
{
    /// Returns a new, empty tree.
    pub fn empty_tree(tree_id: X::Info, msg_action: M, dml: X) -> Self {
        let root_node = dml.insert(Node::empty_leaf(), tree_id);
        Tree::new(root_node, tree_id, msg_action, dml)
    }

    /// Opens a tree identified by the given root node.
    pub fn open(tree_id: X::Info, root_node_ptr: X::ObjectPointer, msg_action: M, dml: X) -> Self {
        Tree::new(X::ref_from_ptr(root_node_ptr), tree_id, msg_action, dml)
    }

    fn new(root_node: R, tree_id: X::Info, msg_action: M, dml: X) -> Self {
        Tree {
            inner: I::from(Inner::new(tree_id, root_node, msg_action)),
            dml,
            evict: true,
            marker: PhantomData,
        }
    }
}

impl<X, R, M, I> Tree<X, M, I>
where
    X: HandlerDml<Object = Node<R>, ObjectRef = R>,
    R: ObjectRef<ObjectPointer = X::ObjectPointer>,
    M: MessageAction,
    I: Borrow<Inner<X::ObjectRef, X::Info, M>>,
{
    /// Returns the inner of the tree.
    pub fn inner(&self) -> &I {
        &self.inner
    }

    /// Returns a new tree with the given inner.
    pub fn from_inner(inner: I, dml: X, evict: bool) -> Self {
        Tree {
            inner,
            dml,
            evict,
            marker: PhantomData,
        }
    }

    /// Returns the DML.
    pub fn dmu(&self) -> &X {
        &self.dml
    }

    /// Locks the root node.
    /// Returns `None` if the root node is modified.
    pub fn try_lock_root(&self) -> Option<OwningRef<RwLockWriteGuard<R>, X::ObjectPointer>> {
        let guard = self.inner.borrow().root_node.write();
        OwningRef::new(guard)
            .try_map(|guard| guard.get_unmodified().ok_or(()))
            .ok()
    }
}

impl<X, R, M, I> Tree<X, M, I>
where
    X: HandlerDml<Object = Node<R>, ObjectRef = R>,
    R: ObjectRef<ObjectPointer = X::ObjectPointer>,
    M: MessageAction,
    I: Borrow<Inner<X::ObjectRef, X::Info, M>>,
{
    fn tree_id(&self) -> X::Info {
        self.inner
            .borrow()
            .tree_id
            .expect("Mutating operations called on read only tree")
    }

    fn msg_action(&self) -> &M {
        &self.inner.borrow().msg_action
    }

    fn get_mut_root_node(&self) -> Result<X::CacheValueRefMut, Error> {
        if let Some(node) = self.dml.try_get_mut(&self.inner.borrow().root_node.read()) {
            return Ok(node);
        }
        Ok(self.dml
            .get_mut(&mut self.inner.borrow().root_node.write(), self.tree_id())?)
    }

    fn get_root_node(&self) -> Result<X::CacheValueRef, Error> {
        self.get_node(&self.inner.borrow().root_node)
    }

    fn get_node(&self, np_ref: &RwLock<X::ObjectRef>) -> Result<X::CacheValueRef, Error> {
        if let Some(node) = self.dml.try_get(&np_ref.read()) {
            return Ok(node);
        }
        Ok(self.dml.get(&mut np_ref.write())?)
    }

    fn try_get_mut_node(&self, np_ref: &mut RwLock<X::ObjectRef>) -> Option<X::CacheValueRefMut> {
        self.dml.try_get_mut(np_ref.get_mut())
    }

    fn get_mut_node_mut(&self, np_ref: &mut X::ObjectRef) -> Result<X::CacheValueRefMut, Error> {
        if let Some(node) = self.dml.try_get_mut(np_ref) {
            return Ok(node);
        }
        Ok(self.dml.get_mut(np_ref, self.tree_id())?)
    }

    fn get_mut_node(
        &self,
        np_ref: &mut RwLock<X::ObjectRef>,
    ) -> Result<X::CacheValueRefMut, Error> {
        self.get_mut_node_mut(np_ref.get_mut())
    }

    fn walk_tree(
        &self,
        mut node: X::CacheValueRefMut,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> Result<(), Error> {
        // TODO rebalance/merge nodes
        loop {
            let (size_delta, next_node) = {
                let level = node.level();
                let (size_delta, children) = node.range_delete(start, end);
                let (l_np, r_np, dead) = match children {
                    None => return Ok(()),
                    Some((l_np, r_np, dead)) => (l_np, r_np, dead),
                };
                if level == 1 {
                    for np in dead {
                        self.dml.remove(np);
                    }
                } else {
                    for mut np in dead {
                        self.remove_subtree(np)?;
                    }
                }
                if let Some(np) = r_np {
                    self.walk_tree(self.get_mut_node_mut(np)?, start, end)?;
                }
                (size_delta, self.get_mut_node_mut(l_np)?)
            };
            node.add_size(size_delta);
            node = next_node;
        }
    }

    fn remove_subtree(&self, np: X::ObjectRef) -> Result<(), Error> {
        let mut nps = vec![np];
        while let Some(np) = nps.pop() {
            {
                let mut node = self.dml.get_and_remove(np)?;
                let level = node.level();
                let result = node.drain_children();
                if let Some(child_nps) = result {
                    if level == 1 {
                        // No need to fetch the leaves
                        for np in child_nps {
                            self.dml.remove(np);
                        }
                    } else {
                        nps.extend(child_nps);
                    }
                }
            }
        }
        Ok(())
    }

    //    pub fn is_modified(&mut self) -> bool {
    //        self.inner.borrow_mut().root_node.is_modified()
    //    }
}

// impl<X, R, M> Tree<X, M, Arc<Inner<X::ObjectRef, X::Info, M>>>
// where
//     X: HandlerDml<Object = Node<Message<M>, R>, ObjectRef = R>,
//     R: ObjectRef,
//     M: MessageAction,
// {
//     pub fn is_modified(&mut self) -> Option<bool> {
// Arc::get_mut(&mut self.inner).map(|inner|
// inner.root_node.is_modified())
//     }
// }

impl<X, R, M, I> TreeBaseLayer<M> for Tree<X, M, I>
where
    X: HandlerDml<Object = Node<R>, ObjectRef = R>,
    R: ObjectRef<ObjectPointer = X::ObjectPointer>,
    M: MessageAction,
    I: Borrow<Inner<X::ObjectRef, X::Info, M>>,
{
    fn get<K: Borrow<[u8]>>(&self, key: K) -> Result<Option<SlicedCowBytes>, Error> {
        let key = key.borrow();
        let mut msgs = Vec::new();
        let mut node = self.get_root_node()?;
        let mut data = loop {
            let next_node = match node.get(key, &mut msgs) {
                GetResult::NextNode(np) => self.get_node(np)?,
                GetResult::Data(data) => break data,
            };
            node = next_node;
        };

        for msg in msgs.into_iter().rev() {
            self.msg_action().apply(key, &msg, &mut data);
        }

        if self.evict {
            self.dml.evict()?;
        }
        Ok(data)
    }

    fn insert<K>(&self, key: K, msg: SlicedCowBytes) -> Result<(), Error>
    where
        K: Borrow<[u8]> + Into<CowBytes>,
    {
        let mut node = self.get_mut_root_node()?;

        let mut parent = None;
        let mut node = loop {
            match Ref::try_new(node, |node| node.try_walk(key.borrow())) {
                Ok(mut child_buffer) => {
                    if let Some(child) = self.try_get_mut_node(child_buffer.node_pointer_mut()) {
                        node = child;
                        parent = Some(child_buffer);
                    } else {
                        break child_buffer.into_owner();
                    }
                }
                Err(node) => break node,
            };
        };

        let added_size = node.insert(key, msg, self.msg_action());
        node.add_size(added_size);

        if parent.is_none() && node.root_needs_merge() {
            // TODO merge
            unimplemented!();
        }

        self.fixup_foo(node, parent)?;

        // TODO evict?
        if self.evict {
            self.dml.evict()?;
        }
        Ok(())
    }

    fn depth(&self) -> Result<u32, Error> {
        Ok(self.get_root_node()?.level() + 1)
    }
}

impl<X, R, M, I> TreeLayer<M> for Tree<X, M, I>
where
    X: Dml<Object = Node<R>, ObjectRef = R>,
    R: ObjectRef<ObjectPointer = X::ObjectPointer>,
    M: MessageAction,
    I: Borrow<Inner<X::ObjectRef, X::Info, M>>,
{
    type Pointer = X::ObjectPointer;

    type Range = RangeIterator<X, M, I>;

    fn range<K, T>(&self, range: T) -> Result<Self::Range, Error>
    where
        T: RangeArgument<K>,
        K: Borrow<[u8]> + Into<CowBytes>,
        Self: Clone,
    {
        Ok(RangeIterator::new(range, self.clone()))
    }

    fn range_delete<K, T>(&self, range: T) -> Result<(), Error>
    where
        T: RangeArgument<K>,
        K: Borrow<[u8]>,
    {
        if let (Bound::Unbounded, Bound::Unbounded) = (range.start(), range.end()) {
            let np = self.dml.insert(Node::empty_leaf(), self.tree_id());
            let old_root_np = replace(&mut *self.inner.borrow().root_node.write(), np);
            return self.remove_subtree(old_root_np);
        }

        let start_box;
        let end_box;
        let start = match range.start() {
            Bound::Unbounded => &[] as &[_],
            Bound::Excluded(x) => {
                let mut v = x.borrow().to_vec();
                v.push(0);
                start_box = v.into_boxed_slice();
                &start_box
            }
            Bound::Included(x) => x.borrow(),
        };
        let end = match range.end() {
            Bound::Unbounded => None,
            Bound::Excluded(x) => Some(x.borrow()),
            Bound::Included(x) => {
                let mut v = x.borrow().to_vec();
                v.push(0);
                end_box = v.into_boxed_slice();
                Some(&end_box[..])
            }
        };

        let node = self.get_mut_root_node()?;
        self.walk_tree(node, start, end)
    }

    fn sync(&self) -> Result<Self::Pointer, Error> {
        // TODO
        let obj_ptr = self.dml
            .write_back(|| self.inner.borrow().root_node.write())?;
        Ok(obj_ptr)
    }
}

mod child_buffer;
mod flush;
mod internal;
mod leaf;
mod node;
mod packed;
mod range;
mod split;

pub use self::node::Node;
pub use self::range::RangeIterator;

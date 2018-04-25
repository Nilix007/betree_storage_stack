use super::child_buffer::ChildBuffer;
use super::internal::TakeChildBuffer;
use super::{FillUpResult, Inner, Node, Tree};
use cache::AddSize;
use data_management::{HandlerDml, ObjectRef};
use size::Size;
use stable_deref_trait::StableDeref;
use std::borrow::Borrow;
use std::mem::transmute;
use std::ops::{Deref, DerefMut};
use tree::MessageAction;
use tree::errors::*;

impl<X, R, M, I> Tree<X, M, I>
where
    X: HandlerDml<Object = Node<R>, ObjectRef = R>,
    R: ObjectRef<ObjectPointer = X::ObjectPointer>,
    M: MessageAction,
    I: Borrow<Inner<X::ObjectRef, X::Info, M>>,
{
    pub(super) fn fixup_foo(
        &self,
        mut node: X::CacheValueRefMut,
        mut parent: Option<Ref<X::CacheValueRefMut, TakeChildBuffer<'static, ChildBuffer<R>>>>,
    ) -> Result<(), Error> {
        //        let mut parent: Option<Ref<_, _>> = None;
        loop {
            if !node.is_too_large() {
                return Ok(());
            }
            warn!(
                "{}, {:?}, lvl: {}, size: {}, {}",
                node.kind(),
                node.fanout(),
                node.level(),
                node.size(),
                node.actual_size()
            );
            let mut child_buffer = match Ref::try_new(node, |node| node.try_flush()) {
                Err(_node) => match parent {
                    None => {
                        self.split_root_node(_node);
                        return Ok(());
                    }
                    Some(ref mut parent) => {
                        let (next_node, size_delta) = self.split_node(_node, parent)?;
                        parent.add_size(size_delta);
                        node = next_node;
                        continue;
                    }
                },
                Ok(selected_child_buffer) => selected_child_buffer,
            };
            let mut child = self.get_mut_node(child_buffer.node_pointer_mut())?;
            if !child.is_leaf() && child.is_too_large() {
                warn!("Aborting flush, child is too large already");
                parent = Some(child_buffer);
                node = child;
                continue;
            }
            if child.has_too_low_fanout() {
                let size_delta = {
                    let mut m = child_buffer.prepare_merge();
                    let mut sibling = self.get_mut_node(m.sibling_node_pointer())?;
                    let is_right_sibling = m.is_right_sibling();
                    let (pivot, old_np, size_delta) = m.merge_children();
                    if is_right_sibling {
                        let size_delta = child.merge(&mut sibling, pivot);
                        child.add_size(size_delta);
                    } else {
                        let size_delta = sibling.merge(&mut child, pivot);
                        child.add_size(size_delta);
                    }
                    self.dml.remove(old_np);
                    size_delta
                };
                child_buffer.add_size(size_delta);
                node = child_buffer.into_owner();
                continue;
            }
            let (buffer, size_delta) = child_buffer.take_buffer();
            warn!("Flushed {}", -size_delta);
            child_buffer.add_size(size_delta);
            let size_delta_child = child.insert_msg_buffer(buffer, self.msg_action());
            child.add_size(size_delta_child);

            if child.is_too_small_leaf() {
                let size_delta = {
                    let mut m = child_buffer.prepare_merge();
                    let mut sibling = self.get_mut_node(m.sibling_node_pointer())?;
                    // TODO size delta for child/sibling
                    // TODO deallocation
                    let result = if m.is_right_sibling() {
                        child.leaf_rebalance(&mut sibling)
                    } else {
                        sibling.leaf_rebalance(&mut child)
                    };
                    match result {
                        FillUpResult::Merged => m.merge_children().2,
                        FillUpResult::Rebalanced(pivot) => m.rebalanced(pivot),
                    }
                };
                child_buffer.add_size(size_delta);
            }
            while child.is_too_large_leaf() {
                let (next_node, size_delta) = self.split_node(child, &mut child_buffer)?;
                child_buffer.add_size(size_delta);
                child = next_node;
            }

            if child_buffer.size() > super::MAX_INTERNAL_NODE_SIZE {
                warn!("Node is still too large");
                if child.is_too_large() {
                    warn!("... but child, too");
                }
                node = child_buffer.into_owner();
                continue;
            }
            // Drop old parent here.
            parent = Some(child_buffer);
            node = child;
        }
    }
}

pub struct Ref<T, U> {
    inner: U,
    owner: T,
}

impl<T: StableDeref + DerefMut, U> Ref<T, TakeChildBuffer<'static, U>> {
    pub fn try_new<F>(mut owner: T, f: F) -> Result<Self, T>
    where
        F: for<'a> FnOnce(&'a mut T::Target) -> Option<TakeChildBuffer<'a, U>>,
    {
        match unsafe { transmute(f(&mut owner)) } {
            None => Err(owner),
            Some(inner) => Ok(Ref { owner, inner }),
        }
    }

    pub fn into_owner(self) -> T {
        self.owner
    }
}

impl<T: AddSize, U> AddSize for Ref<T, U> {
    fn add_size(&self, size_delta: isize) {
        self.owner.add_size(size_delta);
    }
}

impl<T, U> Deref for Ref<T, U> {
    type Target = U;
    fn deref(&self) -> &U {
        &self.inner
    }
}

impl<T, U> DerefMut for Ref<T, U> {
    fn deref_mut(&mut self) -> &mut U {
        &mut self.inner
    }
}

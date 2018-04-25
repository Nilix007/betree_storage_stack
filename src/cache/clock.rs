use std::marker::PhantomData;
use std::ptr::Unique;

struct ClockEntry<T> {
    value: T,
    next: Unique<ClockEntry<T>>,
}

/// A simple clock, i.e. a circular singly linked list.
pub struct Clock<T> {
    /// Contains the pointer to the current tail of the circular list.
    tail: Option<Unique<ClockEntry<T>>>,
}

impl<T> Default for Clock<T> {
    fn default() -> Self {
        Clock { tail: None }
    }
}

impl<T> Clock<T> {
    /// Returns the number of elements in this clock.
    pub fn len(&self) -> usize {
        self.iter().count()
    }

    /// Removes the element at the head of the list.
    pub fn pop_front(&mut self) -> Option<T> {
        self.tail.map(|mut tail| {
            let head = unsafe { tail.as_ref() }.next;
            if head.as_ptr() != tail.as_ptr() {
                unsafe { tail.as_mut() }.next = unsafe { head.as_ref() }.next;
            } else {
                self.tail = None;
            }
            unsafe { Box::from_raw(head.as_ptr()) }.value
        })
    }

    /// Peeks at the element at the head of the list.
    pub fn peek_front(&self) -> Option<&T> {
        self.tail
            .map(|tail| unsafe { tail.as_ref() }.next)
            .map(|head| &unsafe { &*head.as_ptr() }.value)
    }

    /// Increments the *hand* so that the head of the list becomes the tail.
    pub fn next(&mut self) {
        if let Some(ref mut tail) = self.tail {
            *tail = unsafe { tail.as_ref() }.next;
        };
    }

    /// Inserts the given element at the end of the list.
    pub fn push_back(&mut self, value: T) {
        let entry = Box::new(ClockEntry {
            value,
            next: Unique::empty(),
        });
        let entry = Box::into_raw(entry);
        let mut entry = Unique::new(entry).unwrap();

        if let Some(ref mut tail) = self.tail {
            unsafe { entry.as_mut() }.next = unsafe { tail.as_ref() }.next;
            unsafe { tail.as_mut() }.next = entry;
        } else {
            unsafe { entry.as_mut() }.next = entry;
        }
        self.tail = Some(entry);
    }

    /// Returns an iterator that iterates over the list.
    pub fn iter(&self) -> ClockIter<T> {
        if let Some(tail) = self.tail {
            ClockIter {
                current: Some(unsafe { tail.as_ref() }.next),
                last: tail,
                marker: PhantomData,
            }
        } else {
            ClockIter {
                current: None,
                last: Unique::empty(),
                marker: PhantomData,
            }
        }
    }

    /// Returns an iterator that iterates over the list that allows
    /// modification.
    pub fn iter_mut(&self) -> ClockIterMut<T> {
        if let Some(tail) = self.tail {
            ClockIterMut {
                current: Some(unsafe { tail.as_ref() }.next),
                last: tail,
                marker: PhantomData,
            }
        } else {
            ClockIterMut {
                current: None,
                last: Unique::empty(),
                marker: PhantomData,
            }
        }
    }

    /// Retains only the elements in the list for which `f` returns `true`.
    pub fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&T) -> bool,
    {
        unsafe {
            let tail = match self.tail {
                None => return,
                Some(tail) => tail,
            };
            let mut last = tail;
            let mut current = tail.as_ref().next;

            while tail.as_ptr() != current.as_ptr() {
                if f(&current.as_ref().value) {
                    // Retain element
                    last = current;
                    current = current.as_ref().next;
                } else {
                    // Remove element
                    let next = current.as_ref().next;
                    Box::from_raw(current.as_ptr());
                    last.as_mut().next = next;
                    current = next;
                }
            }

            if !f(&current.as_ref().value) {
                // Remove tail element of list.
                if last.as_ptr() == current.as_ptr() {
                    // List contains only one element
                    self.tail = None;
                } else {
                    self.tail = Some(last);
                    last.as_mut().next = current.as_ref().next;
                }
                Box::from_raw(current.as_ptr());
            }
        }
    }
}

impl<T> Drop for Clock<T> {
    fn drop(&mut self) {
        while self.pop_front().is_some() {}
    }
}

/// Immutable clock iterator
pub struct ClockIter<'a, T: 'a> {
    current: Option<Unique<ClockEntry<T>>>,
    last: Unique<ClockEntry<T>>,
    marker: PhantomData<&'a T>,
}

impl<'a, T> Iterator for ClockIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        self.current.take().map(|current| {
            let entry = unsafe { &*current.as_ptr() };
            self.current = if current.as_ptr() == self.last.as_ptr() {
                None
            } else {
                Some(entry.next)
            };
            &entry.value
        })
    }
}

/// Mutable clock iterator
pub struct ClockIterMut<'a, T: 'a> {
    current: Option<Unique<ClockEntry<T>>>,
    last: Unique<ClockEntry<T>>,
    marker: PhantomData<&'a mut T>,
}

impl<'a, T> Iterator for ClockIterMut<'a, T> {
    type Item = &'a mut T;

    fn next(&mut self) -> Option<Self::Item> {
        self.current.take().map(|current| {
            let entry = unsafe { &mut *current.as_ptr() };
            self.current = if current.as_ptr() == self.last.as_ptr() {
                None
            } else {
                Some(entry.next)
            };
            &mut entry.value
        })
    }
}

#[cfg(test)]
mod tests {
    use super::Clock;
    use std::cell::Cell;
    use std::collections::VecDeque;

    #[test]
    fn push_pop_next_retain() {
        let mut clock = Clock::default();
        let mut vec_deque = VecDeque::new();
        assert!(clock.iter().eq(&vec_deque));

        for i in 0..1000 {
            if i % 3 == 0 {
                assert_eq!(clock.pop_front(), vec_deque.pop_front());
            } else if i % 5 == 0 {
                clock.next();
                if let Some(entry) = vec_deque.pop_front() {
                    vec_deque.push_back(entry);
                }
            } else {
                clock.push_back(i);
                vec_deque.push_back(i);
            }
            assert!(clock.iter().eq(&vec_deque));
        }
        for i in &[4, 5, 1] {
            clock.retain(|e| e % i == 1);
            vec_deque.retain(|e| e % i == 1);
            assert!(clock.iter().eq(&vec_deque));
        }
    }

    #[test]
    fn test_drop() {
        struct DropMe<'a>(&'a Cell<bool>);
        impl<'a> Drop for DropMe<'a> {
            fn drop(&mut self) {
                self.0.set(true);
            }
        }
        {
            let dropped = Cell::new(false);
            let mut clock = Clock::default();
            clock.push_back(DropMe(&dropped));
            assert!(!dropped.get());
            clock.pop_front();
            assert!(dropped.get());
        }

        {
            let first = Cell::new(false);
            let second = Cell::new(false);
            let mut clock = Clock::default();
            clock.push_back(DropMe(&first));
            clock.push_back(DropMe(&second));

            let mut cnt = 0;
            clock.retain(|_| {
                cnt += 1;
                cnt == 1
            });
            assert!(!first.get());
            assert!(second.get());
            clock.retain(|_| false);
            assert!(first.get());
        }
    }
}

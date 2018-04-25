use super::{Dml, DmlBase, Error, HandlerDml};
use std::ops::{Deref, DerefMut};

impl<T> DmlBase for T
where
    T: Deref,
    T::Target: DmlBase,
{
    type ObjectRef = <T::Target as DmlBase>::ObjectRef;
    type ObjectPointer = <T::Target as DmlBase>::ObjectPointer;
    type Info = <T::Target as DmlBase>::Info;
}

impl<T> HandlerDml for T
where
    T: Deref,
    T::Target: HandlerDml,
{
    type Object = <T::Target as HandlerDml>::Object;
    type CacheValueRef = <T::Target as HandlerDml>::CacheValueRef;
    type CacheValueRefMut = <T::Target as HandlerDml>::CacheValueRefMut;

    fn try_get(&self, or: &Self::ObjectRef) -> Option<Self::CacheValueRef> {
        (**self).try_get(or)
    }

    fn get(&self, or: &mut Self::ObjectRef) -> Result<Self::CacheValueRef, Error> {
        (**self).get(or)
    }

    fn get_mut(
        &self,
        or: &mut Self::ObjectRef,
        info: Self::Info,
    ) -> Result<Self::CacheValueRefMut, Error> {
        (**self).get_mut(or, info)
    }

    fn try_get_mut(&self, or: &Self::ObjectRef) -> Option<Self::CacheValueRefMut> {
        (**self).try_get_mut(or)
    }

    fn insert(&self, object: Self::Object, info: Self::Info) -> Self::ObjectRef {
        (**self).insert(object, info)
    }

    fn insert_and_get_mut(
        &self,
        object: Self::Object,
        info: Self::Info,
    ) -> (Self::CacheValueRefMut, Self::ObjectRef) {
        (**self).insert_and_get_mut(object, info)
    }

    fn remove(&self, or: Self::ObjectRef) {
        (**self).remove(or)
    }

    fn get_and_remove(&self, or: Self::ObjectRef) -> Result<Self::Object, Error> {
        (**self).get_and_remove(or)
    }

    fn evict(&self) -> Result<(), Error> {
        (**self).evict()
    }

    fn ref_from_ptr(r: Self::ObjectPointer) -> Self::ObjectRef {
        <T::Target as HandlerDml>::ref_from_ptr(r)
    }
}

impl<T> Dml for T
where
    T: Deref,
    T::Target: Dml,
{
    fn write_back<F>(&self, acquire_or_lock: F) -> Result<Self::ObjectPointer, Error>
    where
        F: FnMut<()>,
        F::Output: DerefMut<Target = Self::ObjectRef>,
    {
        (**self).write_back(acquire_or_lock)
    }

    type Prefetch = <T::Target as Dml>::Prefetch;

    fn prefetch(&self, or: &Self::ObjectRef) -> Result<Option<Self::Prefetch>, Error> {
        (**self).prefetch(or)
    }

    fn finish_prefetch(&self, p: Self::Prefetch) -> Result<(), Error> {
        (**self).finish_prefetch(p)
    }

    fn drop_cache(&self) {
        (**self).drop_cache();
    }
}

use SizeMut;
use allocator::{Action, SegmentAllocator, SegmentId};
use storage_pool::DiskOffset;
use vdev::Block;

struct Handler;

impl super::HandlerTypes for Handler {
    type Info = ();
    type Generation = ();
}

#[derive(Serialize, Deserialize)]
struct Object<P>(P);

impl<P: SizeMut> SizeMut for Object<P> {
    fn size(&mut self) -> usize {
        self.0.size()
    }
}

impl<X: super::DmlBase> super::Object<X> for Object<X::ObjectRef> {
    fn for_each_child<E, F>(&mut self, mut f: F) -> Result<(), E>
    where
        F: FnMut(&mut X::ObjectRef) -> Result<(), E>,
    {
        f(&mut self.0)
    }
}

impl<X: super::HandlerDml<Handler = Self>> super::Handler<X> for Handler {
    type Error = super::errors::Error;
    type Object = Object<X::ObjectRef>;

    fn current_generation(&self) -> Self::Generation {
        ()
    }

    fn update_allocation_bitmap(
        &self,
        offset: DiskOffset,
        size: Block<u32>,
        action: Action,
        dmu: &X,
    ) -> Result<(), Self::Error> {
        unimplemented!()
    }
    fn get_allocation_bitmap(
        &self,
        id: SegmentId,
        dmu: &X,
    ) -> Result<SegmentAllocator, Self::Error> {
        unimplemented!()
    }

    fn get_free_space(&self, disk_id: u16) -> Block<u64> {
        unimplemented!()
    }

    fn copy_on_write(
        &self,
        offset: DiskOffset,
        size: Block<u32>,
        generation: Self::Generation,
        info: Self::Info,
        dml: &X,
    ) -> Result<(), Self::Error> {
        unimplemented!()
    }
}

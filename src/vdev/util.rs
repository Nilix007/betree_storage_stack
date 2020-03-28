pub fn alloc_uninitialized(size: usize) -> Box<[u8]> {
    let mut v = Vec::new();
    v.reserve(size);
    unsafe {
        v.set_len(size);
    }
    v.into_boxed_slice()
}

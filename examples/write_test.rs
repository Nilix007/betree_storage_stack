#![feature(never_type)]
#![feature(integer_atomics)]

extern crate betree_storage_stack;
extern crate bincode;
extern crate byteorder;
extern crate clap;
extern crate env_logger;
extern crate indicatif;
extern crate parking_lot;
extern crate rand;
extern crate scoped_threadpool;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate unbytify;

use betree_storage_stack::allocator::{Action, SegmentAllocator, SegmentId, SEGMENT_SIZE};
use betree_storage_stack::atomic_option::AtomicOption;
use betree_storage_stack::cache::{Cache, ClockCache};
use betree_storage_stack::checksum::{XxHash, XxHashBuilder};
use betree_storage_stack::compression;
use betree_storage_stack::cow_bytes::{CowBytes, SlicedCowBytes};
use betree_storage_stack::data_management::{
    self, Dml, Dmu, Handler as HandlerTrait, HandlerDml, ObjectRef,
};
use betree_storage_stack::storage_pool::{
    configuration, Configuration, DiskOffset, StoragePoolLayer, StoragePoolUnit,
};
use betree_storage_stack::tree::{
    DefaultMessageAction, Error as TreeError, Inner as TreeInner, Node, Tree, TreeBaseLayer,
    TreeLayer,
};
use betree_storage_stack::vdev::{Block, BLOCK_SIZE};
use byteorder::{BigEndian, ByteOrder, NativeEndian};
use clap::{App, Arg};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use parking_lot::Mutex;
use rand::{RngCore, SeedableRng, XorShiftRng};
use scoped_threadpool::Pool;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter, Result as FmtResult};
use std::fs::OpenOptions;
use std::io::Read;
use std::mem::replace;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

struct Size(pub u64);

impl Display for Size {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        let (size, suffix) = unbytify::bytify(**self);
        if let Some(precision) = f.precision() {
            f.pad_integral(true, &"", &format!("{:.*} {}", precision, size, suffix))
        } else {
            f.pad(&format!("{} {}", size, suffix))
        }
    }
}

impl FromStr for Size {
    type Err = unbytify::ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        unbytify::unbytify(s).map(Size)
    }
}

impl Deref for Size {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

fn parse_size<T: Into<Option<u64>>, U: Into<Option<u64>>>(
    s: &str,
    min: T,
    max: U,
) -> Result<u64, Box<dyn Error>> {
    static SUFFICES: [char; 4] = ['k', 'm', 'g', 't'];
    if s.len() == 0 {
        return Err("not a number".into());
    }
    let mut number = 0;
    let mut iter = s.chars();
    while let Some(c) = iter.next() {
        if let Some(d) = c.to_digit(10) {
            number = d as u64 + 10 * number;
        } else if let Some(idx) = SUFFICES.iter().position(|&x| x == c) {
            number *= 1024u64.pow(idx as u32 + 1);
            break;
        } else {
            return Err("invalid char".into());
        }
    }
    if iter.next().is_some() {
        Err("invalid input".into())
    } else if min.into().map(|min| number < min).unwrap_or(false) {
        Err("number too small".into())
    } else if max.into().map(|max| number > max).unwrap_or(false) {
        Err("number too large".into())
    } else {
        Ok(number)
    }
}

trait KeyGenerator {
    fn new(idx: usize, shift: usize) -> Self;
    fn next(&mut self) -> [u8; 4];
    fn name() -> &'static str;
}

struct RandomKeyGenerator(XorShiftRng);

impl KeyGenerator for RandomKeyGenerator {
    fn next(&mut self) -> [u8; 4] {
        let mut key_bytes = [0; 4];
        self.0.fill_bytes(&mut key_bytes);
        key_bytes
    }
    fn name() -> &'static str {
        "random"
    }
    fn new(idx: usize, _shift: usize) -> Self {
        RandomKeyGenerator(XorShiftRng::from_seed(unsafe {
            ::std::mem::transmute([1, 0, 0, idx as u32])
        }))
    }
}

struct SequentialKeyGenerator {
    idx: usize,
    shift: usize,
}

impl KeyGenerator for SequentialKeyGenerator {
    fn next(&mut self) -> [u8; 4] {
        let i = self.idx;
        self.idx += self.shift;
        let mut key_bytes = [0; 4];
        BigEndian::write_u32(&mut key_bytes, i as u32);
        key_bytes
    }
    fn name() -> &'static str {
        "sequential"
    }
    fn new(idx: usize, shift: usize) -> Self {
        SequentialKeyGenerator { idx, shift }
    }
}

struct Handler<R> {
    tree_inner: AtomicOption<Arc<TreeInner<R, (), DefaultMessageAction>>>,
    free_space: Box<[AtomicU64]>,
    frees: Mutex<Vec<(DiskOffset, Block<u32>)>>,
}

impl<R> Handler<R> {
    fn segment_id_to_key(segment_id: SegmentId) -> [u8; 9] {
        let mut key = [0; 9];
        BigEndian::write_u64(&mut key[1..], segment_id.0);
        key
    }
}

impl<R: ObjectRef> Handler<R> {
    fn free<X>(&self, dmu: &X) -> Result<(), TreeError>
    where
        X: HandlerDml<Object = Node<R>, ObjectRef = R, ObjectPointer = R::ObjectPointer, Info = ()>,
        R::ObjectPointer: Serialize + DeserializeOwned,
    {
        let v = {
            let mut frees = self.frees.lock();
            replace(&mut *frees, Vec::new())
        };
        for (offset, size) in v {
            self.update_allocation_bitmap(offset, size, Action::Deallocate, dmu)?;
        }
        Ok(())
    }
}

impl<R> data_management::HandlerTypes for Handler<R> {
    type Generation = ();
    type Info = ();
}

impl<R: ObjectRef> data_management::Handler<R> for Handler<R> {
    type Object = Node<R>;
    type Error = TreeError;

    fn current_generation(&self) -> () {
        ()
    }

    fn update_allocation_bitmap<X>(
        &self,
        offset: DiskOffset,
        size: Block<u32>,
        action: Action,
        dmu: &X,
    ) -> Result<(), Self::Error>
    where
        X: HandlerDml<
            Object = Self::Object,
            ObjectRef = R,
            ObjectPointer = R::ObjectPointer,
            Info = (),
        >,
        R::ObjectPointer: Serialize + DeserializeOwned,
    {
        let x = &self.free_space[offset.disk_id()];
        match action {
            Action::Allocate => x.fetch_sub(size.as_u64(), Ordering::Relaxed),
            Action::Deallocate => x.fetch_add(size.as_u64(), Ordering::Relaxed),
        };

        let key = Self::segment_id_to_key(SegmentId::get(offset));
        let msg =
            betree_storage_stack::database::update_allocation_bitmap_msg(offset, size, action);
        Tree::from_inner(&**self.tree_inner.get().unwrap(), dmu, false).insert(&key[..], msg)
    }

    fn get_allocation_bitmap<X>(
        &self,
        id: SegmentId,
        dmu: &X,
    ) -> Result<SegmentAllocator, Self::Error>
    where
        X: HandlerDml<
            Object = Self::Object,
            ObjectRef = R,
            ObjectPointer = R::ObjectPointer,
            Info = (),
        >,
        R::ObjectPointer: Serialize + DeserializeOwned,
    {
        self.free(dmu)?;
        let key = Self::segment_id_to_key(id);
        let mut segment = Tree::from_inner(&**self.tree_inner.get().unwrap(), dmu, false)
            .get(&key[..])?
            .map(|b| b.to_vec())
            .unwrap_or_default();
        segment.resize(SEGMENT_SIZE, 0);
        let segment = segment.into_boxed_slice();
        Ok(SegmentAllocator::new(segment))
    }

    fn get_free_space(&self, disk_id: u16) -> Block<u64> {
        Block(self.free_space[disk_id as usize].load(Ordering::Relaxed))
    }

    fn copy_on_write(&self, offset: DiskOffset, size: Block<u32>, _generation: (), _info: ()) {
        self.frees.lock().push((offset, size));
    }
}

static MAGIC: &[u8] = b"HEAFSv2\0\n";

#[derive(Serialize, Deserialize)]
struct Superblock<P> {
    magic: [u8; 9],
    root_ptr: P,
}

fn round_up(x: u64, y: u64) -> u64 {
    let mask = y - 1;
    (x + mask) & !mask
}

fn main() {
    env_logger::init();
    let matches = App::new("Simple benchmark")
        .arg(
            Arg::with_name("nosync")
                .long("nosync")
                .help("Do not sync to disk"),
        )
        .arg(Arg::with_name("random").long("random").help("Random IO"))
        .arg(
            Arg::with_name("nowrite")
                .long("nowrite")
                .help("Do not write"),
        )
        .arg(Arg::with_name("verify").long("verify").help("Verify"))
        .arg(
            Arg::with_name("block size")
                .short("b")
                .help("IO block size (defaults to 128k)")
                .takes_value(true)
                .validator(|s| match parse_size(&s, 1, 128 * 1024) {
                    Ok(_) => Ok(()),
                    Err(e) => Err(e.description().to_string()),
                }),
        )
        .arg(
            Arg::with_name("cache size")
                .short("c")
                .help("Cache size (defaults to 256M)")
                .takes_value(true)
                .validator(
                    |s| match parse_size(&s, 256 * 1024 * 1024, 16 * 1024 * 1024 * 1024) {
                        Ok(_) => Ok(()),
                        Err(e) => Err(e.description().to_string()),
                    },
                ),
        )
        .arg(
            Arg::with_name("iterations")
                .long("iterations")
                .help("Number of IOs")
                .takes_value(true)
                .validator(|s| match parse_size(&s, None, None) {
                    Ok(_) => Ok(()),
                    Err(e) => Err(e.description().to_string()),
                }),
        )
        .arg(
            Arg::with_name("total size")
                .long("total")
                .help("Total size")
                .takes_value(true)
                .conflicts_with("iterations")
                .validator(|s| match parse_size(&s, None, None) {
                    Ok(_) => Ok(()),
                    Err(e) => Err(e.description().to_string()),
                }),
        )
        .arg(
            Arg::with_name("file size")
                .long("file-size")
                .help("file size")
                .takes_value(true)
                .validator(|s| match parse_size(&s, None, None) {
                    Ok(_) => Ok(()),
                    Err(e) => Err(e.description().to_string()),
                }),
        )
        .arg(
            Arg::with_name("threads")
                .long("threads")
                .takes_value(true)
                .validator(|s| match usize::from_str(&s) {
                    Ok(_) => Ok(()),
                    Err(e) => Err(e.description().to_string()),
                }),
        )
        .get_matches();
    let random = matches.is_present("random");
    let write = !matches.is_present("nowrite");
    let verify = matches.is_present("verify");
    let sync = !matches.is_present("nosync");
    let block_size = matches
        .value_of("block size")
        .map(|s| parse_size(s, None, None).unwrap())
        .unwrap_or(128 * 1024);
    let cache_size = matches
        .value_of("cache size")
        .map(|s| parse_size(s, None, None).unwrap())
        .unwrap_or(256 * 1024 * 1024);
    let iterations = if let Some(total_size) = matches
        .value_of("total size")
        .map(|s| parse_size(s, None, None).unwrap())
    {
        total_size / block_size
    } else {
        matches
            .value_of("iterations")
            .map(|s| parse_size(s, None, None).unwrap())
            .unwrap_or(10240)
    };
    let file_size = matches
        .value_of("file size")
        .map(|s| parse_size(s, None, None).unwrap())
        .unwrap_or({
            let min_size = block_size * iterations;
            let size = min_size + min_size / 3;
            const GIGABYTE: u64 = 1024 * 1024 * 1024;
            round_up(size, GIGABYTE)
        });
    let threads = matches
        .value_of("threads")
        .map(|s| usize::from_str(&s).unwrap())
        .unwrap_or(1);

    if random {
        run::<RandomKeyGenerator>(
            threads, file_size, block_size, cache_size, iterations, sync, write, verify,
        )
    } else {
        run::<SequentialKeyGenerator>(
            threads, file_size, block_size, cache_size, iterations, sync, write, verify,
        )
    }
    .unwrap();
}

fn run<K: KeyGenerator>(
    threads: usize,
    file_size: u64,
    block_size: u64,
    cache_size: u64,
    iterations: u64,
    sync: bool,
    write: bool,
    verify: bool,
) -> Result<(), Box<dyn Error>> {
    //    if write {
    //        OpenOptions::new()
    //            .create(true)
    //            .truncate(true)
    //            .read(true)
    //            .write(true)
    //            .open("/var/tmp/write_test")?
    //            .set_len(file_size)?;
    //    }

    // let disks = [
    //     "/dev/disk/by-id/ata-ST3500418AS_9VM5H65V",
    //     "/dev/disk/by-id/ata-ST3500418AS_9VM5H79W",
    //     "/dev/disk/by-id/ata-ST3500418AS_9VM5RHY4",
    //     "/dev/disk/by-id/ata-ST3500418AS_9VM5SAQ3",
    //     "/dev/disk/by-id/ata-ST3500418AS_9VM5SJB5",
    //     "/dev/disk/by-id/ata-ST3500418AS_9VM5V4MJ",
    // ];
    let disks = ["/var/tmp/write_test"];

    let cfg = Configuration::new(
        // vec![
        //  configuration::Vdev::Leaf(configuration::LeafVdev::from("/var/tmp/write_test")),
        // ]
        disks
            .iter()
            .map(|&s| configuration::Vdev::Leaf(s.into()))
            .collect(),
    );
    let spu = StoragePoolUnit::<XxHash>::new(&cfg)?;
    let dmu = Arc::new(Dmu::new(
        compression::None,
        XxHashBuilder,
        spu,
        ClockCache::new(cache_size as usize),
        Handler {
            tree_inner: AtomicOption::new(),
            free_space: (0..disks.len())
                .map(|_| AtomicU64::new(file_size / BLOCK_SIZE as u64 / disks.len() as u64))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            frees: Mutex::new(Vec::new()),
        },
    ));
    let tree: Tree<_, _, Arc<_>> = if write {
        Tree::empty_tree((), DefaultMessageAction, dmu)
    } else {
        assert!(disks.len() == 1);
        let mut fp = OpenOptions::new().read(true).open(disks[0])?;
        let mut b = [0; 4096];
        assert!(fp.read(&mut b[..])? == 4096);
        let s: Superblock<_> = bincode::deserialize(&b)?;
        assert!(s.magic == MAGIC);
        Tree::open((), s.root_ptr, DefaultMessageAction, dmu)
    };
    tree.dmu().handler().tree_inner.set(tree.inner().clone());
    if write {
        for i in 0..disks.len() {
            tree.dmu().handler().update_allocation_bitmap(
                DiskOffset::new(i, Block(0)),
                Block(1),
                Action::Allocate,
                tree.dmu(),
            )?;
        }
    }

    println!(
        "Running a simple write test

Configuration
===
IO type:  {:>10}
File size:  {:>8}
Cache size: {:>8}
Block size: {:>8}
Iterations: {:>8}
Total:      {:>8}
",
        K::name(),
        Size(file_size),
        Size(cache_size),
        Size(block_size),
        iterations,
        Size(block_size * iterations)
    );

    if write {
        let sync_fn = || {
            let root_ptr = tree.sync()?;
            let mut superblock = Superblock {
                magic: [0; 9],
                root_ptr,
            };
            superblock.magic.copy_from_slice(MAGIC);

            let mut data = bincode::serialize(&superblock)?;
            data.resize(BLOCK_SIZE, 0);

            tree.dmu()
                .pool()
                .write_raw(data.into_boxed_slice(), Block(0))?;
            tree.dmu().pool().flush()?;
            Ok(())
        };
        run_loop::<K, _, _>(
            threads,
            iterations,
            block_size,
            WriteLoop {
                block_size,
                tree: &tree,
                sync: sync_fn,
            },
        )?;

        println!("{}", tree.dmu().cache().read().stats());
    }

    if verify {
        println!("\nVerifying");
        tree.dmu().drop_cache();
        run_loop::<K, _, _>(
            1,
            iterations,
            block_size,
            vrl_new(&tree, iterations, block_size)?,
        )?;
        // run_loop::<K, _, _>(
        //    threads,
        //    iterations,
        //    block_size,
        //    VerifyLoop {
        //        block_size,
        //        tree: &tree,
        //    },
        // )?;

        println!("{}", tree.dmu().cache().read().stats());
    }

    Ok(())
}

trait RunLoop<E: Debug>: Sync + Sized {
    fn inner_loop(&self, key: &[u8; 4]) -> Result<(), E>;
    fn additional_output(&self) -> Result<(), E> {
        Ok(())
    }
    fn before_end(self) -> Result<(), E> {
        Ok(())
    }
}

fn run_loop<G: KeyGenerator, E: Debug, R: RunLoop<E>>(
    num_threads: usize,
    iterations: u64,
    block_size: u64,
    r: R,
) -> Result<(), E> {
    let iterations = iterations / (num_threads as u64); // TODO
    let start = Instant::now();
    Pool::new(num_threads as u32).scoped(|scope| {
        let mp = MultiProgress::new();
        for idx in 0..num_threads {
            let bar = ProgressBar::new(iterations);
            bar.set_style(
                ProgressStyle::default_bar()
                    .template("[{eta}] {bar:40.cyan/blue} {pos:>7}/{len:7} ({percent:>3}%) {msg}"),
            );
            let bar = mp.add(bar);
            let r = &r;
            scope.execute(move || {
                let mut key_generator = G::new(idx, num_threads);
                let mut last_checkpoint = start;
                for i in 0..iterations {
                    if i * block_size % (100 * 1024 * 1024) == 0 {
                        let now = Instant::now();
                        let elapsed = now.duration_since(last_checkpoint);
                        last_checkpoint = now;
                        let bytes = (100 * 1024 * 1024) as f64;
                        let seconds =
                            elapsed.as_secs() as f64 + elapsed.subsec_nanos() as f64 / 1e9;

                        bar.set_message(&format!("{:>11.2}/s", Size((bytes / seconds) as u64)));
                        bar.set_position(i);
                    }
                    r.inner_loop(&key_generator.next()).unwrap(); // TODO ?
                }
                bar.finish();
            });
        }

        mp.set_move_cursor(true);
        mp.join().unwrap();
    });

    r.before_end()?;

    let elapsed = start.elapsed();
    let bytes = block_size * iterations;
    let seconds = elapsed.as_secs() as f64 + elapsed.subsec_nanos() as f64 / 1e9;

    println!("\nDone\n===");
    println!("{}.{}s", elapsed.as_secs(), elapsed.subsec_nanos() / 100000);
    println!("{}/s", Size((bytes as f64 / seconds) as u64));
    Ok(())
}

struct WriteLoop<'a, T: 'a, F> {
    tree: &'a T,
    block_size: u64,
    sync: F,
}

impl<'a, T, F> RunLoop<Box<dyn Error>> for WriteLoop<'a, T, F>
where
    T: TreeLayer<DefaultMessageAction> + Sync,
    F: FnOnce() -> Result<(), Box<dyn Error>> + Sync,
{
    fn inner_loop(&self, key: &[u8; 4]) -> Result<(), Box<dyn Error>> {
        let mut vec = Vec::with_capacity(self.block_size as usize);
        vec.resize(self.block_size as usize, key[0] ^ key[1] ^ key[2] ^ key[3]);
        let msg = DefaultMessageAction::insert_msg(&vec);

        self.tree.insert(&key[..], msg.into())?;
        Ok(())
    }
    fn additional_output(&self) -> Result<(), Box<dyn Error>> {
        Ok(print!(", tree depth: {}", self.tree.depth()?))
    }
    fn before_end(self) -> Result<(), Box<dyn Error>> {
        println!("Tree depth: {}", self.tree.depth()?);
        (self.sync)()
    }
}

struct VerifyLoop<'a, T: 'a> {
    tree: &'a T,
    block_size: u64,
}

impl<'a, T> RunLoop<Box<dyn Error>> for VerifyLoop<'a, T>
where
    T: TreeLayer<DefaultMessageAction> + Sync,
{
    fn inner_loop(&self, key: &[u8; 4]) -> Result<(), Box<dyn Error>> {
        //        debug!("Key: {:?}", &key);
        let data = self
            .tree
            .get(&key[..])?
            .expect(&format!("Key not found: {:?}", &key));
        assert!(data.len() as u64 == self.block_size);
        fn check(mut data: &[u8], i: u8) -> bool {
            let x = 0x0101010101010101 * i as u64;
            while data.len() >= 32 {
                if x != NativeEndian::read_u64(&data[0..8]) {
                    return false;
                }
                if x != NativeEndian::read_u64(&data[8..16]) {
                    return false;
                }
                if x != NativeEndian::read_u64(&data[16..24]) {
                    return false;
                }
                if x != NativeEndian::read_u64(&data[24..32]) {
                    return false;
                }
                data = &data[32..];
            }
            while data.len() >= 8 {
                if x != NativeEndian::read_u64(&data[0..8]) {
                    return false;
                }
                data = &data[8..];
            }
            while data.len() > 0 {
                if (i & 0xFF) as u8 != data[0] {
                    return false;
                }
                data = &data[1..];
            }
            true
        }
        let v = key[0] ^ key[1] ^ key[2] ^ key[3];
        assert!(
            check(&*data, v),
            format!("Failed to verify data at {:?}, found: {:?}", &key, &data)
        );
        Ok(())
    }
    fn before_end(self) -> Result<(), Box<dyn Error>> {
        //        self.tree.dump_cache_statistics();
        Ok(())
    }
}

struct VerifyRangeLoop<I> {
    iter: Mutex<I>,
    block_size: u64,
}

fn vrl_new<T>(
    tree: &T,
    iterations: u64,
    block_size: u64,
) -> Result<VerifyRangeLoop<T::Range>, Box<dyn Error>>
where
    T: TreeLayer<DefaultMessageAction> + Clone + Sync,
{
    let min_key = &[0; 4][..];
    let max_key = &{
        let mut key = [0; 4];
        BigEndian::write_u32(&mut key, iterations as u32);
        key
    }[..];
    Ok(VerifyRangeLoop {
        iter: Mutex::new(tree.range(min_key..max_key)?),
        block_size,
    })
}

impl<I, E> RunLoop<Box<dyn Error>> for VerifyRangeLoop<I>
where
    E: Into<Box<dyn Error>>,
    I: Iterator<Item = Result<(CowBytes, SlicedCowBytes), E>> + Send,
{
    fn inner_loop(&self, key: &[u8; 4]) -> Result<(), Box<dyn Error>> {
        let (actual_key, data) = loop {
            match self.iter.lock().next().expect("No more entries") {
                // skip allocation data :)
                Ok((ref k, _)) if k.len() > 4 => continue,
                Ok(x) => break x,
                Err(e) => return Err(e.into()),
            }
        };
        assert!(
            &actual_key[..] == &key[..],
            "Expected key {:?}, but got {:?}",
            &key[..],
            &actual_key[..]
        );
        assert!(
            data.len() as u64 == self.block_size,
            "{} != {}, found: {:?}",
            data.len(),
            self.block_size,
            data
        );
        fn check(mut data: &[u8], i: u8) -> bool {
            let x = 0x0101010101010101 * i as u64;
            while data.len() >= 32 {
                if x != NativeEndian::read_u64(&data[0..8]) {
                    return false;
                }
                if x != NativeEndian::read_u64(&data[8..16]) {
                    return false;
                }
                if x != NativeEndian::read_u64(&data[16..24]) {
                    return false;
                }
                if x != NativeEndian::read_u64(&data[24..32]) {
                    return false;
                }
                data = &data[32..];
            }
            while data.len() >= 8 {
                if x != NativeEndian::read_u64(&data[0..8]) {
                    return false;
                }
                data = &data[8..];
            }
            while data.len() > 0 {
                if (i & 0xFF) as u8 != data[0] {
                    return false;
                }
                data = &data[1..];
            }
            true
        }
        let v = key[0] ^ key[1] ^ key[2] ^ key[3];
        assert!(
            check(&*data, v),
            format!("Failed to verify data at {:?}, found: {:?}", &key, &data)
        );
        Ok(())
    }
}

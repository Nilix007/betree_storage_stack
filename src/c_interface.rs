//! This module provides the C interface to the database.
#![allow(non_camel_case_types)]
use std::ffi::CStr;
use std::io::{stderr, Write};
use std::os::raw::{c_char, c_int, c_uint};
use std::process::abort;
use std::ptr::null_mut;
use std::ptr::{read, write};
use std::slice::from_raw_parts;
use std::sync::Arc;

use crate::cow_bytes::{CowBytes, SlicedCowBytes};
use crate::database::{Database, Dataset, Error, Snapshot};
use crate::storage_pool::Configuration;
use error_chain::ChainedError;

/// The type for a storage pool configuration
pub struct cfg_t(Configuration);
/// The general error type
pub struct err_t(Error);
/// The database type
pub struct db_t(Database);
/// The data set type
pub struct ds_t(Dataset);
/// The snapshot type
pub struct ss_t(Snapshot);
/// The data set/snapshot name iterator type
pub struct name_iter_t(Box<Iterator<Item = Result<SlicedCowBytes, Error>>>);
/// The range iterator type
pub struct range_iter_t(Box<Iterator<Item = Result<(CowBytes, SlicedCowBytes), Error>>>);

/// A reference counted byte slice
#[repr(C)]
pub struct byte_slice_t {
    ptr: *const c_char,
    len: c_uint,
    arc: *const byte_slice_rc_t,
}

impl From<CowBytes> for byte_slice_t {
    fn from(x: CowBytes) -> Self {
        let ptr = &x[..] as *const [u8] as *const u8 as *const c_char;
        let len = x.len() as c_uint;
        let arc = Arc::into_raw(x.inner) as *const byte_slice_rc_t;
        byte_slice_t { ptr, len, arc }
    }
}

impl From<SlicedCowBytes> for byte_slice_t {
    fn from(x: SlicedCowBytes) -> Self {
        let ptr = &x[..] as *const [u8] as *const u8 as *const c_char;
        let len = x.len() as c_uint;
        let arc = Arc::into_raw(x.data.inner) as *const byte_slice_rc_t;
        byte_slice_t { ptr, len, arc }
    }
}

impl Drop for byte_slice_t {
    fn drop(&mut self) {
        unsafe { Arc::from_raw(self.arc as *const Vec<u8>) };
    }
}

/// A byte slice reference counter
#[repr(C)]
pub struct byte_slice_rc_t(Vec<u8>);

trait HandleResult {
    type Result;
    fn success(self) -> Self::Result;
    fn fail() -> Self::Result;
}

impl HandleResult for () {
    type Result = c_int;
    fn success(self) -> c_int {
        0
    }
    fn fail() -> c_int {
        -1
    }
}

impl HandleResult for Configuration {
    type Result = *mut cfg_t;
    fn success(self) -> *mut cfg_t {
        b(cfg_t(self))
    }
    fn fail() -> *mut cfg_t {
        null_mut()
    }
}

impl HandleResult for Database {
    type Result = *mut db_t;
    fn success(self) -> *mut db_t {
        b(db_t(self))
    }
    fn fail() -> *mut db_t {
        null_mut()
    }
}

impl HandleResult for Dataset {
    type Result = *mut ds_t;
    fn success(self) -> *mut ds_t {
        b(ds_t(self))
    }
    fn fail() -> *mut ds_t {
        null_mut()
    }
}

impl HandleResult for Snapshot {
    type Result = *mut ss_t;
    fn success(self) -> *mut ss_t {
        b(ss_t(self))
    }
    fn fail() -> *mut ss_t {
        null_mut()
    }
}

impl HandleResult for Box<Iterator<Item = Result<SlicedCowBytes, Error>>> {
    type Result = *mut name_iter_t;
    fn success(self) -> *mut name_iter_t {
        b(name_iter_t(self))
    }
    fn fail() -> *mut name_iter_t {
        null_mut()
    }
}

impl HandleResult for Box<Iterator<Item = Result<(CowBytes, SlicedCowBytes), Error>>> {
    type Result = *mut range_iter_t;
    fn success(self) -> *mut range_iter_t {
        b(range_iter_t(self))
    }
    fn fail() -> *mut range_iter_t {
        null_mut()
    }
}

trait HandleResultExt {
    type Result;
    fn handle_result(self, err: *mut *mut err_t) -> Self::Result;
}

impl<T: HandleResult> HandleResultExt for Result<T, Error> {
    type Result = T::Result;
    fn handle_result(self, err: *mut *mut err_t) -> T::Result {
        match self {
            Ok(x) => x.success(),
            Err(e) => {
                handle_err(e, err);
                T::fail()
            }
        }
    }
}

fn handle_err(e: Error, ptr: *mut *mut err_t) {
    if !ptr.is_null() {
        let r = unsafe { &mut *ptr };
        *r = Box::into_raw(Box::new(err_t(e)));
    }
}

fn b<T>(x: T) -> *mut T {
    Box::into_raw(Box::new(x))
}

/// Parse the configuration string for a storage pool.
///
/// On success, return a `cfg_t` which has to be freed with `betree_free_cfg`.
/// On error, return null.  If `err` is not null, store an error in `err`.
#[no_mangle]
pub unsafe extern "C" fn betree_parse_configuration(
    cfg_strings: *const *const c_char,
    cfg_strings_len: c_uint,
    err: *mut *mut err_t,
) -> *mut cfg_t {
    let cfg_strings = from_raw_parts(cfg_strings, cfg_strings_len as usize);
    let cfg_string_iter = cfg_strings
        .iter()
        .map(|&p| CStr::from_ptr(p).to_string_lossy());
    Configuration::parse_zfs_like(cfg_string_iter)
        .map_err(Error::from)
        .handle_result(err)
}

/// Free a configuration object.
#[no_mangle]
pub unsafe extern "C" fn betree_free_cfg(cfg: *mut cfg_t) {
    Box::from_raw(cfg);
}

/// Free an error object.
#[no_mangle]
pub unsafe extern "C" fn betree_free_err(err: *mut err_t) {
    Box::from_raw(err);
}

/// Free a byte slice.
#[no_mangle]
pub unsafe extern "C" fn betree_free_byte_slice(x: *mut byte_slice_t) {
    read(x);
}

/// Free a data set/snapshot name iterator.
#[no_mangle]
pub unsafe extern "C" fn betree_free_name_iter(name_iter: *mut name_iter_t) {
    Box::from_raw(name_iter);
}

/// Free a range iterator.
#[no_mangle]
pub unsafe extern "C" fn betree_free_range_iter(range_iter: *mut range_iter_t) {
    Box::from_raw(range_iter);
}

/// Open a database given by a storate pool configuration.
///
/// On success, return a `db_t` which has to be freed with `betree_close_db`.
/// On error, return null.  If `err` is not null, store an error in `err`.
#[no_mangle]
pub unsafe extern "C" fn betree_open_db(cfg: *const cfg_t, err: *mut *mut err_t) -> *mut db_t {
    Database::open(&(*cfg).0).handle_result(err)
}

/// Create a database given by a storate pool configuration.
///
/// On success, return a `db_t` which has to be freed with `betree_close_db`.
/// On error, return null.  If `err` is not null, store an error in `err`.
///
/// Note that any existing database will be overwritten!
#[no_mangle]
pub unsafe extern "C" fn betree_create_db(cfg: *const cfg_t, err: *mut *mut err_t) -> *mut db_t {
    Database::create(&(*cfg).0).handle_result(err)
}

/// Sync a database.
///
/// On success, return 0.
/// On error, return -1.  If `err` is not null, store an error in `err`.
#[no_mangle]
pub unsafe extern "C" fn betree_sync_db(db: *mut db_t, err: *mut *mut err_t) -> c_int {
    let db = &mut (*db).0;
    db.sync().handle_result(err)
}

/// Closes a database.
///
/// Note that the `db_t` may not be used afterwards.
#[no_mangle]
pub unsafe extern "C" fn betree_close_db(db: *mut db_t) {
    Box::from_raw(db);
}

/// Open a data set identified by the given name.
///
/// On success, return a `ds_t` which has to be freed with `betree_close_ds`.
/// On error, return null.  If `err` is not null, store an error in `err`.
#[no_mangle]
pub unsafe extern "C" fn betree_open_ds(
    db: *mut db_t,
    name: *const c_char,
    len: c_uint,
    err: *mut *mut err_t,
) -> *mut ds_t {
    let db = &mut (*db).0;
    let name = from_raw_parts(name as *const u8, len as usize);
    db.open_dataset(name).handle_result(err)
}

/// Create a new data set with the given name.
///
/// On success, return 0.
/// On error, return -1.  If `err` is not null, store an error in `err`.
///
/// Note that the creation fails if a data set with same name exists already.
#[no_mangle]
pub unsafe extern "C" fn betree_create_ds(
    db: *mut db_t,
    name: *const c_char,
    len: c_uint,
    err: *mut *mut err_t,
) -> c_int {
    let db = &mut (*db).0;
    let name = from_raw_parts(name as *const u8, len as usize);
    db.create_dataset(name).handle_result(err)
}

/// Close a data set.
///
/// On success, return 0.
/// On error, return -1.  If `err` is not null, store an error in `err`.
///
/// Note that the `ds_t` may not be used afterwards.
#[no_mangle]
pub unsafe extern "C" fn betree_close_ds(
    db: *mut db_t,
    ds: *mut ds_t,
    err: *mut *mut err_t,
) -> c_int {
    let db = &mut (*db).0;
    let ds = Box::from_raw(ds).0;
    db.close_dataset(ds).handle_result(err)
}

/// Iterate over all data sets of a database.
///
/// On success, return a `name_iter_t` which has to be freed with
/// `betree_free_name_iter`. On error, return null.  If `err` is not null,
/// store an error in `err`.
#[no_mangle]
pub unsafe extern "C" fn betree_iter_datasets(
    db: *mut db_t,
    err: *mut *mut err_t,
) -> *mut name_iter_t {
    let db = &mut (*db).0;
    db.iter_datasets()
        .map(|it| Box::new(it) as Box<Iterator<Item = Result<SlicedCowBytes, Error>>>)
        .handle_result(err)
}

/// Save the next item in the iterator in `name`.
///
/// On success, return 0.
/// On error, return -1.  If `err` is not null, store an error in `err`.
///
/// Note that `name` may not be used on error but on success,
/// it has to be freed with `betree_free_byte_slice` afterwards.
#[no_mangle]
pub unsafe extern "C" fn betree_name_iter_next(
    name_iter: *mut name_iter_t,
    name: *mut byte_slice_t,
    err: *mut *mut err_t,
) -> c_int {
    let name_iter = &mut (*name_iter).0;
    match name_iter.next() {
        None => -1,
        Some(Err(e)) => {
            handle_err(e, err);
            -1
        }
        Some(Ok(next_name)) => {
            write(name, next_name.into());
            0
        }
    }
}

/// Save the next key-value pair in the iterator.
///
/// On success, return 0.
/// On error, return -1.  If `err` is not null, store an error in `err`.
///
/// Note that `key` and `value` may not be used on error but on success,
/// both have to be freed with `betree_free_byte_slice` afterwards.
#[no_mangle]
pub unsafe extern "C" fn betree_range_iter_next(
    range_iter: *mut range_iter_t,
    key: *mut byte_slice_t,
    value: *mut byte_slice_t,
    err: *mut *mut err_t,
) -> c_int {
    let range_iter = &mut (*range_iter).0;
    match range_iter.next() {
        None => -1,
        Some(Err(e)) => {
            handle_err(e, err);
            -1
        }
        Some(Ok((next_key, next_value))) => {
            write(key, next_key.into());
            write(value, next_value.into());
            0
        }
    }
}

/// Create a new snapshot for the given data set with the given name.
///
/// On success, return 0.
/// On error, return -1.  If `err` is not null, store an error in `err`.
///
/// Note that the creation fails if a snapshot with same name exists already
/// for this data set.
#[no_mangle]
pub unsafe extern "C" fn betree_create_snapshot(
    db: *mut db_t,
    ds: *mut ds_t,
    name: *const c_char,
    len: c_uint,
    err: *mut *mut err_t,
) -> c_int {
    let db = &mut (*db).0;
    let ds = &mut (*ds).0;
    let name = from_raw_parts(name as *const u8, len as usize);
    db.create_snapshot(ds, name).handle_result(err)
}

/// Delete the snapshot for the given data set with the given name.
///
/// On success, return 0.
/// On error, return -1.  If `err` is not null, store an error in `err`.
///
/// Note that the deletion fails if a snapshot with the given name does not
/// exist for this data set.
#[no_mangle]
pub unsafe extern "C" fn betree_delete_snapshot(
    db: *mut db_t,
    ds: *mut ds_t,
    name: *const c_char,
    len: c_uint,
    err: *mut *mut err_t,
) -> c_int {
    let db = &mut (*db).0;
    let ds = &mut (*ds).0;
    let name = from_raw_parts(name as *const u8, len as usize);
    db.delete_snapshot(ds, name).handle_result(err)
}

/// Iterate over all snapshots of a data set.
///
/// On success, return a `name_iter_t` which has to be freed with
/// `betree_free_name_iter`. On error, return null.  If `err` is not null,
/// store an error in `err`.
#[no_mangle]
pub unsafe extern "C" fn betree_iter_snapshots(
    db: *const db_t,
    ds: *const ds_t,
    err: *mut *mut err_t,
) -> *mut name_iter_t {
    let db = &(*db).0;
    let ds = &(*ds).0;
    db.iter_snapshots(ds)
        .map(|it| Box::new(it) as Box<Iterator<Item = Result<SlicedCowBytes, Error>>>)
        .handle_result(err)
}

/// Retrieve the `value` for the given `key`.
///
/// On success, return 0.  If the key does not exist, return -1.
/// On error, return -1.  If `err` is not null, store an error in `err`.
///
/// Note that on success `value` has to be freed with `betree_free_byte_slice`.
#[no_mangle]
pub unsafe extern "C" fn betree_dataset_get(
    ds: *const ds_t,
    key: *const c_char,
    len: c_uint,
    value: *mut byte_slice_t,
    err: *mut *mut err_t,
) -> c_int {
    let ds = &(*ds).0;
    let key = from_raw_parts(key as *const u8, len as usize);
    match ds.get(key) {
        Err(e) => {
            handle_err(e, err);
            -1
        }
        Ok(None) => -1,
        Ok(Some(v)) => {
            write(value, v.into());
            0
        }
    }
}

/// Delete the value for the given `key` if the key exists.
///
/// On success, return 0.
/// On error, return -1.  If `err` is not null, store an error in `err`.
#[no_mangle]
pub unsafe extern "C" fn betree_dataset_delete(
    ds: *const ds_t,
    key: *const c_char,
    len: c_uint,
    err: *mut *mut err_t,
) -> c_int {
    let ds = &(*ds).0;
    let key = from_raw_parts(key as *const u8, len as usize);
    ds.delete(key).handle_result(err)
}

/// Insert the given key-value pair.
///
/// On success, return 0.
/// On error, return -1.  If `err` is not null, store an error in `err`.
///
/// Note that any existing value will be overwritten.
#[no_mangle]
pub unsafe extern "C" fn betree_dataset_insert(
    ds: *const ds_t,
    key: *const c_char,
    key_len: c_uint,
    data: *const c_char,
    data_len: c_uint,
    err: *mut *mut err_t,
) -> c_int {
    let ds = &(*ds).0;
    let key = from_raw_parts(key as *const u8, key_len as usize);
    let data = from_raw_parts(data as *const u8, data_len as usize);
    ds.insert(key, data).handle_result(err)
}

/// Upsert the value for the given key at the given offset.
///
/// On success, return 0.
/// On error, return -1.  If `err` is not null, store an error in `err`.
///
/// Note that the value will be zeropadded as needed.
#[no_mangle]
pub unsafe extern "C" fn betree_dataset_upsert(
    ds: *const ds_t,
    key: *const c_char,
    key_len: c_uint,
    data: *const c_char,
    data_len: c_uint,
    offset: c_uint,
    err: *mut *mut err_t,
) -> c_int {
    let ds = &(*ds).0;
    let key = from_raw_parts(key as *const u8, key_len as usize);
    let data = from_raw_parts(data as *const u8, data_len as usize);
    ds.upsert(key, data, offset as u32).handle_result(err)
}

/// Delete all key-value pairs in the given key range.
/// `low_key` is inclusive, `high_key` is exclusive.
///
/// On success, return 0.
/// On error, return -1.  If `err` is not null, store an error in `err`.
#[no_mangle]
pub unsafe extern "C" fn betree_dataset_range_delete(
    ds: *const ds_t,
    low_key: *const c_char,
    low_key_len: c_uint,
    high_key: *const c_char,
    high_key_len: c_uint,
    err: *mut *mut err_t,
) -> c_int {
    let ds = &(*ds).0;
    let low_key = from_raw_parts(low_key as *const u8, low_key_len as usize);
    let high_key = from_raw_parts(high_key as *const u8, high_key_len as usize);
    ds.range_delete(low_key..high_key).handle_result(err)
}

/// Return the data set's name.
#[no_mangle]
pub unsafe extern "C" fn betree_dataset_name(ds: *const ds_t, len: *mut c_uint) -> *const c_char {
    let ds = &(*ds).0;
    let name = ds.name();
    let ret = &name[..] as *const [u8] as *const u8 as *const c_char;
    write(len, name.len() as c_uint);
    ret
}

/// Iterate over all key-value pairs in the given key range.
/// `low_key` is inclusive, `high_key` is exclusive.
///
/// On success, return a `range_iter_t` which has to be freed with
/// `betree_free_range_iter`. On error, return null.  If `err` is not null,
/// store an error in `err`.
#[no_mangle]
pub unsafe extern "C" fn betree_dataset_range(
    ds: *const ds_t,
    low_key: *const c_char,
    low_key_len: c_uint,
    high_key: *const c_char,
    high_key_len: c_uint,
    err: *mut *mut err_t,
) -> *mut range_iter_t {
    let ds = &(*ds).0;
    let low_key = from_raw_parts(low_key as *const u8, low_key_len as usize);
    let high_key = from_raw_parts(high_key as *const u8, high_key_len as usize);
    ds.range(low_key..high_key).handle_result(err)
}

/// Retrieve the `value` for the given `key`.
///
/// On success, return 0.  If the key does not exist, return -1.
/// On error, return -1.  If `err` is not null, store an error in `err`.
///
/// Note that on success `value` has to be freed with `betree_free_byte_slice`.
#[no_mangle]
pub unsafe extern "C" fn betree_snapshot_get(
    ss: *const ss_t,
    key: *const c_char,
    len: c_uint,
    value: *mut byte_slice_t,
    err: *mut *mut err_t,
) -> c_int {
    let ss = &(*ss).0;
    let key = from_raw_parts(key as *const u8, len as usize);
    match ss.get(key) {
        Err(e) => {
            handle_err(e, err);
            -1
        }
        Ok(None) => -1,
        Ok(Some(v)) => {
            write(value, v.into());
            0
        }
    }
}

/// Iterate over all key-value pairs in the given key range.
/// `low_key` is inclusive, `high_key` is exclusive.
///
/// On success, return a `range_iter_t` which has to be freed with
/// `betree_free_range_iter`. On error, return null.  If `err` is not null,
/// store an error in `err`.
#[no_mangle]
pub unsafe extern "C" fn betree_snapshot_range(
    ss: *const ss_t,
    low_key: *const c_char,
    low_key_len: c_uint,
    high_key: *const c_char,
    high_key_len: c_uint,
    err: *mut *mut err_t,
) -> *mut range_iter_t {
    let ss = &(*ss).0;
    let low_key = from_raw_parts(low_key as *const u8, low_key_len as usize);
    let high_key = from_raw_parts(high_key as *const u8, high_key_len as usize);
    ss.range(low_key..high_key).handle_result(err)
}

/// Print the given error to stderr.
#[no_mangle]
pub unsafe extern "C" fn betree_print_error(err: *mut err_t) {
    let err = &(*err).0;
    if write!(&mut stderr(), "{}", err.display_chain()).is_err() {
        abort();
    }
}

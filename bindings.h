#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>

/*
 * A byte slice reference counter
 */
typedef struct byte_slice_rc_t byte_slice_rc_t;

/*
 * The type for a storage pool configuration
 */
typedef struct cfg_t cfg_t;

/*
 * The database type
 */
typedef struct db_t db_t;

/*
 * The data set type
 */
typedef struct ds_t ds_t;

/*
 * The general error type
 */
typedef struct err_t err_t;

/*
 * The data set/snapshot name iterator type
 */
typedef struct name_iter_t name_iter_t;

/*
 * The range iterator type
 */
typedef struct range_iter_t range_iter_t;

/*
 * The snapshot type
 */
typedef struct ss_t ss_t;

/*
 * A reference counted byte slice
 */
typedef struct {
  const char *ptr;
  unsigned int len;
  const byte_slice_rc_t *arc;
} byte_slice_t;

/*
 * Closes a database.
 *
 * Note that the `db_t` may not be used afterwards.
 */
void betree_close_db(db_t *db);

/*
 * Close a data set.
 *
 * On success, return 0.
 * On error, return -1.  If `err` is not null, store an error in `err`.
 *
 * Note that the `ds_t` may not be used afterwards.
 */
int betree_close_ds(db_t *db, ds_t *ds, err_t **err);

/*
 * Create a database given by a storate pool configuration.
 *
 * On success, return a `db_t` which has to be freed with `betree_close_db`.
 * On error, return null.  If `err` is not null, store an error in `err`.
 *
 * Note that any existing database will be overwritten!
 */
db_t *betree_create_db(const cfg_t *cfg, err_t **err);

/*
 * Create a new data set with the given name.
 *
 * On success, return 0.
 * On error, return -1.  If `err` is not null, store an error in `err`.
 *
 * Note that the creation fails if a data set with same name exists already.
 */
int betree_create_ds(db_t *db, const char *name, unsigned int len, err_t **err);

/*
 * Create a new snapshot for the given data set with the given name.
 *
 * On success, return 0.
 * On error, return -1.  If `err` is not null, store an error in `err`.
 *
 * Note that the creation fails if a snapshot with same name exists already for this data set.
 */
int betree_create_snapshot(db_t *db, ds_t *ds, const char *name, unsigned int len, err_t **err);

/*
 * Delete the value for the given `key` if the key exists.
 *
 * On success, return 0.
 * On error, return -1.  If `err` is not null, store an error in `err`.
 */
int betree_dataset_delete(const ds_t *ds, const char *key, unsigned int len, err_t **err);

/*
 * Retrieve the `value` for the given `key`.
 *
 * On success, return 0.  If the key does not exist, return -1.
 * On error, return -1.  If `err` is not null, store an error in `err`.
 *
 * Note that on success `value` has to be freed with `betree_free_byte_slice`.
 */
int betree_dataset_get(const ds_t *ds,
                       const char *key,
                       unsigned int len,
                       byte_slice_t *value,
                       err_t **err);

/*
 * Insert the given key-value pair.
 *
 * On success, return 0.
 * On error, return -1.  If `err` is not null, store an error in `err`.
 *
 * Note that any existing value will be overwritten.
 */
int betree_dataset_insert(const ds_t *ds,
                          const char *key,
                          unsigned int key_len,
                          const char *data,
                          unsigned int data_len,
                          err_t **err);

/*
 * Return the data set's name.
 */
const char *betree_dataset_name(const ds_t *ds, unsigned int *len);

/*
 * Iterate over all key-value pairs in the given key range.
 * `low_key` is inclusive, `high_key` is exclusive.
 *
 * On success, return a `range_iter_t` which has to be freed with `betree_free_range_iter`.
 * On error, return null.  If `err` is not null, store an error in `err`.
 */
range_iter_t *betree_dataset_range(const ds_t *ds,
                                   const char *low_key,
                                   unsigned int low_key_len,
                                   const char *high_key,
                                   unsigned int high_key_len,
                                   err_t **err);

/*
 * Delete all key-value pairs in the given key range.
 * `low_key` is inclusive, `high_key` is exclusive.
 *
 * On success, return 0.
 * On error, return -1.  If `err` is not null, store an error in `err`.
 */
int betree_dataset_range_delete(const ds_t *ds,
                                const char *low_key,
                                unsigned int low_key_len,
                                const char *high_key,
                                unsigned int high_key_len,
                                err_t **err);

/*
 * Upsert the value for the given `key` at the given offset.
 *
 * On success, return 0.
 * On error, return -1.  If `err` is not null, store an error in `err`.
 *
 * Note that the value will be zeropadded as needed.
 */
int betree_dataset_upsert(const ds_t *ds,
                          const char *key,
                          unsigned int key_len,
                          const char *data,
                          unsigned int data_len,
                          unsigned int offset,
                          err_t **err);

/*
 * Delete the snapshot for the given data set with the given name.
 *
 * On success, return 0.
 * On error, return -1.  If `err` is not null, store an error in `err`.
 *
 * Note that the deletion fails if a snapshot with the given name does not existfor this data set.
 */
int betree_delete_snapshot(db_t *db, ds_t *ds, const char *name, unsigned int len, err_t **err);

/*
 * Free a byte slice.
 */
void betree_free_byte_slice(byte_slice_t *x);

/*
 * Free a configuration object.
 */
void betree_free_cfg(cfg_t *cfg);

/*
 * Free an error object.
 */
void betree_free_err(err_t *err);

/*
 * Free a data set/snapshot name iterator.
 */
void betree_free_name_iter(name_iter_t *name_iter);

/*
 * Free a range iterator.
 */
void betree_free_range_iter(range_iter_t *range_iter);

/*
 * Iterate over all data sets of a database.
 *
 * On success, return a `name_iter_t` which has to be freed with `betree_free_name_iter`.
 * On error, return null.  If `err` is not null, store an error in `err`.
 */
name_iter_t *betree_iter_datasets(db_t *db, err_t **err);

/*
 * Iterate over all snapshots of a data set.
 *
 * On success, return a `name_iter_t` which has to be freed with `betree_free_name_iter`.
 * On error, return null.  If `err` is not null, store an error in `err`.
 */
name_iter_t *betree_iter_snapshots(const db_t *db, const ds_t *ds, err_t **err);

/*
 * Save the next item in the iterator in `name`.
 *
 * On success, return 0.
 * On error, return -1.  If `err` is not null, store an error in `err`.
 *
 * Note that `name` may not be used on error but on success,
 * it has to be freed with `betree_free_byte_slice` afterwards.
 */
int betree_name_iter_next(name_iter_t *name_iter, byte_slice_t *name, err_t **err);

/*
 * Open a database given by a storate pool configuration.
 *
 * On success, return a `db_t` which has to be freed with `betree_close_db`.
 * On error, return null.  If `err` is not null, store an error in `err`.
 */
db_t *betree_open_db(const cfg_t *cfg, err_t **err);

/*
 * Open a data set identified by the given name.
 *
 * On success, return a `ds_t` which has to be freed with `betree_close_ds`.
 * On error, return null.  If `err` is not null, store an error in `err`.
 */
ds_t *betree_open_ds(db_t *db, const char *name, unsigned int len, err_t **err);

/*
 * Parse the configuration string for a storage pool.
 *
 * On success, return a `cfg_t` which has to be freed with `betree_free_cfg`.
 * On error, return null.  If `err` is not null, store an error in `err`.
 */
cfg_t *betree_parse_configuration(const char *const *cfg_strings,
                                  unsigned int cfg_strings_len,
                                  err_t **err);

/*
 * Print the given error to stderr.
 */
void betree_print_error(err_t *err);

/*
 * Save the next key-value pair in the iterator.
 *
 * On success, return 0.
 * On error, return -1.  If `err` is not null, store an error in `err`.
 *
 * Note that `key` and `value` may not be used on error but on success,
 * both have to be freed with `betree_free_byte_slice` afterwards.
 */
int betree_range_iter_next(range_iter_t *range_iter,
                           byte_slice_t *key,
                           byte_slice_t *value,
                           err_t **err);

/*
 * Retrieve the `value` for the given `key`.
 *
 * On success, return 0.  If the key does not exist, return -1.
 * On error, return -1.  If `err` is not null, store an error in `err`.
 *
 * Note that on success `value` has to be freed with `betree_free_byte_slice`.
 */
int betree_snapshot_get(const ss_t *ss,
                        const char *key,
                        unsigned int len,
                        byte_slice_t *value,
                        err_t **err);

/*
 * Iterate over all key-value pairs in the given key range.
 * `low_key` is inclusive, `high_key` is exclusive.
 *
 * On success, return a `range_iter_t` which has to be freed with `betree_free_range_iter`.
 * On error, return null.  If `err` is not null, store an error in `err`.
 */
range_iter_t *betree_snapshot_range(const ss_t *ss,
                                    const char *low_key,
                                    unsigned int low_key_len,
                                    const char *high_key,
                                    unsigned int high_key_len,
                                    err_t **err);

/*
 * Sync a database.
 *
 * On success, return 0.
 * On error, return -1.  If `err` is not null, store an error in `err`.
 */
int betree_sync_db(db_t *db, err_t **err);

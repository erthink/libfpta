/*
 * Copyright 2016-2017 libfpta authors: please see AUTHORS file.
 *
 * This file is part of libfpta, aka "Fast Positive Tables".
 *
 * libfpta is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * libfpta is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with libfpta.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "fast_positive/config.h"
#include "fast_positive/tables.h"
#include "fast_positive/tuples_internal.h"

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <malloc.h>
#include <stdlib.h>
#include <time.h>

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#else
static __inline long int mrand48(void) { return 0; }
#endif /* windows must die (HAVE_UNISTD_H) */

#ifdef CMAKE_HAVE_PTHREAD_H
#include <pthread.h>
#else
struct pthread_rwlock_t {};
static int __inline pthread_rwlock_init(struct pthread_rwlock_t *, void *) {
  return ENOSYS;
}
static int __inline pthread_rwlock_rdlock(struct pthread_rwlock_t *) {
  return ENOSYS;
}
static int __inline pthread_rwlock_wrlock(struct pthread_rwlock_t *) {
  return ENOSYS;
}
static int __inline pthread_rwlock_unlock(struct pthread_rwlock_t *) {
  return ENOSYS;
}
static int __inline pthread_rwlock_destroy(struct pthread_rwlock_t *) {
  return ENOSYS;
}

struct pthread_mutex_t {};
static int __inline pthread_mutex_init(struct pthread_mutex_t *, void *) {
  return ENOSYS;
}
static int __inline pthread_mutex_lock(struct pthread_mutex_t *) {
  return ENOSYS;
}
static int __inline pthread_mutex_unlock(struct pthread_mutex_t *) {
  return ENOSYS;
}
static int __inline pthread_mutex_destroy(struct pthread_mutex_t *) {
  return ENOSYS;
}

#endif /* windows must die (CMAKE_HAVE_PTHREAD_H) */

#ifdef __linux__
#include "libmdbx/mdbx.h"
#else
#ifdef _MSC_VER
#pragma message("Windows would die, be ready ;)")
#endif
typedef struct stub_MDB_env MDB_env;
typedef struct stub_MDB_txn MDB_txn;
typedef struct stub_MDB_cursor MDB_cursor;
typedef struct iovec MDB_val;
#define mv_size iov_len
#define mv_data iov_base
typedef unsigned MDB_dbi;
typedef int MDB_cursor_op;
typedef int(MDB_cmp_func)(const MDB_val *a, const MDB_val *b);

typedef struct mdbx_canary { size_t v; } mdbx_canary;
static __inline int mdbx_canary_put(MDB_txn *, const mdbx_canary *) {
  return ENOSYS;
}
static __inline size_t mdbx_canary_get(MDB_txn *, mdbx_canary *) { return 0; }
static __inline int mdbx_cursor_eof(MDB_cursor *) { return ENOSYS; }
static __inline int mdbx_replace(MDB_txn *, MDB_dbi, MDB_val *, MDB_val *,
                                 MDB_val *, unsigned) {
  return ENOSYS;
}
static __inline int mdbx_get_ex(MDB_txn *, MDB_dbi, MDB_val *, MDB_val *,
                                int *) {
  return ENOSYS;
}

static __inline int mdbx_is_dirty(const MDB_txn *, const void *) {
  return ENOSYS;
}

static __inline int mdbx_dbi_open_ex(MDB_txn *, const char *, unsigned,
                                     MDB_dbi *, MDB_cmp_func *,
                                     MDB_cmp_func *) {
  return ENOSYS;
}

static __inline int mdbx_get(MDB_txn *, MDB_dbi, MDB_val *, MDB_val *) {
  return ENOSYS;
}
static __inline int mdbx_put(MDB_txn *, MDB_dbi, MDB_val *, MDB_val *,
                             unsigned) {
  return ENOSYS;
}
static __inline int mdbx_del(MDB_txn *, MDB_dbi, MDB_val *, MDB_val *) {
  return ENOSYS;
}
static __inline int mdbx_set_dupsort(MDB_txn *, MDB_dbi, MDB_cmp_func *) {
  return ENOSYS;
}
static __inline int mdbx_set_compare(MDB_txn *, MDB_dbi, MDB_cmp_func *) {
  return ENOSYS;
}
static __inline int mdbx_drop(MDB_txn *, MDB_dbi, int) { return ENOSYS; }
static __inline void mdbx_dbi_close(MDB_env *, MDB_dbi) {}
static __inline int mdbx_dbi_open(MDB_txn *, const char *, unsigned,
                                  MDB_dbi *) {
  return ENOSYS;
}
static __inline int mdbx_txn_renew(MDB_txn *) { return ENOSYS; }
static __inline int mdbx_txn_reset(MDB_txn *) { return ENOSYS; }
static __inline int mdbx_txn_abort(MDB_txn *) { return ENOSYS; }
static __inline int mdbx_txn_commit(MDB_txn *) { return ENOSYS; }
static __inline int mdbx_txn_begin(MDB_env *, MDB_txn *, unsigned,
                                   MDB_txn **) {
  return ENOSYS;
}
static __inline int mdbx_env_set_maxdbs(MDB_env *, MDB_dbi) { return ENOSYS; }
static __inline int mdbx_env_set_maxreaders(MDB_env *, unsigned) {
  return ENOSYS;
}
static __inline void mdbx_env_close(MDB_env *) {}
static __inline int mdbx_env_sync(MDB_env *, int) { return ENOSYS; }
static __inline int mdbx_env_open(MDB_env *, const char *, unsigned, mode_t) {
  return ENOSYS;
}
static __inline int mdbx_env_create(MDB_env **) { return ENOSYS; }
static __inline const char *mdbx_strerror(int) { return "ENOSYS"; }
static __inline int mdbx_cursor_open(MDB_txn *, MDB_dbi, MDB_cursor **) {
  return ENOSYS;
}
static __inline void mdbx_cursor_close(MDB_cursor *) {}
static __inline int mdbx_cursor_renew(MDB_txn *, MDB_cursor *) {
  return ENOSYS;
}
static __inline int mdbx_cursor_get(MDB_cursor *, MDB_val *, MDB_val *,
                                    MDB_cursor_op) {
  return ENOSYS;
}
static __inline int mdbx_cursor_put(MDB_cursor *, MDB_val *, MDB_val *,
                                    unsigned) {
  return ENOSYS;
}
static __inline int mdbx_cursor_del(MDB_cursor *, unsigned) { return ENOSYS; }
static __inline int mdbx_cursor_count(MDB_cursor *, size_t *) {
  return ENOSYS;
}
static __inline int mdbx_cmp(MDB_txn *, MDB_dbi, const MDB_val *,
                             const MDB_val *) {
  return 0;
}
static __inline int mdbx_dcmp(MDB_txn *, MDB_dbi, const MDB_val *,
                              const MDB_val *) {
  return 0;
}
static __inline int mdbx_env_set_userctx(MDB_env *, void *) { return ENOSYS; }
static __inline int mdbx_env_set_mapsize(MDB_env *, size_t) { return ENOSYS; }
static __inline int mdbx_env_close_ex(MDB_env *, int) { return ENOSYS; }

enum stub_MDB_defs {
  MDB_NOSUBDIR,
  MDB_RDONLY,
  MDBX_LIFORECLAIM,
  MDBX_COALESCE,
  MDB_NOSYNC,
  MDB_NOMETASYNC,
  MDB_WRITEMAP,
  MDB_MAPASYNC,
  MDBX_UTTERLY_NOSYNC,
  MDB_SUCCESS,
  MDB_NOTFOUND,
  MDB_KEYEXIST,
  MDBX_RESULT_TRUE,
  MDBX_RESULT_FALSE,

  MDB_REVERSEKEY,
  MDB_DUPSORT,
  MDB_INTEGERKEY,
  MDB_DUPFIXED,
  MDB_INTEGERDUP,
  MDB_REVERSEDUP,
  MDB_CREATE,
  MDB_NOOVERWRITE,
  MDB_NODUPDATA,
  MDB_CURRENT,

  MDB_GET_CURRENT,
  MDB_NEXT,
  MDB_NEXT_NODUP,
  MDB_PREV,
  MDB_PREV_NODUP,
  MDB_PREV_DUP,
  MDB_NEXT_DUP,
  MDB_FIRST,
  MDB_SET_KEY,
  MDB_LAST,
  MDB_FIRST_DUP,
  MDB_LAST_DUP,
  MDB_GET_BOTH,
  MDB_SET,
  MDB_GET_BOTH_RANGE,
  MDB_SET_RANGE,
};
#endif /* windows must die (__LINUX__) */

#include "t1ha/t1ha.h"

#include <algorithm>
#include <cfloat> // for float limits
#include <cmath>  // for fabs()
#include <functional>

#if defined(__cland__) && !__CLANG_PREREQ(3, 8)
// LY: workaround for https://llvm.org/bugs/show_bug.cgi?id=18402
extern "C" char *gets(char *);
#endif

void fpta_pollute(void *ptr, size_t bytes, uintptr_t xormask = 0);

//----------------------------------------------------------------------------

struct fpta_table_schema {
  uint64_t checksum;
  uint32_t signature;
  uint32_t count;
  uint64_t version;
  fpta_shove_t shove;
  fpta_shove_t columns[fpta_max_cols];
};

static __inline size_t fpta_table_schema_size(size_t cols) {
  assert(cols <= fpta_max_cols);
  return sizeof(fpta_table_schema) -
         sizeof(fpta_shove_t) * (fpta_max_cols - cols);
}

enum fpta_internals {
  /* используем некорретный для индекса набор флагов, чтобы в fpta_name
   * отличать таблицу от колонки, у таблицы в internal будет fpta_ftable. */
  fpta_flag_table = fpta_index_fsecondary,
  fpta_dbi_cache_size = fpta_tables_max * 2,
  FTPA_SCHEMA_SIGNATURE = 603397211,
  FTPA_SCHEMA_CHECKSEED = 1546032023
};

//----------------------------------------------------------------------------

struct fpta_db {
  fpta_db(const fpta_db &) = delete;
  pthread_rwlock_t schema_rwlock;
  MDB_env *mdbx_env;
  MDB_dbi schema_dbi;
  bool alterable_schema;

  pthread_mutex_t dbi_mutex;
  fpta_shove_t dbi_shoves[fpta_dbi_cache_size];
  MDB_dbi dbi_handles[fpta_dbi_cache_size];
};

struct fpta_txn {
  fpta_txn(const fpta_txn &) = delete;
  fpta_db *db;
  MDB_txn *mdbx_txn;
  fpta_level level;
  uint64_t schema_version;
  uint64_t data_version;
};

struct fpta_key {
  fpta_key() {
#ifndef NDEBUG
    fpta_pollute(this, sizeof(fpta_key));
#endif
  }
  fpta_key(const fpta_key &) = delete;

  MDB_val mdbx;
  union {
    int32_t i32;
    uint32_t u32;
    int64_t i64;
    uint64_t u64;
    float f32;
    double f64;

    struct {
      uint64_t head[fpta_max_keylen / sizeof(uint64_t)];
      uint64_t tailhash;
    } longkey_msb;
    struct {
      uint64_t headhash;
      uint64_t tail[fpta_max_keylen / sizeof(uint64_t)];
    } longkey_lsb;
  } place;
};

struct fpta_cursor {
  fpta_cursor(const fpta_cursor &) = delete;
  MDB_cursor *mdbx_cursor;
  fpta_cursor_options options;
  MDB_val current;

#if defined(_MSC_VER) && (_MSC_FULL_VER < 190024215)
#define poor nullptr
#else
  static constexpr void *poor = nullptr;
#endif
  bool is_poor() const { return current.iov_base == poor; }
  void set_poor() { current.iov_base = poor; }

  enum eof_mode : uintptr_t { before_first = 1, after_last = 2 };

#if FPTA_ENABLE_RETURN_INTO_RANGE
  static void *eof(eof_mode mode = after_last) { return (void *)mode; }
  bool is_filled() const { return current.iov_base > eof(); }

  int unladed_state() const {
    assert(!is_filled());
    return current.iov_base ? FPTA_NODATA : FPTA_ECURSOR;
  }

  bool is_before_first() const {
    return current.iov_base == eof(before_first);
  }
  bool is_after_last() const { return current.iov_base == eof(after_last); }
  void set_eof(eof_mode mode) { current.iov_base = eof(mode); }
#else
  bool is_filled() const { return !is_poor(); }
  int unladed_state() const {
    assert(!is_filled());
    return FPTA_ECURSOR;
  }
  void set_eof(eof_mode mode) {
    (void)mode;
    set_poor();
  }
  bool is_before_first() const { return false; }
  bool is_after_last() const { return false; }
#endif

  fpta_name *table_id;
  struct {
    unsigned shove;
    unsigned column_order;
    unsigned mdbx_dbi;
  } index;

  fpta_value range_from_value;
  fpta_key range_from_key;

  fpta_value range_to_value;
  fpta_key range_to_key;

  fpta_filter *filter;
  fpta_txn *txn;
  fpta_db *db;
};

//----------------------------------------------------------------------------

static __inline fpta_shove_t fpta_column_shove(fpta_shove_t shove,
                                               fptu_type data_type,
                                               fpta_index_type index_type) {
  assert((data_type & ~fpta_column_typeid_mask) == 0);
  assert((index_type & ~fpta_column_index_mask) == 0);
  assert((shove & ((1 << fpta_name_hash_shift) - 1)) == 0);
  return shove | data_type | index_type;
}

static __inline bool fpta_shove_eq(fpta_shove_t a, fpta_shove_t b) {
  static_assert(fpta_name_hash_shift > 0, "expect hash/shove is shifted");
  /* A равно B, если отличия только в бладших битах */
  return (a ^ b) < ((1u << fpta_name_hash_shift) - 1);
}

static __inline fptu_type fpta_shove2type(fpta_shove_t shove) {
  static_assert(fpta_column_typeid_shift == 0,
                "expecting column_typeid_shift is zero");
  unsigned type = shove & fpta_column_typeid_mask;
  return (fptu_type)type;
}

static __inline fpta_index_type fpta_shove2index(fpta_shove_t shove) {
  static_assert((int)fpta_primary < fpta_column_index_mask,
                "check fpta_column_index_mask");
  static_assert((int)fpta_primary > (1 << fpta_column_index_shift) - 1,
                "expect fpta_primary is shifted");
  static_assert((fpta_column_index_mask & fpta_column_typeid_mask) == 0,
                "seems a bug");
  unsigned index = shove & fpta_column_index_mask;
  return (fpta_index_type)index;
}

static __inline fptu_type fpta_id2type(const fpta_name *id) {
  return fpta_shove2type(id->shove);
}

static __inline fpta_index_type fpta_id2index(const fpta_name *id) {
  return fpta_shove2index(id->shove);
}

MDB_cmp_func *fpta_index_shove2comparator(fpta_shove_t shove);
unsigned fpta_index_shove2primary_dbiflags(fpta_shove_t shove);
unsigned fpta_index_shove2secondary_dbiflags(fpta_shove_t pk_shove,
                                             fpta_shove_t shove);

bool fpta_index_is_compat(fpta_shove_t shove, const fpta_value &value);

int fpta_index_value2key(fpta_shove_t shove, const fpta_value &value,
                         fpta_key &key, bool copy = false);
int fpta_index_key2value(fpta_shove_t shove, const MDB_val &mdbx_key,
                         fpta_value &key_value);

int fpta_index_row2key(fpta_shove_t shove, size_t column, const fptu_ro &row,
                       fpta_key &key, bool copy = false);

int fpta_secondary_upsert(fpta_txn *txn, fpta_name *table_id,
                          MDB_val pk_key_old, const fptu_ro &row_old,
                          MDB_val pk_key_new, const fptu_ro &row_new,
                          unsigned stepover);

int fpta_check_constraints(fpta_txn *txn, fpta_name *table_id,
                           const fptu_ro &row_old, const fptu_ro &row_new,
                           unsigned stepover);

int fpta_secondary_remove(fpta_txn *txn, fpta_name *table_id, MDB_val &pk_key,
                          const fptu_ro &row_old, unsigned stepover);

//----------------------------------------------------------------------------

int fpta_open_column(fpta_txn *txn, fpta_name *column_id);
int fpta_open_table(fpta_txn *txn, fpta_name *table_id);
int fpta_open_secondaries(fpta_txn *txn, fpta_name *table_id,
                          MDB_dbi *dbi_array);

//----------------------------------------------------------------------------

fpta_cursor *fpta_cursor_alloc(fpta_db *db);
void fpta_cursor_free(fpta_db *db, fpta_cursor *cursor);

//----------------------------------------------------------------------------

fptu_lge fpta_filter_cmp(const fptu_field *pf, const fpta_value &right);
bool fpta_filter_validate(const fpta_filter *filter);
bool fpta_cursor_validate(const fpta_cursor *cursor, fpta_level min_level);
int fpta_column_set_validate(fpta_column_set *column_set);
bool fpta_schema_validate(const MDB_val def);

static __inline bool fpta_table_has_secondary(const fpta_name *table_id) {
  return table_id->table.def->count > 1 &&
         fpta_index_none != fpta_shove2index(table_id->table.def->columns[1]);
}

static __inline bool fpta_db_validate(fpta_db *db) {
  if (unlikely(db == nullptr || db->mdbx_env == nullptr))
    return false;

  // TODO
  return true;
}

static __inline bool fpta_txn_validate(fpta_txn *txn, fpta_level min_level) {
  if (unlikely(txn == nullptr || !fpta_db_validate(txn->db)))
    return false;
  if (unlikely(txn->level < min_level || txn->level > fpta_schema))
    return false;

  // TODO
  return true;
}

enum fpta_schema_item { fpta_table, fpta_column };

static __inline bool fpta_id_validate(const fpta_name *id,
                                      fpta_schema_item schema_item) {
  if (unlikely(id == nullptr))
    return false;

  switch (schema_item) {
  default:
    return false;
  case fpta_table:
    if (unlikely(fpta_shove2index(id->shove) !=
                 (fpta_index_type)fpta_flag_table))
      return false;
    // TODO: ?
    return true;

  case fpta_column:
    if (unlikely(fpta_shove2index(id->shove) ==
                 (fpta_index_type)fpta_flag_table))
      return false;
    // TODO: ?
    return true;
  }
}

static __inline bool fpta_index_is_unique(fpta_shove_t index) {
  assert(index != fpta_index_none);
  return (index & fpta_index_funique) != 0;
}

static __inline bool fpta_index_is_ordered(fpta_shove_t index) {
  assert(index != fpta_index_none);
  return (index & fpta_index_fordered) != 0;
}

static __inline bool fpta_index_is_reverse(fpta_shove_t index) {
  assert(index != fpta_index_none);
  return (index & fpta_index_fobverse) == 0;
}

static __inline bool fpta_index_is_primary(fpta_shove_t index) {
  assert(index != fpta_index_none);
  return (index & fpta_index_fsecondary) == 0;
}

static __inline bool fpta_index_is_secondary(fpta_shove_t index) {
  assert(index != fpta_index_none);
  return (index & fpta_index_fsecondary) != 0;
}

static __inline bool fpta_cursor_is_ordered(fpta_cursor_options op) {
  return (op & (fpta_descending | fpta_ascending)) != fpta_unsorted;
}

static __inline bool fpta_cursor_is_descending(fpta_cursor_options op) {
  return (op & (fpta_descending | fpta_ascending)) == fpta_descending;
}

static __inline bool fpta_cursor_is_ascending(fpta_cursor_options op) {
  return (op & (fpta_descending | fpta_ascending)) == fpta_ascending;
}

int fpta_inconsistent_abort(fpta_txn *txn, int errnum);

static __inline bool fpta_is_same(const MDB_val &a, const MDB_val &b) {
  return a.iov_len == b.iov_len &&
         memcmp(a.iov_base, b.iov_base, a.iov_len) == 0;
}

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

#ifdef _MSC_VER
#pragma warning(disable : 4514) /* 'xyz': unreferenced inline function         \
                                   has been removed */
#pragma warning(disable : 4710) /* 'xyz': function not inlined */
#pragma warning(disable : 4711) /* function 'xyz' selected for                 \
                                   automatic inline expansion */
#pragma warning(disable : 4061) /* enumerator 'abc' in switch of enum          \
                                   'xyz' is not explicitly handled by a case   \
                                   label */
#pragma warning(disable : 4201) /* nonstandard extension used :                \
                                   nameless struct / union */
#pragma warning(disable : 4127) /* conditional expression is constant          \
                                   */

#pragma warning(push, 1)
#pragma warning(disable : 4530) /* C++ exception handler used, but             \
                                    unwind semantics are not enabled. Specify  \
                                    /EHsc */
#pragma warning(disable : 4577) /* 'noexcept' used with no exception           \
                                    handling mode specified; termination on    \
                                    exception is not guaranteed. Specify /EHsc \
                                    */
#endif                          /* _MSC_VER (warnings) */

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <malloc.h>
#include <stdlib.h>
#include <time.h>

__extern_C int32_t mrand64(void);

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#else
static __inline int32_t mrand48(void) { return mrand64(); }
#endif

#include <algorithm>
#include <cfloat> // for float limits
#include <cmath>  // for fabs()
#include <functional>

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#include "libmdbx/mdbx.h"
#include "t1ha/t1ha.h"

#if defined(__cland__) && !__CLANG_PREREQ(3, 8)
// LY: workaround for https://llvm.org/bugs/show_bug.cgi?id=18402
extern "C" char *gets(char *);
#endif

//----------------------------------------------------------------------------

struct fpta_table_schema {
  uint64_t checksum;
  uint32_t signature;
  uint32_t count;
  uint64_t csn;
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
  fpta_dbi_cache_size = fpta_tables_max * 5,
  FTPA_SCHEMA_SIGNATURE = 603397211,
  FTPA_SCHEMA_CHECKSEED = 1546032023
};

//----------------------------------------------------------------------------

struct fpta_txn {
  fpta_txn(const fpta_txn &) = delete;
  fpta_db *db;
  MDB_txn *mdbx_txn;
  fpta_level level;
  int unused_gap;
  uint64_t db_version;
  mdbx_canary canary;

  uint64_t &schema_version() { return canary.x; }
  uint64_t &db_sequence() { return canary.y; }
  uint64_t &manna() { return canary.z; }

  uint64_t schema_version() const { return canary.x; }
  uint64_t db_sequence() const { return canary.y; }
  uint64_t manna() const { return canary.z; }
};

struct fpta_key {
  fpta_key() {
#ifndef NDEBUG
    fpta_pollute(this, sizeof(fpta_key), 0);
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

  bool is_before_first() const { return current.iov_base == eof(before_first); }
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
  fpta_cursor_options options;

  fpta_key range_from_key;
  fpta_key range_to_key;

  const fpta_filter *filter;
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

int fpta_internal_abort(fpta_txn *txn, int errnum);

static __inline bool fpta_is_same(const MDB_val &a, const MDB_val &b) {
  return a.iov_len == b.iov_len &&
         memcmp(a.iov_base, b.iov_base, a.iov_len) == 0;
}

namespace std {
FPTA_API string to_string(const MDB_val &);
FPTA_API string to_string(const fpta_key &);
}

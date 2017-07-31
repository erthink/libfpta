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

/*
 * libfpta = { Fast Positive Tables, aka Позитивные Таблицы }
 *
 * Ultra fast, compact, embeddable storage engine for (semi)structured data:
 * multiprocessing with zero-overhead, full ACID semantics with MVCC,
 * variety of indexes, saturation, sequences and much more.
 * Please see README.md at https://github.com/leo-yuriev/libfpta
 *
 * The Future will Positive. Всё будет хорошо.
 *
 * "Позитивные таблицы" предназначены для построения высокоскоростных
 * локальных хранилищ структурированных данных, с целевой производительностью
 * до 1.000.000 запросов в секунду на каждое ядро процессора.
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
#pragma warning(disable : 4061) /* enumerator 'abc' in switch of enum 'xyz' is \
                                   not explicitly handled by a case label */
#pragma warning(disable : 4201) /* nonstandard extension used :                \
                                   nameless struct / union */
#pragma warning(disable : 4127) /* conditional expression is constant */

#pragma warning(push, 1)
#pragma warning(disable : 4548) /* expression before comma has no effect;      \
                                   expected expression with side - effect */
#pragma warning(disable : 4530) /* C++ exception handler used, but unwind      \
                                   semantics are not enabled. Specify /EHsc */
#pragma warning(disable : 4577) /* 'noexcept' used with no exception handling  \
                                   mode specified; termination on exception    \
                                   is not guaranteed. Specify /EHsc */
#endif                          /* _MSC_VER (warnings) */

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <malloc.h>
#include <stdlib.h>
#include <time.h>

__extern_C int_fast32_t mrand64(void);

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#else
static __inline int_fast32_t mrand48(void) { return mrand64(); }
#endif

#include <algorithm>
#include <cfloat> // for float limits
#include <cmath>  // for fabs()
#include <limits> // for numeric_limits<>

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

struct fpta_table_stored_schema {
  uint64_t checksum;
  uint32_t signature;
  uint32_t count;
  uint64_t csn;
  fpta_shove_t columns[fpta_max_cols];
};

static __inline size_t fpta_table_stored_schema_size(size_t cols) {
  assert(cols <= fpta_max_cols);
  return sizeof(fpta_table_stored_schema) -
         sizeof(fpta_shove_t) * (fpta_max_cols - cols);
}

struct fpta_table_schema {
  fpta_table_stored_schema _stored;
  fpta_shove_t _key;

  uint64_t checksum() const { return _stored.checksum; }
  uint32_t signature() const { return _stored.signature; }
  fpta_shove_t table_shove() const { return _key; }
  uint64_t version_csn() const { return _stored.csn; }
  size_t column_count() const { return _stored.count; }
  fpta_shove_t column_shove(size_t number) const {
    assert(number < _stored.count);
    return _stored.columns[number];
  }
  const fpta_shove_t *column_shoves_array() const { return _stored.columns; }
  fpta_shove_t table_pk() const { return column_shove(0); }

  unsigned _cache_hints[fpta_max_cols]; /* подсказки для кэша дескрипторов */
  unsigned &handle_cache(size_t number) {
    assert(number < _stored.count);
    return _cache_hints[number];
  }
  unsigned handle_cache(size_t number) const {
    assert(number < _stored.count);
    return _cache_hints[number];
  }

  typedef const uint8_t *composite_iter_t;
  int composite_list(size_t number, composite_iter_t &list_begin,
                     composite_iter_t &list_end) const {
    /* TODO */
    (void)number;
    list_begin = list_end = nullptr;
    return FPTA_ENOIMP;
  }
};

enum fpta_internals {
  /* используем некорретный для индекса набор флагов, чтобы в fpta_name
   * отличать таблицу от колонки, у таблицы в internal будет fpta_ftable. */
  fpta_flag_table = fpta_index_fsecondary,
  fpta_dbi_cache_size = fpta_tables_max * 5,
  FTPA_SCHEMA_SIGNATURE = 1636722823,
  FTPA_SCHEMA_CHECKSEED = 67413473,
  fpta_shoved_keylen = fpta_max_keylen + 8,
  fpta_notnil_prefix_byte = 42,
  fpta_notnil_prefix_length = 1
};

//----------------------------------------------------------------------------

struct fpta_txn {
  fpta_txn(const fpta_txn &) = delete;
  fpta_db *db;
  MDBX_txn *mdbx_txn;
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
    mdbx.iov_base = nullptr; /* hush coverity */
    mdbx.iov_len = ~0u;
  }
  fpta_key(const fpta_key &) = delete;

  MDBX_val mdbx;
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
    } longkey_obverse;
    struct {
      uint64_t headhash;
      uint64_t tail[fpta_max_keylen / sizeof(uint64_t)];
    } longkey_reverse;
  } place;
};

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4820) /* bytes padding added after data member       \
                                   for aligment */
#endif                          /* _MSC_VER (warnings) */

struct fpta_cursor {
  fpta_cursor(const fpta_cursor &) = delete;
  MDBX_cursor *mdbx_cursor;
  MDBX_val current;

#if __cplusplus < 201103L
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

  const fpta_filter *filter;
  fpta_txn *txn;
  fpta_db *db;

  fpta_name *table_id;
  unsigned column_number;
  fpta_cursor_options options;
  MDBX_dbi tbl_handle, idx_handle;

  fpta_table_schema *table_schema() const { return table_id->table_schema; }
  fpta_shove_t index_shove() const {
    return table_schema()->column_shove(column_number);
  }

  fpta_key range_from_key;
  fpta_key range_to_key;
};

#ifdef _MSC_VER
#pragma warning(pop)
#endif

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
  static_assert((int)fpta_primary_unique_ordered_obverse <
                    fpta_column_index_mask,
                "check fpta_column_index_mask");
  static_assert((int)fpta_primary_unique_ordered_obverse >
                    (1 << fpta_column_index_shift) - 1,
                "expect fpta_primary_unique_ordered_obverse is shifted");
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

MDBX_cmp_func *fpta_index_shove2comparator(fpta_shove_t shove);
unsigned fpta_index_shove2primary_dbiflags(fpta_shove_t shove);
unsigned fpta_index_shove2secondary_dbiflags(fpta_shove_t pk_shove,
                                             fpta_shove_t shove);

bool fpta_index_is_compat(fpta_shove_t shove, const fpta_value &value);

int fpta_index_value2key(fpta_shove_t shove, const fpta_value &value,
                         fpta_key &key, bool copy = false);
int fpta_index_key2value(fpta_shove_t shove, MDBX_val mdbx_key,
                         fpta_value &key_value);

int fpta_index_row2key(const fpta_table_schema *const schema, size_t column,
                       const fptu_ro &row, fpta_key &key, bool copy = false);

int fpta_composite_row2key(const fpta_table_schema *const schema, size_t column,
                           const fptu_ro &row, fpta_key &key);

int fpta_secondary_upsert(fpta_txn *txn, fpta_table_schema *table_def,
                          MDBX_val pk_key_old, const fptu_ro &row_old,
                          MDBX_val pk_key_new, const fptu_ro &row_new,
                          const unsigned stepover);

int fpta_secondary_check(fpta_txn *txn, fpta_table_schema *table_def,
                         const fptu_ro &row_old, const fptu_ro &row_new,
                         const unsigned stepover);

int fpta_secondary_remove(fpta_txn *txn, fpta_table_schema *table_def,
                          MDBX_val &pk_key, const fptu_ro &row_old,
                          const unsigned stepover);

int fpta_check_notindexed_cols(const fpta_table_schema *table_def,
                               const fptu_ro &row);

//----------------------------------------------------------------------------

int fpta_open_table(fpta_txn *txn, fpta_table_schema *table_def,
                    MDBX_dbi &handle);
int fpta_open_column(fpta_txn *txn, fpta_name *column_id, MDBX_dbi &tbl_handle,
                     MDBX_dbi &idx_handle);
int fpta_open_secondaries(fpta_txn *txn, fpta_table_schema *table_def,
                          MDBX_dbi *dbi_array);

//----------------------------------------------------------------------------

fpta_cursor *fpta_cursor_alloc(fpta_db *db);
void fpta_cursor_free(fpta_db *db, fpta_cursor *cursor);

//----------------------------------------------------------------------------

static __inline bool fpta_is_indexed(const fpta_shove_t index) {
  return (index & (fpta_column_index_mask - fpta_index_fnullable)) != 0;
}

static __inline bool fpta_index_is_unique(const fpta_shove_t index) {
  assert(fpta_is_indexed(index));
  return (index & fpta_index_funique) != 0;
}

static __inline bool fpta_index_is_ordered(const fpta_shove_t index) {
  assert(fpta_is_indexed(index));
  return (index & fpta_index_fordered) != 0;
}

static __inline bool fpta_index_is_unordered(const fpta_shove_t index) {
  return !fpta_index_is_ordered(index);
}

static __inline bool fpta_index_is_obverse(const fpta_shove_t index) {
  return (index & fpta_index_fobverse) != 0;
}

static __inline bool fpta_index_is_reverse(const fpta_shove_t index) {
  return (index & fpta_index_fobverse) == 0;
}

static __inline bool fpta_index_is_primary(const fpta_shove_t index) {
  assert(fpta_is_indexed(index));
  return (index & fpta_index_fsecondary) == 0;
}

static __inline bool fpta_index_is_secondary(const fpta_shove_t index) {
  return (index & fpta_index_fsecondary) != 0;
}

static __inline bool fpta_index_is_nullable(const fpta_index_type index) {
  assert(index == (index & fpta_column_index_mask));
  return index > fpta_index_fnullable;
}

static __inline bool fpta_column_is_nullable(const fpta_name *column_id) {
  return (column_id->shove & fpta_index_fnullable) != 0;
}

static __inline bool fpta_cursor_is_ordered(const fpta_cursor_options op) {
  return (op & (fpta_descending | fpta_ascending)) != fpta_unsorted;
}

static __inline bool fpta_cursor_is_descending(const fpta_cursor_options op) {
  return (op & (fpta_descending | fpta_ascending)) == fpta_descending;
}

static __inline bool fpta_cursor_is_ascending(const fpta_cursor_options op) {
  return (op & (fpta_descending | fpta_ascending)) == fpta_ascending;
}

int fpta_internal_abort(fpta_txn *txn, int errnum, bool txn_maybe_dead = false);

static __inline bool fpta_is_same(const MDBX_val &a, const MDBX_val &b) {
  return a.iov_len == b.iov_len &&
         memcmp(a.iov_base, b.iov_base, a.iov_len) == 0;
}

namespace std {
FPTA_API string to_string(const MDBX_val &);
FPTA_API string to_string(const fpta_key &);
}

template <typename type>
static __inline bool binary_eq(const type &a, const type &b) {
  return memcmp(&a, &b, sizeof(type)) == 0;
}

template <typename type>
static __inline bool binary_ne(const type &a, const type &b) {
  return memcmp(&a, &b, sizeof(type)) != 0;
}

//----------------------------------------------------------------------------

static __inline bool fpta_nullable_reverse_sensitive(const fptu_type type) {
  return type == fptu_uint16 || type == fptu_uint32 || type == fptu_uint64 ||
         (type >= fptu_96 && type <= fptu_256);
}

typedef union {
  uint32_t __i;
  float __f;
} fpta_fp32_t;

#define FPTA_DENIL_FP32_BIN UINT32_C(0xFFFFffff)
FPTA_API extern const fpta_fp32_t fpta_fp32_denil;

#define FPTA_QSNAN_FP32_BIN UINT32_C(0xFFFFfffE)
FPTA_API extern const fpta_fp32_t fpta_fp32_qsnan;

#define FPTA_DENIL_FP32x64_BIN UINT64_C(0xFFFFffffE0000000)
FPTA_API extern const fpta_fp64_t fpta_fp32x64_denil;

#define FPTA_QSNAN_FP32x64_BIN UINT64_C(0xFFFFffffC0000000)
FPTA_API extern const fpta_fp64_t fpta_fp32x64_qsnan;

#ifndef _MSC_VER /* MSVC provides invalid nan() */
#define FPTA_DENIL_FP32_MAS "8388607"
#define FPTA_QSNAN_FP32_MAS "8388606"
#define FPTA_DENIL_FP32x64_MAS "4503599090499584"
#define FPTA_QSNAN_FP32x64_MAS "4503598553628672"
#endif /* ! _MSC_VER */

#if __GNUC_PREREQ(3, 3) || __CLANG_PREREQ(3, 6)
#define FPTA_DENIL_FP32 (-__builtin_nanf(FPTA_DENIL_FP32_MAS))
#define FPTA_QSNAN_FP32 (-__builtin_nanf(FPTA_QSNAN_FP32_MAS))
#else
#define FPTA_DENIL_FP32 (fpta_fp32_denil.__f)
#define FPTA_QSNAN_FP32 (fpta_fp32_qsnan.__f)
#endif
#define FPTA_DENIL_FP64 FPTA_DENIL_FP

template <fptu_type type>
static __inline bool is_fixbin_denil(const fpta_index_type index,
                                     const void *fixbin) {
  assert(fpta_index_is_nullable(index));
  const uint64_t denil = fpta_index_is_obverse(index)
                             ? FPTA_DENIL_FIXBIN_OBVERSE |
                                   (uint64_t)FPTA_DENIL_FIXBIN_OBVERSE << 8 |
                                   (uint64_t)FPTA_DENIL_FIXBIN_OBVERSE << 16 |
                                   (uint64_t)FPTA_DENIL_FIXBIN_OBVERSE << 24 |
                                   (uint64_t)FPTA_DENIL_FIXBIN_OBVERSE << 32 |
                                   (uint64_t)FPTA_DENIL_FIXBIN_OBVERSE << 40 |
                                   (uint64_t)FPTA_DENIL_FIXBIN_OBVERSE << 48 |
                                   (uint64_t)FPTA_DENIL_FIXBIN_OBVERSE << 56
                             : FPTA_DENIL_FIXBIN_REVERSE |
                                   (uint64_t)FPTA_DENIL_FIXBIN_REVERSE << 8 |
                                   (uint64_t)FPTA_DENIL_FIXBIN_REVERSE << 16 |
                                   (uint64_t)FPTA_DENIL_FIXBIN_REVERSE << 24 |
                                   (uint64_t)FPTA_DENIL_FIXBIN_REVERSE << 32 |
                                   (uint64_t)FPTA_DENIL_FIXBIN_REVERSE << 40 |
                                   (uint64_t)FPTA_DENIL_FIXBIN_REVERSE << 48 |
                                   (uint64_t)FPTA_DENIL_FIXBIN_REVERSE << 56;

  /* FIXME: unaligned access */
  const uint64_t *by64 = (const uint64_t *)fixbin;
  const uint32_t *by32 = (const uint32_t *)fixbin;

  switch (type) {
  case fptu_96:
    return by64[0] == denil && by32[2] == (uint32_t)denil;
  case fptu_128:
    return by64[0] == denil && by64[1] == denil;
  case fptu_160:
    return by64[0] == denil && by64[1] == denil && by32[4] == (uint32_t)denil;
  case fptu_256:
    return by64[0] == denil && by64[1] == denil && by64[2] == denil &&
           by64[3] == denil;
  default:
    assert(false && "unexpected column type");
    __unreachable();
    return true;
  }
}

static __inline bool check_fixbin_not_denil(const fpta_index_type index,
                                            const fptu_payload *payload,
                                            const size_t bytes) {
  assert(fpta_index_is_nullable(index));
  for (size_t i = 0; i < bytes; i++)
    if (payload->fixbin[i] != (fpta_index_is_obverse(index)
                                   ? FPTA_DENIL_FIXBIN_OBVERSE
                                   : FPTA_DENIL_FIXBIN_REVERSE))
      return true;
  return false;
}

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
#include "fast_positive/tables_internal.h"
#include "osal.h"

#include <atomic>
#include <functional>

struct fpta_db {
  fpta_db(const fpta_db &) = delete;
  fpta_rwl_t schema_rwlock;
  MDBX_env *mdbx_env;
  MDBX_dbi schema_dbi;
  bool alterable_schema;
  char unused_gap[3];

  fpta_mutex_t dbi_mutex /* TODO: убрать мьютекс и перевести на atomic */;
  fpta_shove_t dbi_shoves[fpta_dbi_cache_size];
  MDBX_dbi dbi_handles[fpta_dbi_cache_size];
};

bool fpta_filter_validate(const fpta_filter *filter);
int fpta_name_refresh_filter(fpta_txn *txn, fpta_name *table_id,
                             fpta_filter *filter);
bool fpta_schema_validate(const MDBX_val def);

static __inline bool fpta_table_has_secondary(const fpta_name *table_id) {
  return table_id->table.def->count > 1 &&
         fpta_index_is_secondary(table_id->table.def->columns[1]);
}

static __inline bool fpta_db_validate(fpta_db *db) {
  if (unlikely(db == nullptr || db->mdbx_env == nullptr))
    return false;

  // TODO
  return true;
}

static __inline int fpta_txn_validate(fpta_txn *txn, fpta_level min_level) {
  if (unlikely(txn == nullptr || !fpta_db_validate(txn->db)))
    return FPTA_EINVAL;
  if (unlikely(txn->level < min_level || txn->level > fpta_schema))
    return FPTA_EINVAL;

  if (unlikely(txn->mdbx_txn == nullptr))
    return FPTA_TXN_CANCELLED;

  return FPTA_OK;
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

static __inline int fpta_cursor_validate(const fpta_cursor *cursor,
                                         fpta_level min_level) {
  if (unlikely(cursor == nullptr || cursor->mdbx_cursor == nullptr))
    return FPTA_EINVAL;

  return fpta_txn_validate(cursor->txn, min_level);
}

//----------------------------------------------------------------------------

template <fptu_type type> struct numeric_traits;

template <> struct numeric_traits<fptu_uint16> {
  typedef uint16_t native;
  typedef uint_fast16_t fast;
  enum { has_native_saturation = false };
  typedef std::numeric_limits<native> native_limits;
  static fast denil(const fpta_index_type index) {
    assert(fpta_index_is_nullable(index));
    return fpta_index_is_obverse(index) ? FPTA_DENIL_UINT16_OBVERSE
                                        : FPTA_DENIL_UINT16_REVERSE;
  }
  static fpta_value_type value_type() { return fpta_unsigned_int; }
  static fpta_value make_value(const fast value) {
    return fpta_value_uint(value);
  }
};

template <> struct numeric_traits<fptu_uint32> {
  typedef uint32_t native;
  typedef uint_fast32_t fast;
  enum { has_native_saturation = false };
  typedef std::numeric_limits<native> native_limits;
  static fast denil(const fpta_index_type index) {
    assert(fpta_index_is_nullable(index));
    return fpta_index_is_obverse(index) ? FPTA_DENIL_UINT32_OBVERSE
                                        : FPTA_DENIL_UINT32_REVERSE;
  }
  static fpta_value_type value_type() { return fpta_unsigned_int; }
  static fpta_value make_value(const fast value) {
    return fpta_value_uint(value);
  }
};

template <> struct numeric_traits<fptu_uint64> {
  typedef uint64_t native;
  typedef uint_fast64_t fast;
  enum { has_native_saturation = false };
  typedef std::numeric_limits<native> native_limits;
  static fast denil(const fpta_index_type index) {
    assert(fpta_index_is_nullable(index));
    return fpta_index_is_obverse(index) ? FPTA_DENIL_UINT64_OBVERSE
                                        : FPTA_DENIL_UINT64_REVERSE;
  }
  static fpta_value_type value_type() { return fpta_unsigned_int; }
  static fpta_value make_value(const fast value) {
    return fpta_value_uint(value);
  }
};

template <> struct numeric_traits<fptu_int32> {
  typedef int32_t native;
  typedef int_fast32_t fast;
  enum { has_native_saturation = false };
  typedef std::numeric_limits<native> native_limits;
  static fast denil(const fpta_index_type index) {
    assert(fpta_index_is_nullable(index));
    (void)index;
    return FPTA_DENIL_SINT32;
  }
  static fpta_value_type value_type() { return fpta_signed_int; }
  static fpta_value make_value(const fast value) {
    return fpta_value_sint(value);
  }
};

template <> struct numeric_traits<fptu_int64> {
  typedef int64_t native;
  typedef int_fast64_t fast;
  enum { has_native_saturation = false };
  typedef std::numeric_limits<native> native_limits;
  static fast denil(const fpta_index_type index) {
    assert(fpta_index_is_nullable(index));
    (void)index;
    return FPTA_DENIL_SINT64;
  }
  static fpta_value_type value_type() { return fpta_signed_int; }
  static fpta_value make_value(const fast value) {
    return fpta_value_sint(value);
  }
};

template <> struct numeric_traits<fptu_fp32> {
  typedef float native;
  typedef float_t fast;
  enum { has_native_saturation = true };
  typedef std::numeric_limits<native> native_limits;
  static fast denil(const fpta_index_type index) {
    assert(fpta_index_is_nullable(index));
    (void)index;
    return FPTA_DENIL_FP32;
  }
  static fpta_value_type value_type() { return fpta_float_point; }
  static fpta_value make_value(const fast value) {
    return fpta_value_float(value);
  }
};

template <> struct numeric_traits<fptu_fp64> {
  typedef double native;
  typedef double_t fast;
  enum { has_native_saturation = true };
  typedef std::numeric_limits<native> native_limits;
  static fast denil(const fpta_index_type index) {
    assert(fpta_index_is_nullable(index));
    (void)index;
    return FPTA_DENIL_FP64;
  }
  static fpta_value_type value_type() { return fpta_float_point; }
  static fpta_value make_value(const fast value) {
    return fpta_value_float(value);
  }
};

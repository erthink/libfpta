/*
 * Copyright 2016-2018 libfpta authors: please see AUTHORS file.
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

#include <algorithm>
#include <atomic>
#include <functional>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4820) /* bytes padding added after data member       \
                                   for aligment */
#endif                          /* _MSC_VER (warnings) */

struct fpta_db {
  fpta_db(const fpta_db &) = delete;
  MDBX_env *mdbx_env;
  bool alterable_schema;
  MDBX_dbi schema_dbi;
  fpta_rwl_t schema_rwlock;

  fpta_mutex_t dbi_mutex /* TODO: убрать мьютекс и перевести на atomic */;
  fpta_shove_t dbi_shoves[fpta_dbi_cache_size];
  uint64_t dbi_csns[fpta_dbi_cache_size];
  MDBX_dbi dbi_handles[fpta_dbi_cache_size];
};

#ifdef _MSC_VER
#pragma warning(pop)
#endif

enum fpta_schema_item {
  fpta_table,
  fpta_column,
  fpta_table_with_schema,
  fpta_column_with_schema
};

//----------------------------------------------------------------------------

bool fpta_filter_validate(const fpta_filter *filter);

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
    return FPTA_EPERM;

  if (unlikely(txn->mdbx_txn == nullptr))
    return FPTA_TXN_CANCELLED;

  return FPTA_OK;
}

static __inline int fpta_id_validate(const fpta_name *id,
                                     fpta_schema_item schema_item) {
  if (unlikely(id == nullptr))
    return FPTA_EINVAL;

  switch (schema_item) {
  default:
    return FPTA_EOOPS;

  case fpta_table:
  case fpta_table_with_schema:
    if (unlikely(fpta_shove2index(id->shove) !=
                 (fpta_index_type)fpta_flag_table))
      return FPTA_EINVAL;

    if (schema_item > fpta_table) {
      const fpta_table_schema *table_schema = id->table_schema;
      if (unlikely(table_schema == nullptr))
        return FPTA_EINVAL;
      if (unlikely(table_schema->signature() != FTPA_SCHEMA_SIGNATURE))
        return FPTA_SCHEMA_CORRUPTED;
      if (unlikely(table_schema->table_shove() != id->shove))
        return FPTA_SCHEMA_CORRUPTED;
      assert(id->version >= table_schema->version_csn());
    }
    return FPTA_SUCCESS;

  case fpta_column:
  case fpta_column_with_schema:
    if (unlikely(fpta_shove2index(id->shove) ==
                 (fpta_index_type)fpta_flag_table))
      return FPTA_EINVAL;

    if (schema_item > fpta_column) {
      if (unlikely(id->column.num > fpta_max_cols))
        return FPTA_EINVAL;
      int rc = fpta_id_validate(id->column.table, fpta_table_with_schema);
      if (unlikely(rc != FPTA_SUCCESS))
        return rc;
      const fpta_table_schema *table_schema = id->column.table->table_schema;
      if (unlikely(id->column.num > table_schema->column_count()))
        return FPTA_SCHEMA_CORRUPTED;
      if (unlikely(table_schema->column_shove(id->column.num) != id->shove))
        return FPTA_SCHEMA_CORRUPTED;
    }
    return FPTA_SUCCESS;
  }
}

static __inline int fpta_cursor_validate(const fpta_cursor *cursor,
                                         fpta_level min_level) {
  if (unlikely(cursor == nullptr || cursor->mdbx_cursor == nullptr))
    return FPTA_EINVAL;

  return fpta_txn_validate(cursor->txn, min_level);
}

//----------------------------------------------------------------------------

fpta_shove_t fpta_shove_name(const char *name, enum fpta_schema_item type);

static __inline fpta_shove_t fpta_dbi_shove(const fpta_shove_t table_shove,
                                            const size_t index_id) {
  assert(table_shove > fpta_flag_table);
  assert(index_id < fpta_max_indexes);

  fpta_shove_t dbi_shove = table_shove - fpta_flag_table;
  assert(0 == (dbi_shove & (fpta_column_typeid_mask | fpta_column_index_mask)));
  dbi_shove += index_id;

  assert(fpta_shove_eq(table_shove, dbi_shove));
  return dbi_shove;
}

static __inline unsigned fpta_dbi_flags(const fpta_shove_t *shoves_defs,
                                        const size_t n) {
  const unsigned dbi_flags =
      (n == 0)
          ? fpta_index_shove2primary_dbiflags(shoves_defs[0])
          : fpta_index_shove2secondary_dbiflags(shoves_defs[0], shoves_defs[n]);
  return dbi_flags;
}

static __inline fpta_shove_t fpta_data_shove(const fpta_shove_t *shoves_defs,
                                             const size_t n) {
  const fpta_shove_t data_shove =
      n ? shoves_defs[0]
        : fpta_column_shove(0, fptu_nested,
                            fpta_primary_unique_ordered_obverse);
  return data_shove;
}

int fpta_dbi_close(fpta_txn *txn, const fpta_shove_t shove,
                   unsigned *cache_hint);
int fpta_dbi_open(fpta_txn *txn, const fpta_shove_t shove, MDBX_dbi &handle,
                  const unsigned dbi_flags, const fpta_shove_t key_shove,
                  const fpta_shove_t data_shove, unsigned *const cache_hint,
                  const uint64_t csn);

MDBX_dbi fpta_dbicache_remove(fpta_db *db, const fpta_shove_t shove,
                              unsigned *const cache_hint = nullptr);
int fpta_dbicache_flush(fpta_txn *txn);

//----------------------------------------------------------------------------

template <fptu_type type> struct numeric_traits;

template <> struct numeric_traits<fptu_uint16> {
  typedef uint16_t native;
  typedef uint_fast16_t fast;
  enum { has_native_saturation = false };
  typedef std::numeric_limits<native> native_limits;
  static fast denil(const fpta_shove_t shove) {
    assert(fpta_column_is_nullable(shove));
    return fpta_index_is_obverse(shove) ? (fast)FPTA_DENIL_UINT16_OBVERSE
                                        : (fast)FPTA_DENIL_UINT16_REVERSE;
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
  static fast denil(const fpta_shove_t shove) {
    assert(fpta_column_is_nullable(shove));
    return fpta_index_is_obverse(shove) ? FPTA_DENIL_UINT32_OBVERSE
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
  static fast denil(const fpta_shove_t shove) {
    assert(fpta_column_is_nullable(shove));
    return fpta_index_is_obverse(shove) ? FPTA_DENIL_UINT64_OBVERSE
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
  static fast denil(const fpta_shove_t shove) {
    assert(fpta_column_is_nullable(shove));
    (void)shove;
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
  static fast denil(const fpta_shove_t shove) {
    assert(fpta_column_is_nullable(shove));
    (void)shove;
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
  static fast denil(const fpta_shove_t shove) {
    assert(fpta_column_is_nullable(shove));
    (void)shove;
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
  static fast denil(const fpta_shove_t shove) {
    assert(fpta_column_is_nullable(shove));
    (void)shove;
    return FPTA_DENIL_FP64;
  }
  static fpta_value_type value_type() { return fpta_float_point; }
  static fpta_value make_value(const fast value) {
    return fpta_value_float(value);
  }
};

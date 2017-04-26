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

struct fpta_db {
  fpta_db(const fpta_db &) = delete;
  fpta_rwl_t schema_rwlock;
  MDB_env *mdbx_env;
  MDB_dbi schema_dbi;
  bool alterable_schema;
  char unused_gap[3];

  fpta_mutex_t dbi_mutex;
  fpta_shove_t dbi_shoves[fpta_dbi_cache_size];
  MDB_dbi dbi_handles[fpta_dbi_cache_size];
};

bool fpta_filter_validate(const fpta_filter *filter);
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

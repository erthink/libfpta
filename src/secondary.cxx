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

#include "fast_positive/tables_internal.h"

static __hot bool fpta_is_column_changed(const fptu_ro &row_old,
                                         const fptu_ro &row_new,
                                         unsigned column) {
  auto filed_old = fptu_lookup_ro(row_old, column, fptu_any);
  auto filed_new = fptu_lookup_ro(row_new, column, fptu_any);
  return fptu_cmp_fields(filed_old, filed_new) != fptu_eq;
}

static __inline bool fpta_fk_changed(const fpta_table_schema *def,
                                     const fptu_ro &row_old,
                                     const fptu_ro &row_new,
                                     unsigned column) {
  assert(column > 0 && column < def->count);
  assert(fpta_shove2index(def->columns[column]) != fpta_index_none);
  assert(fpta_index_is_secondary(fpta_shove2index(def->columns[column])));
  return fpta_is_column_changed(row_old, row_new, 0);
}

int fpta_check_constraints(fpta_txn *txn, fpta_name *table_id,
                           MDB_val &pk_key_old, const fptu_ro &row_old,
                           MDB_val &pk_key_new, const fptu_ro &row_new,
                           unsigned stepover) {
  (void)txn;
  (void)table_id;
  (void)pk_key_old;
  (void)row_old;
  (void)pk_key_new;
  (void)row_new;
  (void)stepover;
  // TODO: проверка конфликтов для индексов с контролем уникальности
  return FPTA_SUCCESS;
}

int fpta_secondary_upsert(fpta_txn *txn, fpta_name *table_id,
                          MDB_val &pk_key_old, const fptu_ro &row_old,
                          MDB_val &pk_key_new, const fptu_ro &row_new,
                          unsigned stepover) {
  MDB_dbi dbi[fpta_max_indexes];
  int rc = fpta_open_secondaries(txn, table_id, dbi);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  for (size_t i = 1; i < table_id->table.def->count; ++i) {
    if (fpta_shove2index(table_id->table.def->columns[i]) == fpta_index_none)
      break;
    assert(i < fpta_max_indexes);
    if (i == stepover)
      continue;

    if (row_old.sys.iov_base) {
      const bool fk_changed =
          fpta_fk_changed(table_id->table.def, row_old, row_new, i);
      if (!fk_changed)
        continue;

      fpta_key fk_key_old;
      rc = fpta_index_row2key(table_id->table.def->columns[i], i, row_old,
                              fk_key_old, false);
      if (unlikely(rc != MDB_SUCCESS))
        return rc;

      rc = mdbx_del(txn->mdbx_txn, dbi[i], &fk_key_old.mdbx, &pk_key_old);
      if (unlikely(rc != MDB_SUCCESS))
        return (rc != MDB_NOTFOUND) ? rc : (int)FPTA_INDEX_CORRUPTED;
    }

    fpta_key fk_key_new;
    rc = fpta_index_row2key(table_id->table.def->columns[i], i, row_new,
                            fk_key_new, false);
    if (unlikely(rc != MDB_SUCCESS))
      return rc;
    rc = mdbx_put(txn->mdbx_txn, dbi[i], &fk_key_new.mdbx, &pk_key_new,
                  MDB_NODUPDATA);
    if (unlikely(rc != MDB_SUCCESS))
      return (rc != MDB_KEYEXIST) ? rc : (int)FPTA_INDEX_CORRUPTED;
  }

  return FPTA_SUCCESS;
}

int fpta_secondary_remove(fpta_txn *txn, fpta_name *table_id, MDB_val &pk_key,
                          const fptu_ro &row_old, unsigned stepover) {
  MDB_dbi dbi[fpta_max_indexes];
  int rc = fpta_open_secondaries(txn, table_id, dbi);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  for (size_t i = 1; i < table_id->table.def->count; ++i) {
    if (fpta_shove2index(table_id->table.def->columns[i]) == fpta_index_none)
      break;
    assert(i < fpta_max_indexes);
    if (i == stepover)
      continue;

    fpta_key fk_key_old;
    rc = fpta_index_row2key(table_id->table.def->columns[i], i, row_old,
                            fk_key_old, false);
    if (unlikely(rc != MDB_SUCCESS))
      return rc;

    rc = mdbx_del(txn->mdbx_txn, dbi[i], &fk_key_old.mdbx, &pk_key);
    if (unlikely(rc != MDB_SUCCESS))
      return (rc != MDB_NOTFOUND) ? rc : (int)FPTA_INDEX_CORRUPTED;
  }

  return FPTA_SUCCESS;
}

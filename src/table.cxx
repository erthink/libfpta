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

#include "details.h"

int fpta_table_info(fpta_txn *txn, fpta_name *table_id, size_t *row_count,
                    fpta_table_stat *stat) {
  int rc = fpta_name_refresh_couple(txn, table_id, nullptr);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  MDBX_dbi handle;
  rc = fpta_open_table(txn, table_id, handle);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  MDBX_stat mdbx_stat;
  rc = mdbx_dbi_stat(txn->mdbx_txn, handle, &mdbx_stat, sizeof(mdbx_stat));
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(stat)) {
    stat->row_count = mdbx_stat.ms_entries;
    stat->btree_depth = mdbx_stat.ms_depth;
    stat->leaf_pages = (size_t)mdbx_stat.ms_leaf_pages;
    stat->branch_pages = (size_t)mdbx_stat.ms_branch_pages;
    stat->large_pages = (size_t)mdbx_stat.ms_overflow_pages;
    stat->total_bytes = (mdbx_stat.ms_leaf_pages + mdbx_stat.ms_branch_pages +
                         mdbx_stat.ms_overflow_pages) *
                        (size_t)mdbx_stat.ms_psize;
  }

  if (likely(row_count)) {
    if (unlikely(mdbx_stat.ms_entries > SIZE_MAX)) {
      *row_count = (size_t)FPTA_DEADBEEF;
      return FPTA_EVALUE;
    }
    *row_count = (size_t)mdbx_stat.ms_entries;
  }

  return FPTA_SUCCESS;
}

int fpta_table_sequence(fpta_txn *txn, fpta_name *table_id, uint64_t *result,
                        uint64_t increment) {
  int rc = fpta_name_refresh_couple(txn, table_id, nullptr);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  MDBX_dbi handle;
  rc = fpta_open_table(txn, table_id, handle);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  rc = mdbx_dbi_sequence(txn->mdbx_txn, handle, result, increment);
  static_assert(FPTA_NODATA == MDBX_RESULT_TRUE, "expect equal");
  return rc;
}

int fpta_table_clear(fpta_txn *txn, fpta_name *table_id, bool reset_sequence) {
  int rc = fpta_name_refresh_couple(txn, table_id, nullptr);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  MDBX_dbi handle;
  rc = fpta_open_table(txn, table_id, handle);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  MDBX_dbi dbi[fpta_max_indexes];
  if (fpta_table_has_secondary(table_id)) {
    rc = fpta_open_secondaries(txn, table_id, dbi);
    if (unlikely(rc != FPTA_SUCCESS))
      return rc;
  }

  uint64_t sequence = 0;
  if (!reset_sequence) {
    rc = mdbx_dbi_sequence(txn->mdbx_txn, handle, &sequence, 0);
    if (unlikely(rc != FPTA_SUCCESS))
      return rc;
  }

  rc = mdbx_drop(txn->mdbx_txn, handle, 0);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (fpta_table_has_secondary(table_id)) {
    for (size_t i = 1; i < table_id->table.def->count; ++i) {
      rc = mdbx_drop(txn->mdbx_txn, dbi[i], 0);
      if (unlikely(rc != MDBX_SUCCESS))
        return fpta_internal_abort(txn, rc);
    }
  }

  if (sequence) {
    rc = mdbx_dbi_sequence(txn->mdbx_txn, handle, nullptr, sequence);
    if (unlikely(rc != FPTA_SUCCESS))
      return fpta_internal_abort(txn, rc);
  }

  return FPTA_SUCCESS;
}

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

int fpta_check_notindexed_cols(const fpta_table_schema *table_def,
                               const fptu_ro &row) {
  assert(table_def->column_count() > 0);
  for (size_t i = table_def->column_count(); --i > 0;) {
    const auto shove = table_def->column_shove(i);
    const auto index = fpta_shove2index(shove);
    assert(i < fpta_max_indexes);
    if (index > fpta_index_none) {
      assert(fpta_index_is_secondary(index) || (index & fpta_index_fnullable));
#ifdef NDEBUG
      break;
#endif
    } else {
      const fptu_type type = fpta_shove2type(shove);
      const fptu_field *field = fptu_lookup_ro(row, (unsigned)i, type);
      if (unlikely(field == nullptr))
        return FPTA_COLUMN_MISSING;
    }
  }
  return FPTA_SUCCESS;
}

int fpta_secondary_check(fpta_txn *txn, fpta_table_schema *table_def,
                         const fptu_ro &row_old, const fptu_ro &row_new,
                         const unsigned stepover) {
  MDBX_dbi dbi[fpta_max_indexes];
  int rc = fpta_open_secondaries(txn, table_def, dbi);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  for (size_t i = 1; i < table_def->column_count(); ++i) {
    const auto shove = table_def->column_shove(i);
    const auto index = fpta_shove2index(shove);
    assert(i < fpta_max_indexes);
    if (!fpta_index_is_secondary(index))
      break;
    if (i == stepover || !fpta_index_is_unique(index))
      continue;

    fpta_key fk_key_new;
    rc = fpta_index_row2key(table_def, i, row_new, fk_key_new, false);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;

    if (row_old.sys.iov_base) {
      fpta_key fk_key_old;
      rc = fpta_index_row2key(table_def, i, row_old, fk_key_old, false);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
      if (fpta_is_same(fk_key_old.mdbx, fk_key_new.mdbx))
        continue;
    }

    MDBX_val pk_exist;
    rc = mdbx_get(txn->mdbx_txn, dbi[i], &fk_key_new.mdbx, &pk_exist);
    if (unlikely(rc != MDBX_NOTFOUND))
      return (rc == MDBX_SUCCESS) ? MDBX_KEYEXIST : rc;
  }

  return FPTA_SUCCESS;
}

int fpta_secondary_upsert(fpta_txn *txn, fpta_table_schema *table_def,
                          MDBX_val pk_key_old, const fptu_ro &row_old,
                          MDBX_val pk_key_new, const fptu_ro &row_new,
                          const unsigned stepover) {
  MDBX_dbi dbi[fpta_max_indexes];
  int rc = fpta_open_secondaries(txn, table_def, dbi);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  for (size_t i = 1; i < table_def->column_count(); ++i) {
    const auto shove = table_def->column_shove(i);
    const auto index = fpta_shove2index(shove);
    assert(i < fpta_max_indexes);
    if (!fpta_index_is_secondary(index))
      break;
    if (i == stepover)
      continue;

    fpta_key fk_key_new;
    rc = fpta_index_row2key(table_def, i, row_new, fk_key_new, false);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;

    if (row_old.sys.iov_base == nullptr) {
      /* Старой версии нет, выполняется добавление новой строки */
      assert(pk_key_old.iov_base == pk_key_new.iov_base);
      /* Вставляем новую пару в secondary индекс */
      rc = mdbx_put(txn->mdbx_txn, dbi[i], &fk_key_new.mdbx, &pk_key_new,
                    fpta_index_is_unique(index)
                        ? MDBX_NODUPDATA | MDBX_NOOVERWRITE
                        : MDBX_NODUPDATA);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;

      continue;
    }
    /* else: Выполняется обновление существующей строки */

    fpta_key fk_key_old;
    rc = fpta_index_row2key(table_def, i, row_old, fk_key_old, false);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;

    if (!fpta_is_same(fk_key_old.mdbx, fk_key_new.mdbx)) {
      /* Изменилось значение индексированного поля, выполняем удаление
       * из индекса пары со старым значением и добавляем пару с новым. */
      rc = mdbx_del(txn->mdbx_txn, dbi[i], &fk_key_old.mdbx, &pk_key_old);
      if (unlikely(rc != MDBX_SUCCESS))
        return (rc != MDBX_NOTFOUND) ? rc : (int)FPTA_INDEX_CORRUPTED;
      rc = mdbx_put(txn->mdbx_txn, dbi[i], &fk_key_new.mdbx, &pk_key_new,
                    fpta_index_is_unique(index)
                        ? MDBX_NODUPDATA | MDBX_NOOVERWRITE
                        : MDBX_NODUPDATA);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
      continue;
    }

    if (pk_key_old.iov_base == pk_key_new.iov_base ||
        fpta_is_same(pk_key_old, pk_key_new))
      continue;

    /* Изменился PK, необходимо обновить пару<SE_value, PK_value> во вторичном
     * индексе. Комбинация MDBX_CURRENT | MDBX_NOOVERWRITE для таблиц с
     * MDBX_DUPSORT включает в mdbx_replace() режим обновления конкретного
     * значения из multivalue. Таким образом, мы меняем ссылку именно со
     * старого значения PK на новое, даже если для индексируемого поля
     * разрешены не уникальные значения. */
    rc = mdbx_replace(txn->mdbx_txn, dbi[i], &fk_key_new.mdbx, &pk_key_new,
                      &pk_key_old,
                      fpta_index_is_unique(index)
                          ? MDBX_CURRENT | MDBX_NODUPDATA
                          : MDBX_CURRENT | MDBX_NODUPDATA | MDBX_NOOVERWRITE);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }

  return FPTA_SUCCESS;
}

int fpta_secondary_remove(fpta_txn *txn, fpta_table_schema *table_def,
                          MDBX_val &pk_key, const fptu_ro &row_old,
                          const unsigned stepover) {
  MDBX_dbi dbi[fpta_max_indexes];
  int rc = fpta_open_secondaries(txn, table_def, dbi);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  for (size_t i = 1; i < table_def->column_count(); ++i) {
    const auto shove = table_def->column_shove(i);
    const auto index = fpta_shove2index(shove);
    assert(i < fpta_max_indexes);
    if (!fpta_index_is_secondary(index))
      break;
    if (i == stepover)
      continue;

    fpta_key fk_key_old;
    rc = fpta_index_row2key(table_def, i, row_old, fk_key_old, false);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;

    rc = mdbx_del(txn->mdbx_txn, dbi[i], &fk_key_old.mdbx, &pk_key);
    if (unlikely(rc != MDBX_SUCCESS))
      return (rc != MDBX_NOTFOUND) ? rc : (int)FPTA_INDEX_CORRUPTED;
  }

  return FPTA_SUCCESS;
}

//----------------------------------------------------------------------------

int fpta_table_info(fpta_txn *txn, fpta_name *table_id, size_t *row_count,
                    fpta_table_stat *stat) {
  int rc = fpta_name_refresh_couple(txn, table_id, nullptr);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  MDBX_dbi handle;
  rc = fpta_open_table(txn, table_id->table_schema, handle);
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
  rc = fpta_open_table(txn, table_id->table_schema, handle);
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

  fpta_table_schema *table_def = table_id->table_schema;
  MDBX_dbi handle;
  rc = fpta_open_table(txn, table_def, handle);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  MDBX_dbi dbi[fpta_max_indexes];
  if (table_def->has_secondary()) {
    rc = fpta_open_secondaries(txn, table_def, dbi);
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

  if (table_def->has_secondary()) {
    for (size_t i = 1; i < table_def->column_count(); ++i) {
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

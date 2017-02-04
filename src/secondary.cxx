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

int fpta_check_constraints(fpta_txn *txn, fpta_name *table_id,
                           const fptu_ro &row_old, const fptu_ro &row_new,
                           unsigned stepover) {
  MDB_dbi dbi[fpta_max_indexes];
  int rc = fpta_open_secondaries(txn, table_id, dbi);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  for (size_t i = 1; i < table_id->table.def->count; ++i) {
    const auto shove = table_id->table.def->columns[i];
    const auto index = fpta_shove2index(shove);
    if (index == fpta_index_none)
      break;
    assert(i < fpta_max_indexes);
    if (i == stepover || !fpta_index_is_unique(index))
      continue;

    fpta_key fk_key_new;
    rc = fpta_index_row2key(shove, i, row_new, fk_key_new, false);
    if (unlikely(rc != MDB_SUCCESS))
      return rc;

    if (row_old.sys.iov_base) {
      fpta_key fk_key_old;
      rc = fpta_index_row2key(shove, i, row_old, fk_key_old, false);
      if (unlikely(rc != MDB_SUCCESS))
        return rc;
      if (fpta_is_same(fk_key_old.mdbx, fk_key_new.mdbx))
        continue;
    }

    MDB_val pk_exist;
    rc = mdbx_get(txn->mdbx_txn, dbi[i], &fk_key_new.mdbx, &pk_exist);
    if (unlikely(rc != MDB_NOTFOUND))
      return (rc == MDB_SUCCESS) ? MDB_KEYEXIST : rc;
  }

  return FPTA_SUCCESS;
}

int fpta_secondary_upsert(fpta_txn *txn, fpta_name *table_id,
                          MDB_val pk_key_old, const fptu_ro &row_old,
                          MDB_val pk_key_new, const fptu_ro &row_new,
                          unsigned stepover) {
  MDB_dbi dbi[fpta_max_indexes];
  int rc = fpta_open_secondaries(txn, table_id, dbi);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  for (size_t i = 1; i < table_id->table.def->count; ++i) {
    const auto shove = table_id->table.def->columns[i];
    const auto index = fpta_shove2index(shove);
    if (index == fpta_index_none)
      break;
    assert(i < fpta_max_indexes);
    if (i == stepover)
      continue;

    fpta_key fk_key_new;
    rc = fpta_index_row2key(shove, i, row_new, fk_key_new, false);
    if (unlikely(rc != MDB_SUCCESS))
      return rc;

    if (row_old.sys.iov_base == nullptr) {
      /* Старой версии нет, выполняется добавление новой строки */
      assert(pk_key_old.iov_base == pk_key_new.iov_base);
      /* Вставляем новую пару в secondary индекс */
      rc = mdbx_put(txn->mdbx_txn, dbi[i], &fk_key_new.mdbx, &pk_key_new,
                    fpta_index_is_unique(index)
                        ? MDB_NODUPDATA | MDB_NOOVERWRITE
                        : MDB_NODUPDATA);
      if (unlikely(rc != MDB_SUCCESS))
        return rc;

      continue;
    }
    /* else: Выполняется обновление существующей строки */

    fpta_key fk_key_old;
    rc = fpta_index_row2key(shove, i, row_old, fk_key_old, false);
    if (unlikely(rc != MDB_SUCCESS))
      return rc;

    if (!fpta_is_same(fk_key_old.mdbx, fk_key_new.mdbx)) {
      /* Изменилось значение индексированного поля, выполняем удаление
       * из индекса пары со старым значением и добавляем пару с новым. */
      rc = mdbx_del(txn->mdbx_txn, dbi[i], &fk_key_old.mdbx, &pk_key_old);
      if (unlikely(rc != MDB_SUCCESS))
        return (rc != MDB_NOTFOUND) ? rc : (int)FPTA_INDEX_CORRUPTED;
      rc = mdbx_put(txn->mdbx_txn, dbi[i], &fk_key_new.mdbx, &pk_key_new,
                    MDB_NODUPDATA);
      if (unlikely(rc != MDB_SUCCESS))
        return rc;
      continue;
    }

    if (pk_key_old.iov_base == pk_key_new.iov_base ||
        fpta_is_same(pk_key_old, pk_key_new))
      continue;

    /* Изменился PK, необходимо обновить пару<SE_value, PK_value> во вторичном
     * индексе. Комбинация MDB_CURRENT | MDB_NOOVERWRITE для таблиц с
     * MDB_DUPSORT включает в mdbx_replace() режим обновления конкретного
     * значения из multivalue. Таким образом, мы меняем ссылку именно со
     * старого значения PK на новое, даже если для индексируемого поля
     * разрешены не уникальные значения. */
    rc = mdbx_replace(txn->mdbx_txn, dbi[i], &fk_key_new.mdbx, &pk_key_new,
                      &pk_key_old,
                      fpta_index_is_unique(index)
                          ? MDB_CURRENT | MDB_NODUPDATA
                          : MDB_CURRENT | MDB_NODUPDATA | MDB_NOOVERWRITE);
    if (unlikely(rc != MDB_SUCCESS))
      return rc;
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
    const auto shove = table_id->table.def->columns[i];
    const auto index = fpta_shove2index(shove);
    if (index == fpta_index_none)
      break;
    assert(i < fpta_max_indexes);
    if (i == stepover)
      continue;

    fpta_key fk_key_old;
    rc = fpta_index_row2key(shove, i, row_old, fk_key_old, false);
    if (unlikely(rc != MDB_SUCCESS))
      return rc;

    rc = mdbx_del(txn->mdbx_txn, dbi[i], &fk_key_old.mdbx, &pk_key);
    if (unlikely(rc != MDB_SUCCESS))
      return (rc != MDB_NOTFOUND) ? rc : (int)FPTA_INDEX_CORRUPTED;
  }

  return FPTA_SUCCESS;
}

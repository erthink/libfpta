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

static __inline bool fpta_is_same(const MDB_val &a, const MDB_val &b) {
  return a.iov_len == b.iov_len &&
         memcmp(a.iov_base, b.iov_base, a.iov_len) == 0;
}

bool fpta_cursor_validate(const fpta_cursor *cursor, fpta_level min_level) {
  if (unlikely(cursor == nullptr || cursor->mdbx_cursor == nullptr ||
               !fpta_txn_validate(cursor->txn, min_level)))
    return false;

  // TODO
  return true;
}

int fpta_cursor_close(fpta_cursor *cursor) {
  if (unlikely(!fpta_cursor_validate(cursor, fpta_read)))
    return FPTA_EINVAL;

  mdbx_cursor_close(cursor->mdbx_cursor);
  fpta_cursor_free(cursor->db, cursor);
  return FPTA_SUCCESS;
}

int fpta_cursor_open(fpta_txn *txn, fpta_name *column_id,
                     fpta_value range_from, fpta_value range_to,
                     fpta_filter *filter, fpta_cursor_options op,
                     fpta_cursor **pcursor) {
  if (unlikely(pcursor == nullptr))
    return FPTA_EINVAL;
  *pcursor = nullptr;

  switch (op) {
  default:
    return FPTA_EINVAL;

  case fpta_descending:
  case fpta_descending_dont_fetch:
  case fpta_unsorted:
  case fpta_unsorted_dont_fetch:
  case fpta_ascending:
  case fpta_ascending_dont_fetch:
    break;
  }

  if (unlikely(!fpta_id_validate(column_id, fpta_column)))
    return FPTA_EINVAL;
  if (unlikely(!fpta_filter_validate(filter)))
    return FPTA_EINVAL;

  fpta_name *table_id = column_id->column.table;
  int rc = fpta_name_refresh_couple(txn, table_id, column_id);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  fpta_index_type index = fpta_shove2index(column_id->shove);
  if (unlikely(index == fpta_index_none))
    return FPTA_NO_INDEX;

  if (!fpta_index_is_ordered(index) && fpta_cursor_is_ordered(op))
    return FPTA_NO_INDEX;

  if (unlikely(!fpta_index_is_compat(column_id->shove, range_from) ||
               !fpta_index_is_compat(column_id->shove, range_to)))
    return FPTA_ETYPE;

  if (unlikely(range_from.type == fpta_end || range_to.type == fpta_begin))
    return FPTA_EINVAL;

  if (unlikely(column_id->mdbx_dbi < 1)) {
    rc = fpta_open_column(txn, column_id);
    if (unlikely(rc != FPTA_SUCCESS))
      return rc;
  }

  fpta_db *db = txn->db;
  fpta_cursor *cursor = fpta_cursor_alloc(db);
  if (unlikely(cursor == nullptr))
    return FPTA_ENOMEM;

  cursor->options = op;
  cursor->txn = txn;
  cursor->filter = filter;
  cursor->table_id = table_id;
  cursor->index.shove =
      column_id->shove & (fpta_column_typeid_mask | fpta_column_index_mask);
  cursor->index.column_order = (unsigned)column_id->column.num;
  cursor->index.mdbx_dbi = column_id->mdbx_dbi;

  cursor->range_from_value = range_from;
  if (cursor->range_from_value.type != fpta_begin) {
    rc = fpta_index_value2key(cursor->index.shove, cursor->range_from_value,
                              cursor->range_from_key, true);
    if (unlikely(rc != FPTA_SUCCESS))
      goto bailout;
  }

  cursor->range_to_value = range_to;
  if (cursor->range_to_value.type != fpta_end) {
    rc = fpta_index_value2key(cursor->index.shove, cursor->range_to_value,
                              cursor->range_to_key, true);
    if (unlikely(rc != FPTA_SUCCESS))
      goto bailout;
  }

  rc = mdbx_cursor_open(txn->mdbx_txn, cursor->index.mdbx_dbi,
                        &cursor->mdbx_cursor);
  if (unlikely(rc != MDB_SUCCESS))
    goto bailout;

  if ((op & fpta_dont_fetch) == 0) {
    rc = fpta_cursor_move(cursor, fpta_first);
    if (unlikely(rc != MDB_SUCCESS))
      goto bailout;
  }

  *pcursor = cursor;
  return FPTA_SUCCESS;

bailout:
  if (cursor->mdbx_cursor)
    mdbx_cursor_close(cursor->mdbx_cursor);
  fpta_cursor_free(db, cursor);
  return rc;
}

//----------------------------------------------------------------------------

static int fpta_cursor_seek(fpta_cursor *cursor, MDB_cursor_op mdbx_seek_op,
                            MDB_cursor_op mdbx_step_op,
                            MDB_val *mdbx_seek_key, MDB_val *mdbx_seek_data) {
  assert(mdbx_seek_key != &cursor->current);
  fptu_ro mdbx_data;
  int rc;

  if (likely(mdbx_seek_key == NULL)) {
    assert(mdbx_seek_data == NULL);
    rc = mdbx_cursor_get(cursor->mdbx_cursor, &cursor->current,
                         &mdbx_data.sys, mdbx_seek_op);
  } else {
    rc = mdbx_cursor_get(cursor->mdbx_cursor, mdbx_seek_key, mdbx_seek_data,
                         mdbx_seek_op);
    if (unlikely(rc != MDB_SUCCESS))
      goto bailout;

    rc = mdbx_cursor_get(cursor->mdbx_cursor, &cursor->current,
                         &mdbx_data.sys, MDB_GET_CURRENT);
  }

  while (rc == MDB_SUCCESS) {
    if (cursor->range_from_value.type != fpta_begin &&
        mdbx_cmp(cursor->txn->mdbx_txn, cursor->index.mdbx_dbi,
                 &cursor->range_from_key.mdbx, &cursor->current) < 0) {
      if (fpta_index_is_ordered(cursor->index.shove))
        goto eof;
      goto next;
    }

    if (cursor->range_to_value.type != fpta_end &&
        mdbx_cmp(cursor->txn->mdbx_txn, cursor->index.mdbx_dbi,
                 &cursor->range_to_key.mdbx, &cursor->current) >= 0) {
      if (fpta_index_is_ordered(cursor->index.shove))
        goto eof;
      goto next;
    }

    if (!cursor->filter)
      return FPTA_SUCCESS;

    if (fpta_index_is_secondary(cursor->index.shove)) {
      MDB_val pk_key = mdbx_data.sys;
      rc = mdbx_get(cursor->txn->mdbx_txn, cursor->table_id->mdbx_dbi,
                    &pk_key, &mdbx_data.sys);
      if (unlikely(rc != MDB_SUCCESS))
        return (rc != MDB_NOTFOUND) ? rc : (int)FPTA_INDEX_CORRUPTED;
    }

    if (fpta_filter_match(cursor->filter, mdbx_data))
      return FPTA_SUCCESS;

  next:
    rc = mdbx_cursor_get(cursor->mdbx_cursor, &cursor->current,
                         &mdbx_data.sys, mdbx_step_op);
  }

bailout:
  if (unlikely(rc != MDB_NOTFOUND)) {
    cursor->set_poor();
    return rc;
  }

eof:
  switch (mdbx_seek_op) {
  default:
    cursor->set_poor();
    return FPTA_NODATA;

  case MDB_NEXT:
  case MDB_NEXT_NODUP:
    cursor->set_eof(fpta_cursor::after_last);
    return FPTA_NODATA;

  case MDB_PREV:
  case MDB_PREV_NODUP:
    cursor->set_eof(fpta_cursor::before_first);
    return FPTA_NODATA;

  case MDB_PREV_DUP:
  case MDB_NEXT_DUP:
    return FPTA_NODATA;
  }
}

int fpta_cursor_move(fpta_cursor *cursor, fpta_seek_operations op) {
  if (unlikely(!fpta_cursor_validate(cursor, fpta_read)))
    return FPTA_EINVAL;

  if (unlikely(op < fpta_first || op > fpta_key_prev))
    return FPTA_EINVAL;

  if (fpta_cursor_is_descending(cursor->options))
    op = (fpta_seek_operations)(op ^ 1);

  MDB_val *mdbx_seek_key = nullptr;
  MDB_cursor_op mdbx_seek_op, mdbx_step_op;
  switch (op) {
  default:
    assert(false && "unexpecepted seek-op");
    return FPTA_EOOPS;

  case fpta_first:
    if (cursor->range_from_value.type == fpta_begin ||
        !fpta_index_is_ordered(cursor->index.shove)) {
      mdbx_seek_op = MDB_FIRST;
    } else {
      mdbx_seek_key = &cursor->range_from_key.mdbx;
      mdbx_seek_op = MDB_SET_KEY;
    }
    mdbx_step_op = MDB_NEXT;
    break;

  case fpta_last:
    if (cursor->range_to_value.type == fpta_end ||
        !fpta_index_is_ordered(cursor->index.shove)) {
      mdbx_seek_op = MDB_LAST;
    } else {
      mdbx_seek_key = &cursor->range_to_key.mdbx;
      mdbx_seek_op = MDB_SET_KEY;
    }
    mdbx_step_op = MDB_PREV;
    break;

  case fpta_next:
    if (unlikely(cursor->is_poor()))
      return FPTA_ECURSOR;
    mdbx_seek_op = unlikely(cursor->is_before_first()) ? MDB_FIRST : MDB_NEXT;
    mdbx_step_op = MDB_NEXT;
    break;
  case fpta_prev:
    if (unlikely(cursor->is_poor()))
      return FPTA_ECURSOR;
    mdbx_seek_op = unlikely(cursor->is_after_last()) ? MDB_LAST : MDB_PREV;
    mdbx_step_op = MDB_PREV;
    break;

  /* Перемещение по дубликатам значения ключа, в случае если
   * соответствующий индекс был БЕЗ флага fpta_index_uniq */
  case fpta_dup_first:
    if (unlikely(!cursor->is_filled()))
      return cursor->unladed_state();
    if (unlikely(fpta_index_is_unique(cursor->index.shove)))
      return FPTA_SUCCESS;
    mdbx_seek_op = MDB_FIRST_DUP;
    mdbx_step_op = MDB_NEXT_DUP;
    break;

  case fpta_dup_last:
    if (unlikely(!cursor->is_filled()))
      return cursor->unladed_state();
    if (unlikely(fpta_index_is_unique(cursor->index.shove)))
      return FPTA_SUCCESS;
    mdbx_seek_op = MDB_LAST_DUP;
    mdbx_step_op = MDB_PREV_DUP;
    break;

  case fpta_dup_next:
    if (unlikely(!cursor->is_filled()))
      return cursor->unladed_state();
    if (unlikely(fpta_index_is_unique(cursor->index.shove)))
      return FPTA_NODATA;
    mdbx_seek_op = MDB_NEXT_DUP;
    mdbx_step_op = MDB_NEXT_DUP;
    break;

  case fpta_dup_prev:
    if (unlikely(!cursor->is_filled()))
      return cursor->unladed_state();
    if (unlikely(fpta_index_is_unique(cursor->index.shove)))
      return FPTA_NODATA;
    mdbx_seek_op = MDB_PREV_DUP;
    mdbx_step_op = MDB_PREV_DUP;
    break;

  case fpta_key_next:
    if (unlikely(cursor->is_poor()))
      return FPTA_ECURSOR;
    mdbx_seek_op =
        unlikely(cursor->is_before_first()) ? MDB_FIRST : MDB_NEXT_NODUP;
    mdbx_step_op = MDB_NEXT_NODUP;
    break;

  case fpta_key_prev:
    if (unlikely(cursor->is_poor()))
      return FPTA_ECURSOR;
    mdbx_seek_op =
        unlikely(cursor->is_after_last()) ? MDB_LAST : MDB_PREV_NODUP;
    mdbx_step_op = MDB_PREV_NODUP;
    break;
  }

  return fpta_cursor_seek(cursor, mdbx_seek_op, mdbx_step_op, mdbx_seek_key,
                          nullptr);
}

int fpta_cursor_locate(fpta_cursor *cursor, bool exactly,
                       const fpta_value *key, const fptu_ro *row) {
  if (unlikely(!fpta_cursor_validate(cursor, fpta_read)))
    return FPTA_EINVAL;

  if (unlikely((key && row) || (!key && !row)))
    return FPTA_EINVAL;

  fpta_key seek_key;
  MDB_val *mdbx_seek_data;
  fpta_key pk_key;
  int rc;
  if (key) {
    rc = fpta_index_value2key(cursor->index.shove, *key, seek_key, false);
    mdbx_seek_data = nullptr;
  } else {
    rc = fpta_index_row2key(cursor->index.shove, cursor->index.column_order,
                            *row, seek_key, false);
    mdbx_seek_data = const_cast<MDB_val *>(&row->sys);

    if (fpta_index_is_secondary(cursor->index.shove)) {
      if (unlikely(rc != FPTA_SUCCESS))
        return rc;
      rc = fpta_index_row2key(cursor->table_id->table.pk, 0, *row, pk_key,
                              false);
      mdbx_seek_data = &pk_key.mdbx;
    }
  }
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  MDB_cursor_op mdbx_seek_op;
  if (exactly) {
    mdbx_seek_op = row ? MDB_GET_BOTH : MDB_SET;
  } else {
    mdbx_seek_op = row ? MDB_GET_BOTH_RANGE : MDB_SET_RANGE;
  }

  rc = fpta_cursor_seek(cursor, mdbx_seek_op, MDB_NEXT, &seek_key.mdbx,
                        mdbx_seek_data);
  if (likely(rc == FPTA_SUCCESS) && exactly && !row &&
      !fpta_index_is_unique(cursor->index.shove)) {
    size_t dups;
    if (unlikely(mdbx_cursor_count(cursor->mdbx_cursor, &dups) !=
                 MDB_SUCCESS))
      return FPTA_EOOPS;
    if (unlikely(dups > 1))
      /* возвращаем ошибку, если запрошено точное позиционирование
       * по ключу (без указания полного значения строки) и с заданным
       * значением ключа связано более одной строки. */
      return FPTA_EMULTIVAL;
  }
  return rc;
}

//----------------------------------------------------------------------------

int fpta_cursor_eof(fpta_cursor *cursor) {
  if (unlikely(!fpta_cursor_validate(cursor, fpta_read)))
    return FPTA_EINVAL;

  if (likely(cursor->is_filled()))
    return FPTA_SUCCESS;

  return FPTA_NODATA;
}

int fpta_cursor_count(fpta_cursor *cursor, size_t *pcount, size_t limit) {
  if (unlikely(!pcount))
    return FPTA_EINVAL;
  *pcount = (size_t)FPTA_DEADBEEF;

  size_t count = 0;
  int rc = fpta_cursor_move(cursor, fpta_first);
  while (rc == FPTA_SUCCESS && count < limit) {
    ++count;
    rc = fpta_cursor_move(cursor, fpta_next);
  }

  if (rc == FPTA_NODATA) {
    *pcount = count;
    rc = FPTA_SUCCESS;
  }

  cursor->set_poor();
  return rc;
}

int fpta_cursor_dups(fpta_cursor *cursor, size_t *pdups) {
  if (unlikely(pdups == nullptr))
    return FPTA_EINVAL;
  *pdups = (size_t)FPTA_DEADBEEF;

  if (unlikely(!fpta_cursor_validate(cursor, fpta_read)))
    return FPTA_EINVAL;

  if (unlikely(!cursor->is_filled())) {
    if (cursor->is_poor())
      return FPTA_ECURSOR;
    *pdups = 0;
    return FPTA_NODATA;
  }

  *pdups = 0;
  int rc = mdbx_cursor_count(cursor->mdbx_cursor, pdups);
  return (rc == MDB_NOTFOUND) ? (int)FPTA_NODATA : rc;
}

//----------------------------------------------------------------------------

int fpta_cursor_get(fpta_cursor *cursor, fptu_ro *row) {
  if (unlikely(row == nullptr))
    return FPTA_EINVAL;

  row->total_bytes = 0;
  row->units = nullptr;

  if (unlikely(!fpta_cursor_validate(cursor, fpta_read)))
    return FPTA_EINVAL;

  if (unlikely(!cursor->is_filled()))
    return cursor->unladed_state();

  if (fpta_index_is_primary(cursor->index.shove))
    return mdbx_cursor_get(cursor->mdbx_cursor, &cursor->current, &row->sys,
                           MDB_GET_CURRENT);

  MDB_val pk_key;
  int rc = mdbx_cursor_get(cursor->mdbx_cursor, &cursor->current, &pk_key,
                           MDB_GET_CURRENT);
  if (unlikely(rc != MDB_SUCCESS))
    return rc;

  rc = mdbx_get(cursor->txn->mdbx_txn, cursor->table_id->mdbx_dbi, &pk_key,
                &row->sys);
  return (rc != MDB_NOTFOUND) ? rc : (int)FPTA_INDEX_CORRUPTED;
}

int fpta_cursor_key(fpta_cursor *cursor, fpta_value *key) {
  if (unlikely(key == nullptr))
    return FPTA_EINVAL;
  if (unlikely(!fpta_cursor_validate(cursor, fpta_read)))
    return FPTA_EINVAL;

  if (unlikely(!cursor->is_filled()))
    return cursor->unladed_state();

  int rc = fpta_index_key2value(cursor->index.shove, cursor->current, *key);
  return rc;
}

int fpta_cursor_delete(fpta_cursor *cursor) {
  if (unlikely(!fpta_cursor_validate(cursor, fpta_write)))
    return FPTA_EINVAL;

  if (unlikely(!cursor->is_filled()))
    return cursor->unladed_state();

  if (!fpta_table_has_secondary(cursor->table_id)) {
    int rc = mdbx_cursor_del(cursor->mdbx_cursor, 0);
    if (unlikely(rc != FPTA_SUCCESS))
      return rc;
  } else {
    MDB_val pk_key;
    if (fpta_index_is_primary(cursor->index.shove)) {
      pk_key = cursor->current;
    } else {
      int rc = mdbx_cursor_get(cursor->mdbx_cursor, &cursor->current, &pk_key,
                               MDB_GET_CURRENT);
      if (unlikely(rc != MDB_SUCCESS))
        return (rc != MDB_NOTFOUND) ? rc : (int)FPTA_INDEX_CORRUPTED;
    }

    fptu_ro old;
#if defined(NDEBUG) && !defined(_MSC_VER)
    const constexpr size_t likely_enough = 64u * 42u;
#else
    const size_t likely_enough = (time(nullptr) & 1) ? 11u : 64u * 42u;
#endif /* NDEBUG */
    void *buffer = alloca(likely_enough);
    old.sys.iov_base = buffer;
    old.sys.iov_len = likely_enough;

    int rc = mdbx_replace(cursor->txn->mdbx_txn, cursor->table_id->mdbx_dbi,
                          &pk_key, nullptr, &old.sys, MDB_CURRENT);
    if (unlikely(rc == -1) && old.sys.iov_base == nullptr &&
        old.sys.iov_len > likely_enough) {
      old.sys.iov_base = alloca(old.sys.iov_len);
      rc = mdbx_replace(cursor->txn->mdbx_txn, cursor->table_id->mdbx_dbi,
                        &pk_key, nullptr, &old.sys, MDB_CURRENT);
    }
    if (unlikely(rc != MDB_SUCCESS))
      return rc;

    rc = fpta_secondary_remove(cursor->txn, cursor->table_id, pk_key, old,
                               cursor->index.column_order);
    if (unlikely(rc != MDB_SUCCESS))
      return fpta_inconsistent_abort(cursor->txn, rc);

    rc = mdbx_cursor_del(cursor->mdbx_cursor, 0);
    if (unlikely(rc != MDB_SUCCESS))
      return fpta_inconsistent_abort(cursor->txn, rc);
  }

  if (mdbx_cursor_eof(cursor->mdbx_cursor) == 1)
    cursor->set_eof(fpta_cursor::after_last);
  return FPTA_SUCCESS;
}

//----------------------------------------------------------------------------

int fpta_cursor_validate_update(fpta_cursor *cursor, fptu_ro new_row_value) {
  if (unlikely(!fpta_cursor_validate(cursor, fpta_write)))
    return FPTA_EINVAL;

  if (unlikely(!cursor->is_filled()))
    return cursor->unladed_state();

  fpta_key column_key;
  int rc = fpta_index_row2key(cursor->index.shove, cursor->index.column_order,
                              new_row_value, column_key, false);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (!fpta_is_same(cursor->current, column_key.mdbx))
    return FPTA_ROW_MISMATCH;

  if (!fpta_table_has_secondary(cursor->table_id))
    return FPTA_SUCCESS;

  fptu_ro present_row;
  if (fpta_index_is_primary(cursor->index.shove)) {
    rc = mdbx_cursor_get(cursor->mdbx_cursor, &cursor->current,
                         &present_row.sys, MDB_GET_CURRENT);
    if (unlikely(rc != MDB_SUCCESS))
      return rc;

    return fpta_check_constraints(cursor->txn, cursor->table_id, present_row,
                                  new_row_value, 0);
  }

  MDB_val present_pk_key;
  rc = mdbx_cursor_get(cursor->mdbx_cursor, &cursor->current, &present_pk_key,
                       MDB_GET_CURRENT);
  if (unlikely(rc != MDB_SUCCESS))
    return rc;

  fpta_key new_pk_key;
  rc = fpta_index_row2key(cursor->table_id->table.pk, 0, new_row_value,
                          new_pk_key, false);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  rc = mdbx_get(cursor->txn->mdbx_txn, cursor->table_id->mdbx_dbi,
                &present_pk_key, &present_row.sys);
  if (unlikely(rc != MDB_SUCCESS))
    return (rc != MDB_NOTFOUND) ? rc : (int)FPTA_INDEX_CORRUPTED;

  return fpta_check_constraints(cursor->txn, cursor->table_id, present_row,
                                new_row_value, cursor->index.column_order);
}

int fpta_cursor_update(fpta_cursor *cursor, fptu_ro new_row_value) {
  if (unlikely(!fpta_cursor_validate(cursor, fpta_write)))
    return FPTA_EINVAL;

  if (unlikely(!cursor->is_filled()))
    return cursor->unladed_state();

  fpta_key column_key;
  int rc = fpta_index_row2key(cursor->index.shove, cursor->index.column_order,
                              new_row_value, column_key, false);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (!fpta_is_same(cursor->current, column_key.mdbx))
    return FPTA_ROW_MISMATCH;

  if (!fpta_table_has_secondary(cursor->table_id))
    return mdbx_cursor_put(cursor->mdbx_cursor, &column_key.mdbx,
                           &new_row_value.sys, MDB_CURRENT | MDB_NODUPDATA);

  MDB_val old_pk_key;
  if (fpta_index_is_primary(cursor->index.shove)) {
    old_pk_key = cursor->current;
  } else {
    rc = mdbx_cursor_get(cursor->mdbx_cursor, &cursor->current, &old_pk_key,
                         MDB_GET_CURRENT);
    if (unlikely(rc != MDB_SUCCESS))
      return (rc != MDB_NOTFOUND) ? rc : (int)FPTA_INDEX_CORRUPTED;
  }

  fptu_ro old;
  rc = mdbx_get(cursor->txn->mdbx_txn, cursor->table_id->mdbx_dbi,
                &old_pk_key, &old.sys);
  if (unlikely(rc != MDB_SUCCESS))
    return (rc != MDB_NOTFOUND) ? rc : (int)FPTA_INDEX_CORRUPTED;

  fpta_key new_pk_key;
  rc = fpta_index_row2key(cursor->table_id->table.pk, 0, new_row_value,
                          new_pk_key, false);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  rc = fpta_secondary_upsert(cursor->txn, cursor->table_id, old_pk_key, old,
                             new_pk_key.mdbx, new_row_value,
                             cursor->index.column_order);
  if (unlikely(rc != MDB_SUCCESS))
    return fpta_inconsistent_abort(cursor->txn, rc);

  const bool pk_changed = !fpta_is_same(old_pk_key, new_pk_key.mdbx);
  if (pk_changed) {
    rc = mdbx_del(cursor->txn->mdbx_txn, cursor->table_id->mdbx_dbi,
                  &old_pk_key, nullptr);
    if (unlikely(rc != MDB_SUCCESS))
      return fpta_inconsistent_abort(cursor->txn, rc);

    rc = mdbx_put(cursor->txn->mdbx_txn, cursor->table_id->mdbx_dbi,
                  &new_pk_key.mdbx, &new_row_value.sys,
                  MDB_NODUPDATA | MDB_NOOVERWRITE);
    if (unlikely(rc != MDB_SUCCESS))
      return fpta_inconsistent_abort(cursor->txn, rc);

    rc = mdbx_cursor_put(cursor->mdbx_cursor, &column_key.mdbx,
                         &new_pk_key.mdbx, MDB_CURRENT | MDB_NODUPDATA);

  } else {
    rc = mdbx_put(cursor->txn->mdbx_txn, cursor->table_id->mdbx_dbi,
                  &new_pk_key.mdbx, &new_row_value.sys,
                  MDB_CURRENT | MDB_NODUPDATA);
  }
  if (unlikely(rc != MDB_SUCCESS))
    return fpta_inconsistent_abort(cursor->txn, rc);

  return FPTA_SUCCESS;
}

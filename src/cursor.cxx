/*
 * Copyright 2016 libfpta authors: please see AUTHORS file.
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

bool fpta_cursor_validate(const fpta_cursor *cursor, fpta_level min_level)
{
    if (unlikely(cursor == nullptr || cursor->mdbx_cursor == nullptr ||
                 !fpta_txn_validate(cursor->txn, min_level)))
        return false;

    // TODO
    return true;
}

int fpta_cursor_close(fpta_cursor *cursor)
{
    if (unlikely(!fpta_cursor_validate(cursor, fpta_read)))
        return FPTA_EINVAL;

    mdbx_cursor_close(cursor->mdbx_cursor);
    fpta_cursor_free(cursor->db, cursor);
    return FPTA_SUCCESS;
}

int fpta_cursor_open(fpta_txn *txn, fpta_name *column_id,
                     fpta_value range_from, fpta_value range_to,
                     fpta_filter *filter, fpta_cursor_options op,
                     fpta_cursor **pcursor)
{
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

    fpta_name *table_id = (fpta_name *)column_id->handle;
    int rc = fpta_name_refresh_couple(txn, table_id, column_id);
    if (unlikely(rc != FPTA_SUCCESS))
        return rc;

    fpta_index_type index = fpta_shove2index(column_id->internal);
    if (unlikely(index == fpta_index_none))
        return FPTA_EINVAL;

    if (!fpta_index_is_ordered(index) && fpta_cursor_is_ordered(op))
        return FPTA_EINVAL;

    if (unlikely(!fpta_index_is_compat(column_id->internal, range_from) ||
                 !fpta_index_is_compat(column_id->internal, range_to)))
        return FPTA_ETYPE;

    if (unlikely(range_from.type == fpta_end || range_to.type == fpta_begin))
        return FPTA_EINVAL;

    if (fpta_index_is_secondary(index)) {
        // TODO: fpta_secondary
        return FPTA_ENOIMP;
    }

    if (unlikely(table_id->mdbx_dbi < 1)) {
        rc = fpta_table_open(txn, table_id, nullptr);
        if (unlikely(rc != FPTA_SUCCESS))
            return rc;
    }

    assert(column_id->handle == table_id);
    if (fpta_index_is_primary(index)) {
        assert(column_id->column.num == 0);
        column_id->mdbx_dbi = table_id->mdbx_dbi;
    } else if (unlikely(column_id->mdbx_dbi < 1)) {
        assert(column_id->column.num > 0);
        assert(fpta_index_is_secondary(index));
        rc = fpta_table_open(txn, table_id, column_id);
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
    cursor->index.shove = column_id->internal &
                          (fpta_column_typeid_mask | fpta_column_index_mask);
    cursor->index.column_order = column_id->column.num;
    cursor->index.mdbx_dbi = column_id->mdbx_dbi;

    cursor->range_from_value = range_from;
    if (cursor->range_from_value.type != fpta_begin) {
        rc = fpta_index_value2key(cursor->index.shove,
                                  cursor->range_from_value,
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
                            MDB_val *mdbx_seek_key, MDB_val *mdbx_seek_data)
{
    assert(mdbx_seek_key != &cursor->current);
    int rc = mdbx_cursor_get(cursor->mdbx_cursor, mdbx_seek_key,
                             mdbx_seek_data, mdbx_seek_op);
    if (unlikely(rc != MDB_SUCCESS))
        goto bailout;

    fptu_ro mdbx_data;
    rc = mdbx_cursor_get(cursor->mdbx_cursor, &cursor->current,
                         &mdbx_data.sys, MDB_GET_CURRENT);
    while (rc == MDB_SUCCESS) {
        if (cursor->range_from_value.type != fpta_begin &&
            mdbx_cmp(cursor->txn->mdbx_txn, cursor->index.mdbx_dbi,
                     &cursor->range_from_key.mdbx, &cursor->current) < 0)
            goto eof;

        if (cursor->range_to_value.type != fpta_end &&
            mdbx_cmp(cursor->txn->mdbx_txn, cursor->index.mdbx_dbi,
                     &cursor->range_to_key.mdbx, &cursor->current) >= 0)
            goto eof;

        if (fpta_filter_match(cursor->filter, mdbx_data))
            return FPTA_SUCCESS;

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

int fpta_cursor_move(fpta_cursor *cursor, fpta_seek_operations op)
{
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
        if (cursor->range_from_value.type == fpta_begin) {
            mdbx_seek_op = MDB_FIRST;
        } else {
            mdbx_seek_key = &cursor->range_from_key.mdbx;
            mdbx_seek_op = MDB_SET_KEY;
        }
        mdbx_step_op = MDB_NEXT;
        break;

    case fpta_last:
        if (cursor->range_to_value.type == fpta_end) {
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
        mdbx_seek_op =
            unlikely(cursor->is_before_first()) ? MDB_FIRST : MDB_NEXT;
        mdbx_step_op = MDB_NEXT;
        break;
    case fpta_prev:
        if (unlikely(cursor->is_poor()))
            return FPTA_ECURSOR;
        mdbx_seek_op =
            unlikely(cursor->is_after_last()) ? MDB_LAST : MDB_PREV;
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
                       const fpta_value *key, const fptu_ro *row)
{
    if (unlikely(!fpta_cursor_validate(cursor, fpta_read)))
        return FPTA_EINVAL;

    if (unlikely((key && row) || (!key && !row)))
        return FPTA_EINVAL;

    fpta_key seek_key;
    MDB_val *mdbx_seek_data;
    int rc;
    if (key) {
        rc = fpta_index_value2key(cursor->index.shove, *key, seek_key, false);
        mdbx_seek_data = nullptr;
    } else {
        rc = fpta_index_row2key(cursor->index.shove,
                                cursor->index.column_order, *row, seek_key,
                                false);
        mdbx_seek_data = const_cast<MDB_val *>(&row->sys);
    }
    if (unlikely(rc != FPTA_SUCCESS))
        return rc;

    MDB_cursor_op mdbx_seek_op;
    if (exactly) {
        mdbx_seek_op = row ? MDB_GET_BOTH : MDB_SET;
    } else {
        mdbx_seek_op = row ? MDB_GET_BOTH_RANGE : MDB_SET_RANGE;
    }

    return fpta_cursor_seek(cursor, mdbx_seek_op, MDB_NEXT, &seek_key.mdbx,
                            mdbx_seek_data);
}

//----------------------------------------------------------------------------

int fpta_cursor_eof(fpta_cursor *cursor)
{
    if (unlikely(!fpta_cursor_validate(cursor, fpta_read)))
        return FPTA_EINVAL;

    if (likely(cursor->is_filled()))
        return FPTA_SUCCESS;

    return FPTA_NODATA;
}

int fpta_cursor_count(fpta_cursor *cursor, size_t *pcount, size_t limit)
{
    if (unlikely(!pcount))
        return FPTA_EINVAL;
    *pcount = FPTA_DEADBEEF;

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

int fpta_cursor_dups(fpta_cursor *cursor, size_t *pdups)
{
    if (unlikely(pdups == nullptr))
        return FPTA_EINVAL;
    *pdups = FPTA_DEADBEEF;

    if (unlikely(!fpta_cursor_validate(cursor, fpta_read)))
        return FPTA_EINVAL;

    if (unlikely(!cursor->is_filled())) {
        if (cursor->is_poor())
            return FPTA_ECURSOR;
        *pdups = 0;
        return FPTA_NODATA;
    }

    if (fpta_index_is_unique(cursor->index.shove)) {
        *pdups = 1;
        return FPTA_SUCCESS;
    }

    size_t dups = 0;
    int rc = mdbx_cursor_count(cursor->mdbx_cursor, &dups);
    if (rc == MDB_NOTFOUND) {
        *pdups = dups;
        rc = FPTA_SUCCESS;
    }
    return rc;
}

//----------------------------------------------------------------------------

int fpta_cursor_get(fpta_cursor *cursor, fptu_ro *row)
{
    if (unlikely(row == nullptr))
        return FPTA_EINVAL;

    row->total_bytes = 0;
    row->units = nullptr;

    if (unlikely(!fpta_cursor_validate(cursor, fpta_read)))
        return FPTA_EINVAL;

    if (unlikely(!cursor->is_filled()))
        return cursor->unladed_state();

    int rc = mdbx_cursor_get(cursor->mdbx_cursor, &cursor->current, &row->sys,
                             MDB_GET_CURRENT);
    return rc;
}

int fpta_cursor_key(fpta_cursor *cursor, fpta_value *key)
{
    if (unlikely(key == nullptr))
        return FPTA_EINVAL;
    if (unlikely(!fpta_cursor_validate(cursor, fpta_read)))
        return FPTA_EINVAL;

    if (unlikely(!cursor->is_filled()))
        return cursor->unladed_state();

    int rc = fpta_index_key2value(cursor->index.shove, cursor->current, *key);
    return rc;
}

int fpta_cursor_delete(fpta_cursor *cursor, bool all_dups)
{
    if (unlikely(!fpta_cursor_validate(cursor, fpta_write)))
        return FPTA_EINVAL;

    if (unlikely(!cursor->is_filled()))
        return cursor->unladed_state();

    unsigned flags = 0;
    if (all_dups && !fpta_index_is_unique(cursor->index.shove))
        flags |= MDB_NODUPDATA;

    int rc = mdbx_cursor_del(cursor->mdbx_cursor, flags);
    if (rc == MDB_SUCCESS && mdbx_cursor_eof(cursor->mdbx_cursor) == 1)
        cursor->set_eof(fpta_cursor::after_last);

    return rc;
}

int fpta_cursor_update(fpta_cursor *cursor, fptu_ro row_value)
{
    if (unlikely(!fpta_cursor_validate(cursor, fpta_write)))
        return FPTA_EINVAL;

    if (unlikely(!cursor->is_filled()))
        return cursor->unladed_state();

    fpta_key key;
    int rc =
        fpta_index_row2key(cursor->index.shove, cursor->index.column_order,
                           row_value, key, false);
    if (unlikely(rc != FPTA_SUCCESS))
        return rc;

    if (mdbx_cmp(cursor->txn->mdbx_txn, cursor->index.mdbx_dbi,
                 &cursor->current, &key.mdbx) != 0)
        return FPTA_ROW_MISMATCH;

    rc = mdbx_cursor_put(cursor->mdbx_cursor, &key.mdbx, &row_value.sys,
                         MDB_CURRENT | MDB_NODUPDATA);
    return rc;
}

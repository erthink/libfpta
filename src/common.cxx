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

static int fpta_db_lock(fpta_db *db, fpta_level level)
{
    assert(level >= fpta_read && level <= fpta_schema);

    int rc;
    if (level < fpta_schema)
        rc = pthread_rwlock_rdlock(&db->schema_rwlock);
    else
        rc = pthread_rwlock_wrlock(&db->schema_rwlock);

    assert(rc == 0);
    return rc;
}

static int fpta_db_unlock(fpta_db *db, fpta_level level)
{
    assert(level >= fpta_read && level <= fpta_schema);

    int rc = pthread_rwlock_unlock(&db->schema_rwlock);
    assert(rc == 0);
    return rc;
}

static fpta_txn *fpta_txn_alloc(fpta_db *db, fpta_level level)
{
    // TODO: use pool
    fpta_txn *txn = (fpta_txn *)calloc(1, sizeof(fpta_txn));
    if (likely(txn)) {
        txn->db = db;
        txn->level = level;
    }
    return txn;
}

static void fpta_txn_free(fpta_db *db, fpta_txn *txn)
{
    // TODO: use pool
    if (likely(txn)) {
        assert(txn->db == db);
        txn->db = nullptr;
        free(txn);
    }
}

fpta_cursor *fpta_cursor_alloc(fpta_db *db)
{
    // TODO: use pool
    fpta_cursor *cursor = (fpta_cursor *)calloc(1, sizeof(fpta_cursor));
    if (likely(cursor))
        cursor->db = db;
    return cursor;
}

void fpta_cursor_free(fpta_db *db, fpta_cursor *cursor)
{
    // TODO: use pool
    if (likely(cursor)) {
        assert(cursor->db == db);
        cursor->db = nullptr;
        free(cursor);
    }
}

//----------------------------------------------------------------------------

int fpta_db_open(const char *path, fpta_durability durability,
                 mode_t file_mode, size_t megabytes, bool alterable_schema,
                 fpta_db **pdb)
{
    if (unlikely(pdb == nullptr))
        return FPTA_EINVAL;
    *pdb = nullptr;

    if (unlikely(path == nullptr || *path == '\0'))
        return FPTA_EINVAL;

    int mdbx_flags = MDB_NOSUBDIR;
    switch (durability) {
    default:
        return FPTA_EINVAL;
    case fpta_readonly:
        mdbx_flags |= MDB_RDONLY;
        break;
    case fpta_sync:
        mdbx_flags |= MDBX_LIFORECLAIM | MDBX_COALESCE;
        break;
    case fpta_lazy:
        mdbx_flags |=
            MDBX_LIFORECLAIM | MDBX_COALESCE | MDB_NOSYNC | MDB_NOMETASYNC;
        break;
    case fpta_async:
        mdbx_flags |= MDBX_LIFORECLAIM | MDBX_COALESCE | MDB_WRITEMAP |
                      MDB_MAPASYNC | MDBX_UTTERLY_NOSYNC;
        break;
    }

    fpta_db *db = (fpta_db *)calloc(1, sizeof(fpta_db));
    if (unlikely(db == nullptr))
        return FPTA_ENOMEM;

    (void)alterable_schema; // TODO

    int rc = pthread_rwlock_init(&db->schema_rwlock, nullptr);
    if (unlikely(rc != 0)) {
        free(db);
        return (fpta_error)rc;
    }

    rc = pthread_mutex_init(&db->dbi_mutex, nullptr);
    if (unlikely(rc != 0)) {
        int err = pthread_rwlock_destroy(&db->schema_rwlock);
        assert(err == 0);
        free(db);
        return (fpta_error)rc;
    }

    rc = mdbx_env_create(&db->mdbx_env);
    if (unlikely(rc != MDB_SUCCESS))
        goto bailout;

    rc = mdbx_env_set_userctx(db->mdbx_env, db);
    if (unlikely(rc != MDB_SUCCESS))
        goto bailout;

    rc = mdbx_env_set_maxreaders(db->mdbx_env, 42);
    if (unlikely(rc != MDB_SUCCESS))
        goto bailout;

    rc = mdbx_env_set_maxdbs(db->mdbx_env, fpta_tables_max);
    if (unlikely(rc != MDB_SUCCESS))
        goto bailout;

    rc = mdbx_env_set_mapsize(db->mdbx_env, megabytes * (1 << 20));
    if (unlikely(rc != MDB_SUCCESS))
        goto bailout;

    rc = mdbx_env_open(db->mdbx_env, path, mdbx_flags, file_mode);
    if (unlikely(rc != MDB_SUCCESS))
        goto bailout;

    *pdb = db;
    return FPTA_SUCCESS;

bailout:
    if (db->mdbx_env) {
        int err =
            mdbx_env_close_ex(db->mdbx_env, true /* don't touch/save/sync */);
        assert(err == MDB_SUCCESS);
    }

    int err = pthread_mutex_destroy(&db->dbi_mutex);
    assert(err == 0);
    err = pthread_rwlock_destroy(&db->schema_rwlock);
    assert(err == 0);

    free(db);
    return (fpta_error)rc;
}

int fpta_db_close(fpta_db *db)
{
    if (unlikely(!fpta_db_validate(db)))
        return FPTA_EINVAL;

    int rc = fpta_db_lock(db, fpta_schema);
    if (unlikely(rc != 0))
        return (fpta_error)rc;

    rc = pthread_mutex_lock(&db->dbi_mutex);
    if (unlikely(rc != 0)) {
        int err = fpta_db_unlock(db, fpta_schema);
        assert(err == 0);
        return (fpta_error)rc;
    }

    rc = (fpta_error)mdbx_env_close_ex(db->mdbx_env, false);
    assert(rc == MDB_SUCCESS);
    db->mdbx_env = nullptr;

    int err = pthread_mutex_unlock(&db->dbi_mutex);
    assert(err == 0);
    err = pthread_mutex_destroy(&db->dbi_mutex);
    assert(err == 0);

    err = fpta_db_unlock(db, fpta_schema);
    assert(err == 0);
    err = pthread_rwlock_destroy(&db->schema_rwlock);
    assert(err == 0);

    free(db);
    return (fpta_error)rc;
}

//----------------------------------------------------------------------------

int fpta_transaction_begin(fpta_db *db, fpta_level level, fpta_txn **ptxn)
{
    if (unlikely(ptxn == nullptr))
        return FPTA_EINVAL;
    *ptxn = nullptr;

    if (unlikely(level < fpta_read || level > fpta_schema))
        return FPTA_EINVAL;

    if (unlikely(!fpta_db_validate(db)))
        return FPTA_EINVAL;

    int err = fpta_db_lock(db, level);
    if (unlikely(err != 0))
        return (fpta_error)err;

    int rc = FPTA_ENOMEM;
    fpta_txn *txn = fpta_txn_alloc(db, level);
    if (unlikely(txn == nullptr))
        goto bailout;

    rc =
        mdbx_txn_begin(db->mdbx_env, nullptr,
                       (level == fpta_read) ? MDB_RDONLY : 0, &txn->mdbx_txn);
    if (unlikely(rc != MDB_SUCCESS))
        goto bailout;

    mdbx_canary canary;
    txn->data_version = mdbx_canary_get(txn->mdbx_txn, &canary);
    txn->schema_version = canary.v;
    assert(txn->schema_version <= txn->data_version);

    *ptxn = txn;
    return FPTA_SUCCESS;

bailout:
    err = fpta_db_unlock(db, level);
    assert(err == 0);
    fpta_txn_free(db, txn);
    *ptxn = nullptr;
    return (fpta_error)rc;
}

int fpta_transaction_end(fpta_txn *txn, bool abort)
{
    if (unlikely(!fpta_txn_validate(txn, fpta_read)))
        return FPTA_EINVAL;

    int rc;
    if (txn->level == fpta_read) {
        // TODO: reuse txn with mdbx_txn_reset(), but pool needed...
        rc = mdbx_txn_abort(txn->mdbx_txn);
    } else if (!abort) {
        if (txn->level == fpta_schema &&
            txn->schema_version == txn->data_version) {
            rc = mdbx_canary_put(txn->mdbx_txn, nullptr);
            if (rc != MDB_SUCCESS) {
                int err = mdbx_txn_abort(txn->mdbx_txn);
                if (err != MDB_SUCCESS)
                    rc = err;
            } else
                rc = mdbx_txn_commit(txn->mdbx_txn);
        } else
            rc = mdbx_txn_commit(txn->mdbx_txn);
    } else {
        rc = mdbx_txn_abort(txn->mdbx_txn);
    }
    txn->mdbx_txn = nullptr;

    int err = fpta_db_unlock(txn->db, txn->level);
    assert(err == 0);
    fpta_txn_free(txn->db, txn);

    return (fpta_error)rc;
}

int fpta_transaction_versions(fpta_txn *txn, size_t *data, size_t *schema)
{
    if (unlikely(!fpta_txn_validate(txn, fpta_read)))
        return FPTA_EINVAL;

    if (likely(data))
        *data = txn->data_version;
    if (likely(schema))
        *schema = txn->schema_version;
    return FPTA_SUCCESS;
}

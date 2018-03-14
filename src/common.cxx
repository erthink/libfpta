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

#include "details.h"

static int fpta_db_lock(fpta_db *db, fpta_level level) {
  assert(level >= fpta_read && level <= fpta_schema);

  int rc;
  if (db->alterable_schema) {
    if (level < fpta_schema)
      rc = fpta_rwl_sharedlock(&db->schema_rwlock);
    else
      rc = fpta_rwl_exclusivelock(&db->schema_rwlock);
    assert(rc == FPTA_SUCCESS);
  } else {
    rc = (level < fpta_schema) ? FPTA_SUCCESS : FPTA_EPERM;
  }

  return rc;
}

static int fpta_db_unlock(fpta_db *db, fpta_level level) {
  (void)level;
  assert(level >= fpta_read && level <= fpta_schema);

  int rc;
  if (db->alterable_schema) {
    rc = fpta_rwl_unlock(&db->schema_rwlock);
  } else {
    rc = (level < fpta_schema) ? FPTA_SUCCESS : FPTA_EOOPS;
  }
  assert(rc == FPTA_SUCCESS);
  return rc;
}

static fpta_txn *fpta_txn_alloc(fpta_db *db, fpta_level level) {
  // TODO: use pool
  (void)level;
  fpta_txn *txn = (fpta_txn *)calloc(1, sizeof(fpta_txn));
  if (likely(txn)) {
    txn->db = db;
    txn->level = level;
  }
  return txn;
}

static void fpta_txn_free(fpta_db *db, fpta_txn *txn) {
  // TODO: use pool
  (void)db;
  if (likely(txn)) {
    assert(txn->db == db);
    txn->db = nullptr;
    free(txn);
  }
}

fpta_cursor *fpta_cursor_alloc(fpta_db *db) {
  // TODO: use pool
  fpta_cursor *cursor = (fpta_cursor *)calloc(1, sizeof(fpta_cursor));
  if (likely(cursor))
    cursor->db = db;
  return cursor;
}

void fpta_cursor_free(fpta_db *db, fpta_cursor *cursor) {
  // TODO: use pool
  if (likely(cursor)) {
    assert(cursor->db == db);
    (void)db;
    cursor->db = nullptr;
    free(cursor);
  }
}

//----------------------------------------------------------------------------

int fpta_db_open(const char *path, fpta_durability durability,
                 fpta_regime_flags regime_flags, mode_t file_mode,
                 size_t megabytes, bool alterable_schema, fpta_db **pdb) {
  if (unlikely(pdb == nullptr))
    return FPTA_EINVAL;
  *pdb = nullptr;

  if (unlikely(path == nullptr || *path == '\0'))
    return FPTA_EINVAL;

  if (unlikely(megabytes > (SIZE_MAX >> 20)))
    return FPTA_EINVAL;

  unsigned mdbx_flags = MDBX_NOSUBDIR;
  switch (durability) {
  default:
    return FPTA_EFLAG;
  case fpta_readonly:
    mdbx_flags |= MDBX_RDONLY;
    break;
  case fpta_weak:
    mdbx_flags |= MDBX_UTTERLY_NOSYNC;
  /* fall through */
  case fpta_lazy:
    mdbx_flags |= MDBX_NOSYNC | MDBX_NOMETASYNC;
    if (0 == (regime_flags & fpta_saferam))
      mdbx_flags |= MDBX_WRITEMAP;
  /* fall through */
  case fpta_sync:
    if (regime_flags & fpta_frendly4hdd) {
      // LY: nothing for now
    } else {
      if (regime_flags & fpta_frendly4writeback)
        mdbx_flags |= MDBX_LIFORECLAIM;
      if (regime_flags & fpta_frendly4compaction)
        mdbx_flags |= MDBX_COALESCE;
    }
    break;
  }

  fpta_db *db = (fpta_db *)calloc(1, sizeof(fpta_db));
  if (unlikely(db == nullptr))
    return FPTA_ENOMEM;

  int rc;
  db->alterable_schema = alterable_schema;
  if (db->alterable_schema) {
    rc = fpta_rwl_init(&db->schema_rwlock);
    if (unlikely(rc != 0)) {
      free(db);
      return (fpta_error)rc;
    }
  }

  rc = fpta_mutex_init(&db->dbi_mutex);
  if (unlikely(rc != 0)) {
    int err = fpta_rwl_destroy(&db->schema_rwlock);
    assert(err == 0);
    (void)err;
    free(db);
    return (fpta_error)rc;
  }

  rc = mdbx_env_create(&db->mdbx_env);
  if (unlikely(rc != MDBX_SUCCESS))
    goto bailout;

  rc = mdbx_env_set_userctx(db->mdbx_env, db);
  if (unlikely(rc != MDBX_SUCCESS))
    goto bailout;

  rc = mdbx_env_set_maxreaders(db->mdbx_env, 42 /* FIXME */);
  if (unlikely(rc != MDBX_SUCCESS))
    goto bailout;

  rc = mdbx_env_set_maxdbs(db->mdbx_env, fpta_tables_max);
  if (unlikely(rc != MDBX_SUCCESS))
    goto bailout;

  rc = mdbx_env_set_mapsize(db->mdbx_env, megabytes << 20);
  if (unlikely(rc != MDBX_SUCCESS))
    goto bailout;

  rc = mdbx_env_open(db->mdbx_env, path, mdbx_flags, file_mode);
  if (unlikely(rc != MDBX_SUCCESS))
    goto bailout;

  *pdb = db;
  return FPTA_SUCCESS;

bailout:
  if (db->mdbx_env) {
    int err = mdbx_env_close_ex(db->mdbx_env, true /* don't touch/save/sync */);
    assert(err == MDBX_SUCCESS);
    (void)err;
  }

  int err = fpta_mutex_destroy(&db->dbi_mutex);
  assert(err == 0);
  if (alterable_schema) {
    err = fpta_rwl_destroy(&db->schema_rwlock);
    assert(err == 0);
  }
  (void)err;

  free(db);
  return (fpta_error)rc;
}

int fpta_db_close(fpta_db *db) {
  if (unlikely(!fpta_db_validate(db)))
    return FPTA_EINVAL;

  int rc = fpta_db_lock(db, db->alterable_schema ? fpta_schema : fpta_write);
  if (unlikely(rc != 0))
    return (fpta_error)rc;

  rc = fpta_mutex_lock(&db->dbi_mutex);
  if (unlikely(rc != 0)) {
    int err = fpta_db_unlock(db, fpta_schema);
    assert(err == 0);
    (void)err;
    return (fpta_error)rc;
  }

  rc = (fpta_error)mdbx_env_close_ex(db->mdbx_env, false);
  assert(rc == MDBX_SUCCESS);
  db->mdbx_env = nullptr;

  int err = fpta_mutex_unlock(&db->dbi_mutex);
  assert(err == 0);
  err = fpta_mutex_destroy(&db->dbi_mutex);
  assert(err == 0);

  err = fpta_db_unlock(db, db->alterable_schema ? fpta_schema : fpta_write);
  assert(err == 0);
  if (db->alterable_schema) {
    err = fpta_rwl_destroy(&db->schema_rwlock);
    assert(err == 0);
  }
  (void)err;

  free(db);
  return (fpta_error)rc;
}

//----------------------------------------------------------------------------

int fpta_transaction_begin(fpta_db *db, fpta_level level, fpta_txn **ptxn) {
  if (unlikely(ptxn == nullptr))
    return FPTA_EINVAL;
  *ptxn = nullptr;

  if (unlikely(level < fpta_read || level > fpta_schema))
    return FPTA_EFLAG;

  if (unlikely(!fpta_db_validate(db)))
    return FPTA_EINVAL;

  int err = fpta_db_lock(db, level);
  if (unlikely(err != 0))
    return (fpta_error)err;

  int rc = FPTA_ENOMEM;
  fpta_txn *txn = fpta_txn_alloc(db, level);
  if (unlikely(txn == nullptr))
    goto bailout;

  rc = mdbx_txn_begin(db->mdbx_env, nullptr,
                      (level == fpta_read) ? (unsigned)MDBX_RDONLY : 0u,
                      &txn->mdbx_txn);
  if (unlikely(rc != MDBX_SUCCESS))
    goto bailout;

retry:
  rc = mdbx_canary_get(txn->mdbx_txn, &txn->canary);
  if (unlikely(rc != MDBX_SUCCESS))
    goto bailout_abort;

  txn->db_version = mdbx_txn_id(txn->mdbx_txn);
  assert(txn->schema_csn() <=
         ((level > fpta_read) ? txn->db_version - 1 : txn->db_version));

  if (unlikely(db->schema_csn != txn->schema_csn())) {
    fpta_lock_guard guard;
    if (txn->level < fpta_schema) {
      rc = guard.lock(&db->dbi_mutex);
      if (unlikely(rc != 0))
        goto bailout_abort;
    }

    if (db->schema_csn > txn->schema_csn() && level == fpta_read) {
      rc = mdbx_txn_reset(txn->mdbx_txn);
      if (likely(rc == MDBX_SUCCESS))
        rc = mdbx_txn_renew(txn->mdbx_txn);
      if (likely(rc == MDBX_SUCCESS))
        goto retry;
      rc = fpta_internal_abort(txn, rc, true);
      goto bailout;
    }

    rc = fpta_dbicache_cleanup(txn, nullptr, true);
    if (unlikely(rc != FPTA_SUCCESS))
      goto bailout_abort;

    db->schema_csn = txn->schema_csn();
  }

  *ptxn = txn;
  return FPTA_SUCCESS;

bailout_abort:
  rc = fpta_internal_abort(txn, rc, false);

bailout:
  err = fpta_db_unlock(db, level);
  assert(err == 0);
  (void)err;
  fpta_txn_free(db, txn);
  *ptxn = nullptr;
  return (fpta_error)rc;
}

int fpta_transaction_end(fpta_txn *txn, bool abort) {
  int rc = fpta_txn_validate(txn, fpta_read);
  if (unlikely(rc != FPTA_SUCCESS)) {
    if (rc == FPTA_TXN_CANCELLED)
      goto cancelled;
    return rc;
  }

  if (txn->level == fpta_read) {
    // TODO: reuse txn with mdbx_txn_reset(), but pool needed...
    rc = mdbx_txn_commit(txn->mdbx_txn);
  } else if (unlikely(abort)) {
    rc = fpta_internal_abort(txn, FPTA_OK);
  } else {
    rc = mdbx_canary_put(txn->mdbx_txn, &txn->canary);
    if (likely(rc == MDBX_SUCCESS))
      rc = mdbx_txn_commit(txn->mdbx_txn);
    if (unlikely(rc != MDBX_SUCCESS))
      rc = fpta_internal_abort(txn, rc, true);
  }
  txn->mdbx_txn = nullptr;

cancelled:
  int err = fpta_db_unlock(txn->db, txn->level);
  assert(err == 0);
  (void)err;
  fpta_txn_free(txn->db, txn);

  return (fpta_error)rc;
}

int fpta_transaction_versions(fpta_txn *txn, uint64_t *db_version,
                              uint64_t *schema_version) {
  int rc = fpta_txn_validate(txn, fpta_read);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (likely(db_version))
    *db_version = txn->db_version;
  if (likely(schema_version))
    *schema_version = txn->schema_csn();
  return FPTA_SUCCESS;
}

int fpta_db_sequence(fpta_txn *txn, uint64_t *result, uint64_t increment) {
  if (unlikely(result == nullptr))
    return FPTA_EINVAL;

  int rc = fpta_txn_validate(txn, fpta_read);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  *result = txn->db_sequence();
  if (increment) {
    if (unlikely(txn->level < fpta_write))
      return FPTA_EPERM;

    uint64_t value = txn->db_sequence() + increment;
    if (value < increment) {
      static_assert(FPTA_NODATA == MDBX_RESULT_TRUE, "expect equal");
      return FPTA_NODATA;
    }

    assert(txn->db_sequence() < value);
    txn->db_sequence() = value;
  }

  return FPTA_SUCCESS;
}

//----------------------------------------------------------------------------

int
#if defined(__GNUC__) || __has_attribute(weak)
    __attribute__((weak))
#endif
    fpta_panic(int errnum_initial, int errnum_fatal) {
  (void)errnum_initial;
  (void)errnum_fatal;
  return (FPTA_ENABLE_ABORT_ON_PANIC) ? 0 : -1;
}

int fpta_internal_abort(fpta_txn *txn, int errnum, bool txn_maybe_dead) {
  /* Некоторые ошибки (например переполнение БД) могут происходить когда
   * мы выполнили лишь часть операций. В таких случаях можно лишь
   * прервать/откатить всю транзакцию, что и делает эта функция.
   *
   * Однако, могут быть ошибки отката транзакции, что потенциально является
   * более серьезной проблемой. */

  if (txn->level > fpta_read) {
    /* Чистим кеш dbi-хендлов покалеченных таблиц */
    bool dbi_locked = false;
    fpta_db *db = txn->db;
    for (size_t i = 0; i < fpta_dbi_cache_size; ++i) {
      const MDBX_dbi dbi = db->dbi_handles[i];
      const fpta_shove_t shove = db->dbi_shoves[i];
      if (shove && dbi) {
        unsigned tbl_flags, tbl_state;
        int err = mdbx_dbi_flags_ex(txn->mdbx_txn, dbi, &tbl_flags, &tbl_state);
        if (err != MDBX_SUCCESS || (tbl_state & MDBX_TBL_CREAT)) {
          if (!dbi_locked && txn->level < fpta_schema) {
            err = fpta_mutex_lock(&db->dbi_mutex);
            if (unlikely(err != 0))
              return err;
            dbi_locked = true;
          }

          if (shove == db->dbi_shoves[i] && dbi == db->dbi_handles[i]) {
            db->dbi_shoves[i] = 0;
            db->dbi_handles[i] = 0;
          }
        }
      }
    }

    if (db->schema_dbi > 0) {
      unsigned tbl_flags, tbl_state;
      int err = mdbx_dbi_flags_ex(txn->mdbx_txn, db->schema_dbi, &tbl_flags,
                                  &tbl_state);
      if (err != MDBX_SUCCESS || (tbl_state & MDBX_TBL_CREAT)) {
        if (!dbi_locked && txn->level < fpta_schema) {
          err = fpta_mutex_lock(&db->dbi_mutex);
          if (unlikely(err != 0))
            return err;
          dbi_locked = true;
        }
        db->schema_dbi = 0;
      }
    }

    if (dbi_locked) {
      int err = fpta_mutex_unlock(&db->dbi_mutex);
      assert(err == 0);
      (void)err;
    }
  }

  int rc = mdbx_txn_abort(txn->mdbx_txn);
  if (unlikely(rc != MDBX_SUCCESS)) {
    switch (rc) {
    case MDBX_EBADSIGN /* already aborted txn */:
    /* fallthrough */
    case MDBX_THREAD_MISMATCH /* already aborted and started in other thread */:
      if (txn_maybe_dead)
        break;
    /* fallthrough */
    default:
      if (!fpta_panic(errnum, rc))
        abort();
      errnum = FPTA_WANNA_DIE;
    }
  }

  txn->mdbx_txn = nullptr;
  return errnum;
}

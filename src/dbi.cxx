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

struct fpta_dbi_name {
  char cstr[(64 + 6 - 1) / 6 /* 64-битный хэш */ + 1 /* терминирующий 0 */];
};

static void fpta_shove2str(fpta_shove_t shove, fpta_dbi_name *name) {
  const static char aplhabet[65] =
      "@0123456789qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM_";

  char *buf = name->cstr;
  do
    *buf++ = aplhabet[shove & 63];
  while (shove >>= 6);

  *buf = '\0';
  assert(buf < name->cstr + sizeof(name->cstr));
}

static __inline MDBX_dbi fpta_dbicache_peek(const fpta_txn *txn,
                                            const fpta_shove_t shove,
                                            const unsigned cache_hint,
                                            const uint64_t current_csn) {
  if (likely(cache_hint < fpta_dbi_cache_size)) {
    const fpta_db *db = txn->db;
    if (likely(db->dbi_shoves[cache_hint] == shove &&
               db->dbi_csns[cache_hint] == current_csn))
      return db->dbi_handles[cache_hint];
  }
  return 0;
}

static __hot MDBX_dbi fpta_dbicache_lookup(fpta_db *db, fpta_shove_t shove,
                                           unsigned *__restrict cache_hint) {
  if (likely(*cache_hint < fpta_dbi_cache_size)) {
    if (likely(db->dbi_shoves[*cache_hint] == shove))
      return db->dbi_handles[*cache_hint];
    *cache_hint = ~0u;
  }

  const size_t n = shove % fpta_dbi_cache_size;
  size_t i = n;
  do {
    if (db->dbi_shoves[i] == shove) {
      *cache_hint = (unsigned)i;
      return db->dbi_handles[i];
    }
    i = (i + 1) % fpta_dbi_cache_size;
  } while (i != n && db->dbi_shoves[i]);

  return 0;
}

static unsigned fpta_dbicache_update(fpta_db *db, const fpta_shove_t shove,
                                     const MDBX_dbi dbi, const uint64_t csn) {
  assert(shove > 0);

  const size_t n = shove % fpta_dbi_cache_size;
  size_t i = n;
  do {
    assert(db->dbi_shoves[i] != shove);
    if (db->dbi_shoves[i] == 0) {
      db->dbi_handles[i] = dbi;
      db->dbi_csns[i] = csn;
      db->dbi_shoves[i] = shove;
      return (unsigned)i;
    }
    i = (i + 1) % fpta_dbi_cache_size;
  } while (i != n);

  /* TODO: прокричать что кэш переполнен (слишком много таблиц и индексов) */
  return ~0u;
}

__cold MDBX_dbi fpta_dbicache_remove(fpta_db *db, const fpta_shove_t shove,
                                     unsigned *__restrict const cache_hint) {
  assert(shove > 0);

  if (cache_hint) {
    const size_t i = *cache_hint;
    if (i < fpta_dbi_cache_size) {
      *cache_hint = ~0u;
      if (db->dbi_shoves[i] == shove) {
        MDBX_dbi dbi = db->dbi_handles[i];
        db->dbi_shoves[i] = 0;
        return dbi;
      }
    }
    return 0;
  }

  const size_t n = shove % fpta_dbi_cache_size;
  size_t i = n;
  do {
    if (db->dbi_shoves[i] == shove) {
      MDBX_dbi dbi = db->dbi_handles[i];
      db->dbi_shoves[i] = 0;
      return dbi;
    }
    i = (i + 1) % fpta_dbi_cache_size;
  } while (i != n && db->dbi_shoves[i]);

  return 0;
}

static int fpta_dbicache_check_and_invalidate_locked(
    fpta_txn *txn, const fpta_shove_t shove, unsigned *__restrict cache_hint,
    const unsigned dbi_flags, uint64_t tardy_csn) {
  fpta_db *db = txn->db;
  assert(cache_hint != nullptr);

  const MDBX_dbi handle = fpta_dbicache_lookup(db, shove, cache_hint);
  if (!handle)
    return FPTA_NODATA;

  if (db->dbi_csns[*cache_hint] == txn->schema_csn())
    return FPTA_SUCCESS;

  if (db->dbi_csns[*cache_hint] > txn->schema_csn())
    return FPTA_SCHEMA_CHANGED;

  /* get current dbi-mode flags */
  unsigned flags, state;
  int rc = mdbx_dbi_flags_ex(txn->mdbx_txn, handle, &flags, &state);
  if (rc == MDBX_SUCCESS && (state & MDBX_TBL_STALE)) {
    MDBX_stat info;
    /* force refresh */
    rc = mdbx_dbi_stat(txn->mdbx_txn, handle, &info, sizeof(info));
    if (rc == MDBX_SUCCESS) {
      /* fetch again */
      rc = mdbx_dbi_flags_ex(txn->mdbx_txn, handle, &flags, &state);
    }
  }

  if (rc != MDBX_SUCCESS && rc != MDBX_BAD_DBI)
    return rc;

  if (rc == MDBX_SUCCESS && (state & MDBX_TBL_STALE) == 0 &&
      flags == dbi_flags) {
    /* dbi flags have not changed */
    db->dbi_csns[*cache_hint] = txn->schema_csn();
    return FPTA_SUCCESS;
  }

  if (rc != MDBX_BAD_DBI) {
    if (tardy_csn == 0) {
      MDBX_envinfo info;
      int rc = mdbx_env_info(db->mdbx_env, &info, sizeof(info));
      if (unlikely(rc != FPTA_SUCCESS))
        return rc;

      tardy_csn = info.mi_self_latter_reader_txnid;
    }

    if (tardy_csn < txn->schema_csn())
      return FPTA_TARDY_DBI /* handle may be used by other txn */;

    rc = mdbx_dbi_close(db->mdbx_env, handle);
    if (rc != MDBX_SUCCESS && rc != MDBX_BAD_DBI)
      return rc;
  }

  db->dbi_handles[*cache_hint] = 0;
  db->dbi_shoves[*cache_hint] = 0;
  *cache_hint = ~0u;
  return FPTA_NODATA;
}

__cold int fpta_dbi_open(fpta_txn *txn, const fpta_shove_t dbi_shove,
                         MDBX_dbi &__restrict handle, const unsigned dbi_flags,
                         const fpta_shove_t key_shove,
                         const fpta_shove_t data_shove,
                         unsigned *const cache_hint) {
  fpta_lock_guard guard;
  assert(fpta_txn_validate(txn, fpta_read) == FPTA_SUCCESS);
  fpta_db *db = txn->db;

  if (likely(cache_hint)) {
    if (txn->level < fpta_schema) {
      int err = guard.lock(&db->dbi_mutex);
      if (unlikely(err != 0))
        return err;
    }

    int rc = fpta_dbicache_check_and_invalidate_locked(
        txn, dbi_shove, cache_hint, dbi_flags, 0);
    if (rc == FPTA_SUCCESS) {
      handle = db->dbi_handles[*cache_hint];
      return rc;
    }
    if (rc != FPTA_NODATA)
      return rc;
  }

  fpta_dbi_name dbi_name;
  fpta_shove2str(dbi_shove, &dbi_name);

  const auto keycmp = fpta_index_shove2comparator(key_shove);
  const auto datacmp = fpta_index_shove2comparator(data_shove);
  int rc = mdbx_dbi_open_ex(txn->mdbx_txn, dbi_name.cstr, dbi_flags, &handle,
                            keycmp, datacmp);
  if (likely(rc == FPTA_SUCCESS)) {
    assert(handle != 0);
    if (cache_hint)
      *cache_hint =
          fpta_dbicache_update(db, dbi_shove, handle, txn->schema_csn());
  } else {
    assert(handle == 0);
  }

  return rc;
}

__cold int fpta_dbicache_cleanup(fpta_txn *txn, fpta_table_schema *table_def,
                                 bool locked) {
  fpta_db *db = txn->db;
  fpta_lock_guard guard;

  if (!locked && txn->level < fpta_schema) {
    int err = guard.lock(&db->dbi_mutex);
    if (unlikely(err != 0))
      return err;
  }

  MDBX_envinfo info;
  int rc = mdbx_env_info(db->mdbx_env, &info, sizeof(info));
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  const uint64_t tardy_csn =
      (info.mi_self_latter_reader_txnid &&
       info.mi_self_latter_reader_txnid < txn->schema_csn())
          ? info.mi_latter_reader_txnid
          : txn->schema_csn();

  if (table_def) {
    const unsigned tbl_dbi_flags =
        fpta_dbi_flags(table_def->column_shoves_array(), 0);
    rc = fpta_dbicache_check_and_invalidate_locked(
        txn, table_def->table_shove(), &table_def->handle_cache(0),
        tbl_dbi_flags, tardy_csn);
    if (unlikely(rc != FPTA_SUCCESS && rc != FPTA_NODATA))
      return rc;

    for (size_t i = 1; i < table_def->column_count(); ++i) {
      const fpta_shove_t shove = table_def->column_shove(i);
      if (!fpta_is_indexed(shove))
        break;

      const unsigned idx_dbi_flags =
          fpta_dbi_flags(table_def->column_shoves_array(), i);
      const fpta_shove_t dbi_shove =
          fpta_dbi_shove(table_def->table_shove(), i);
      rc = fpta_dbicache_check_and_invalidate_locked(
          txn, dbi_shove, &table_def->handle_cache(i), idx_dbi_flags,
          tardy_csn);
      if (unlikely(rc != FPTA_SUCCESS && rc != FPTA_NODATA))
        return rc;
    }
  }

  if (tardy_csn == txn->schema_csn() && db->schema_csn != txn->schema_csn()) {
    for (size_t i = 0; i < fpta_dbi_cache_size; ++i) {
      if (!db->dbi_handles[i] || db->dbi_csns[i] >= tardy_csn)
        continue;

      int rc = mdbx_dbi_close(db->mdbx_env, db->dbi_handles[i]);
      if (rc != MDBX_SUCCESS && rc != MDBX_BAD_DBI)
        return rc;
      db->dbi_handles[i] = 0;
      db->dbi_shoves[i] = 0;
    }
  }

  return MDBX_SUCCESS;
}

//----------------------------------------------------------------------------

int __hot fpta_open_table(fpta_txn *txn, fpta_table_schema *table_def,
                          MDBX_dbi &handle) {
  const unsigned dbi_flags =
      fpta_dbi_flags(table_def->column_shoves_array(), 0);
  const fpta_shove_t dbi_shove = fpta_dbi_shove(table_def->table_shove(), 0);
  handle = fpta_dbicache_peek(txn, dbi_shove, table_def->handle_cache(0),
                              table_def->version_csn());
  if (likely(handle > 0))
    return FPTA_OK;

  const fpta_shove_t data_shove =
      fpta_data_shove(table_def->column_shoves_array(), 0);
  return fpta_dbi_open(txn, dbi_shove, handle, dbi_flags, table_def->table_pk(),
                       data_shove, &table_def->handle_cache(0));
}

int __hot fpta_open_column(fpta_txn *txn, fpta_name *column_id,
                           MDBX_dbi &tbl_handle, MDBX_dbi &idx_handle) {
  assert(fpta_id_validate(column_id, fpta_column) == FPTA_SUCCESS);

  fpta_table_schema *table_def = column_id->column.table->table_schema;
  int rc = fpta_open_table(txn, table_def, tbl_handle);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (column_id->column.num == 0) {
    idx_handle = tbl_handle;
    return FPTA_SUCCESS;
  }

  const unsigned dbi_flags =
      fpta_dbi_flags(table_def->column_shoves_array(), column_id->column.num);
  fpta_shove_t dbi_shove =
      fpta_dbi_shove(table_def->table_shove(), column_id->column.num);
  idx_handle = fpta_dbicache_peek(
      txn, dbi_shove, table_def->handle_cache(column_id->column.num),
      table_def->version_csn());
  if (likely(idx_handle > 0))
    return FPTA_OK;

  return fpta_dbi_open(txn, dbi_shove, idx_handle, dbi_flags, column_id->shove,
                       table_def->table_pk(),
                       &table_def->handle_cache(column_id->column.num));
}

int __hot fpta_open_secondaries(fpta_txn *txn, fpta_table_schema *table_def,
                                MDBX_dbi *dbi_array) {
  int rc = fpta_open_table(txn, table_def, dbi_array[0]);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  for (size_t i = 1; i < table_def->column_count(); ++i) {
    const fpta_shove_t shove = table_def->column_shove(i);
    if (!fpta_is_indexed(shove))
      break;

    const unsigned dbi_flags =
        fpta_dbi_flags(table_def->column_shoves_array(), i);
    const fpta_shove_t dbi_shove = fpta_dbi_shove(table_def->table_shove(), i);

    dbi_array[i] = fpta_dbicache_peek(
        txn, dbi_shove, table_def->handle_cache(i), table_def->version_csn());
    if (unlikely(dbi_array[i] == 0)) {
      rc = fpta_dbi_open(txn, dbi_shove, dbi_array[i], dbi_flags, shove,
                         table_def->table_pk(), &table_def->handle_cache(i));
      if (unlikely(rc != FPTA_SUCCESS))
        return rc;
    }
  }

  return FPTA_SUCCESS;
}

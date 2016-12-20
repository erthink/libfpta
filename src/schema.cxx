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

static uint64_t fpta_dbi_shove(const uint64_t table_shove,
                               const unsigned index_id)
{
    assert(table_shove > fpta_flag_table);
    assert(index_id < fpta_max_indexes);

    uint64_t dbi_shove = table_shove - fpta_flag_table;
    assert(0 ==
           (dbi_shove & (fpta_column_typeid_mask | fpta_column_index_mask)));
    dbi_shove += index_id;

    assert(fpta_shove_eq(table_shove, dbi_shove));
    return dbi_shove;
}

static __hot uint64_t fpta_shove_name(const char *name,
                                      enum fpta_schema_item type)
{
    char uppercase[fpta_name_len_max];
    size_t i, len = strlen(name);

    for (i = 0; i < len && i < sizeof(uppercase); ++i)
        uppercase[i] = toupper(name[i]);

    uint64_t shove = t1ha(uppercase, i, type) << fpta_name_hash_shift;
    if (type == fpta_table)
        shove |= fpta_flag_table;
    return shove;
}

bool fpta_name_validate(const char *name)
{
    if (unlikely(name == nullptr))
        return false;

    if (unlikely(!isalpha(name[0])))
        return false;

    size_t len = 1;
    while (name[len]) {
        if (unlikely(!isalnum(name[len]) && name[len] != '_'))
            return false;
        if (unlikely(++len > fpta_name_len_max))
            return false;
    }

    if (unlikely(len < fpta_name_len_min))
        return false;

    return fpta_shove_name(name, fpta_column) > (1 << fpta_name_hash_shift);
}

struct fpta_dbi_name {
    char cstr[(64 + 6 - 1) / 6 /* 64-битный хэш */ + 1 /* терминирующий 0 */];
};

static void fpta_shove2str(uint64_t shove, fpta_dbi_name *name)
{
    const static char aplhabet[65] =
        "@0123456789qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM_";

    char *buf = name->cstr;
    do
        *buf++ = aplhabet[shove & 63];
    while (shove >>= 6);

    *buf = '\0';
    assert(buf < name->cstr + sizeof(name->cstr));
}

static MDB_dbi fpta_dbicache_lookup(fpta_db *db, uint64_t shove)
{
    size_t n = shove % fpta_dbi_cache_size, i = n;

    do {
        if (db->dbi_shoves[i] == shove) {
            assert(db->dbi_handles[i] > 0);
            return db->dbi_handles[i];
        }
        i = (i + 1) % fpta_dbi_cache_size;
    } while (i != n && db->dbi_shoves[i]);
    return 0;
}

static void fpta_dbicache_update(fpta_db *db, uint64_t shove, MDB_dbi handle)
{
    assert(shove > 0);

    size_t n = shove % fpta_dbi_cache_size, i = n;
    for (;;) {
        if (db->dbi_shoves[i] == 0) {
            assert(db->dbi_handles[i] == 0);
            db->dbi_handles[i] = handle;
            db->dbi_shoves[i] = shove;
            break;
        }
        i = (i + 1) % fpta_dbi_cache_size;
        assert(i != n);
    }
}

static void fpta_dbicache_remove(fpta_db *db, uint64_t shove)
{
    assert(shove > 0);
    size_t n = shove % fpta_dbi_cache_size, i = n;

    do {
        if (db->dbi_shoves[i] == shove) {
            assert(db->dbi_handles[i] > 0);
            db->dbi_handles[i] = 0;
            db->dbi_shoves[i] = 0;
            break;
        }
        i = (i + 1) % fpta_dbi_cache_size;
    } while (i != n && db->dbi_shoves[i]);
}

static int fpta_dbi_open(fpta_txn *txn, uint64_t shove, MDB_dbi *handle,
                         unsigned dbi_flags = 0)
{
    assert(fpta_txn_validate(txn, fpta_read) && handle);
    fpta_db *db = txn->db;

    if (shove > 0) {
        *handle = fpta_dbicache_lookup(db, shove);
        if (*handle)
            return FPTA_SUCCESS;
    }

    fpta_dbi_name dbi_name;
    fpta_shove2str(shove, &dbi_name);

    if (txn->level < fpta_schema) {
        int err = pthread_mutex_lock(&db->dbi_mutex);
        if (unlikely(err != 0))
            return err;
    }

    int rc = mdbx_dbi_open(txn->mdbx_txn, dbi_name.cstr, dbi_flags, handle);
    if (rc == FPTA_SUCCESS && shove > 0)
        fpta_dbicache_update(db, shove, *handle);

    if (txn->level < fpta_schema) {
        int err = pthread_mutex_unlock(&db->dbi_mutex);
        assert(err == 0);
    }
    return rc;
}

static int fpta_schema_open(fpta_txn *txn, bool create)
{
    assert(fpta_txn_validate(txn, create ? fpta_schema : fpta_read));
    return fpta_dbi_open(txn, 0, &txn->db->schema_dbi,
                         create ? MDB_INTEGERKEY | MDB_CREATE : 0);
}

int fpta_table_open(fpta_txn *txn, fpta_name *table_id, fpta_name *column_id)
{
    if (column_id) {
        assert(column_id->handle == table_id);
        assert(column_id->column.num > 0);
    }

    unsigned column_number = column_id ? column_id->column.num : 0;
    unsigned index_shove =
        column_id ? column_id->internal : table_id->table.pk;
    uint64_t dbi_shove = fpta_dbi_shove(table_id->internal, column_number);
    MDB_dbi *mdbx_dbi = &(column_id ? column_id : table_id)->mdbx_dbi;

    int rc = fpta_dbi_open(txn, dbi_shove, mdbx_dbi);
    if (unlikely(rc != FPTA_SUCCESS)) {
        assert(*mdbx_dbi < 1);
        return rc;
    }

    assert(*mdbx_dbi > 0);
    return mdbx_set_compare(txn->mdbx_txn, *mdbx_dbi,
                            fpta_index_shove2comparator(index_shove));
}

//----------------------------------------------------------------------------

void fpta_column_set_init(fpta_column_set *column_set)
{
    column_set->count = 0;
    column_set->internal[0] = 0;
}

int fpta_column_describe(const char *column_name, enum fptu_type data_type,
                         fpta_index_type index_type,
                         fpta_column_set *column_set)
{
    if (unlikely(!fpta_name_validate(column_name)))
        return FPTA_EINVAL;

    if (unlikely(data_type == fptu_null ||
                 data_type == (fptu_null | fptu_farray) ||
                 data_type > (fptu_nested /* TODO: | fptu_farray */)))
        return FPTA_EINVAL;

    if (index_type && data_type < fptu_96 &&
        fpta_index_is_reverse(index_type))
        return FPTA_EINVAL;

    switch (index_type) {
    default:
        return FPTA_EINVAL;

    case fpta_primary_unique_unordered:
    case fpta_primary_withdups_unordered:
    case fpta_secondary_unique_unordered:
    case fpta_secondary_withdups_unordered:

    case fpta_primary_unique:
    case fpta_primary_withdups:
    case fpta_primary_unique_reversed:
    case fpta_primary_withdups_reversed:

    case fpta_secondary_unique:
    case fpta_secondary_withdups:
    case fpta_secondary_unique_reversed:
    case fpta_secondary_withdups_reversed:

    case fpta_index_none:
        assert((index_type & fpta_column_index_mask) == index_type);
        break;
    }
    assert(index_type != (fpta_index_type)fpta_flag_table);

    if (unlikely(column_set == nullptr || column_set->count > fpta_max_cols))
        return FPTA_EINVAL;

    if (unlikely(column_set->count == fpta_max_cols))
        return FPTA_TOOMANY;

    uint64_t shove = fpta_column_shove(
        fpta_shove_name(column_name, fpta_column), data_type, index_type);
    assert(fpta_shove2index(shove) != (fpta_index_type)fpta_flag_table);

    for (size_t i = 0; i < column_set->count; ++i) {
        if (fpta_shove_eq(column_set->internal[i], shove))
            return EEXIST;
    }

    if (index_type && fpta_index_is_primary(index_type)) {
        if (column_set->internal[0])
            return EEXIST;
        column_set->internal[0] = shove;
        if (column_set->count < 1)
            column_set->count = 1;
    } else {
        size_t place = (column_set->count > 0) ? column_set->count : 1;
        column_set->internal[place] = shove;
        column_set->count = place + 1;
    }

    return FPTA_SUCCESS;
}

static int fpta_column_def_validate(const uint64_t *def, size_t count)
{
    if (unlikely(count < 1))
        return FPTA_EINVAL;
    if (unlikely(count > fpta_max_cols))
        return FPTA_TOOMANY;

    size_t index_count = 0;
    for (size_t i = 0; i < count; ++i) {
        uint64_t shove = def[i];

        int index_type = fpta_shove2index(shove);
        switch (index_type) {
        default:
            return FPTA_EINVAL;

        case fpta_primary_unique:
        case fpta_primary_withdups:
        case fpta_primary_unique_unordered:
        case fpta_primary_withdups_unordered:
        case fpta_primary_unique_reversed:
        case fpta_primary_withdups_reversed:
            if (i != 0)
                /* первичный ключ может быть только один и только в самом
                 * начале */
                return FPTA_EINVAL;
            break;

        case fpta_secondary_unique:
        case fpta_secondary_withdups:
        case fpta_secondary_unique_unordered:
        case fpta_secondary_withdups_unordered:
        case fpta_secondary_unique_reversed:
        case fpta_secondary_withdups_reversed:
            if (i > 0 && fpta_shove2index(def[i - 1]) == fpta_index_none)
                /* сначала должны идти все индексируемые колонки, потом не
                 * индексируемые */
                return FPTA_EINVAL;
            if (!fpta_index_is_unique(def[0]))
                /* для вторичных индексов первичный ключ должен быть
                 * уникальным */
                return FPTA_EINVAL;
            if (++index_count > fpta_max_indexes)
                return FPTA_TOOMANY;
        case fpta_index_none:
            if (i == 0)
                return FPTA_EINVAL;
            break;
        }
        assert((index_type & fpta_column_index_mask) == index_type);
        assert(index_type != fpta_flag_table);

        fptu_type data_type = fpta_shove2type(shove);
        if (data_type < fptu_uint16 ||
            data_type == (fptu_null | fptu_farray) ||
            data_type > (fptu_nested /* TODO: | fptu_farray */))
            return false;

        if (index_type && data_type < fptu_96 &&
            fpta_index_is_reverse(index_type))
            return FPTA_EINVAL;
    }

    // TODO: check for distinctness.
    return FPTA_SUCCESS;
}

int fpta_column_set_validate(fpta_column_set *column_set)
{
    if (column_set == nullptr)
        return FPTA_EINVAL;
    if (unlikely(column_set->count < 1))
        return FPTA_EINVAL;
    if (unlikely(column_set->count > fpta_max_cols))
        return FPTA_TOOMANY;

    /* сортируем описание колонок, так чтобы неиндексируемые были в конце */
    std::stable_sort(
        column_set->internal + 1, column_set->internal + column_set->count,
        [](auto &left, auto &right) {
            return fpta_shove2index(left) > fpta_shove2index(right);
        });

    return fpta_column_def_validate(column_set->internal, column_set->count);
}

//----------------------------------------------------------------------------

bool fpta_schema_validate(const MDB_val def)
{
    if (unlikely(def.mv_size < fpta_table_schema_size(1)))
        return false;

    if (unlikely((def.mv_size - sizeof(fpta_table_schema)) %
                 sizeof(uint64_t)))
        return false;

    const fpta_table_schema *schema = (const fpta_table_schema *)def.mv_data;
    if (unlikely(schema->signature != FTPA_SCHEMA_SIGNATURE))
        return false;

    if (unlikely(schema->count > fpta_max_cols))
        return false;

    if (unlikely(def.mv_size != fpta_table_schema_size(schema->count)))
        return false;

    if (unlikely(schema->version == 0))
        return false;

    if (unlikely(fpta_shove2index(schema->shove) !=
                 (fpta_index_type)fpta_flag_table))
        return false;

    uint64_t checksum =
        t1ha(&schema->signature, def.mv_size - sizeof(checksum),
             FTPA_SCHEMA_CHECKSEED);
    if (unlikely(checksum != schema->checksum))
        return false;

    return FPTA_SUCCESS ==
           fpta_column_def_validate(schema->columns, schema->count);
}

static int fpta_schema_dup(const MDB_val data, fpta_table_schema **def)
{
    assert(data.mv_size >= fpta_table_schema_size(1) &&
           data.mv_size <= sizeof(fpta_table_schema));
    assert(def != nullptr);

    fpta_table_schema *schema =
        (fpta_table_schema *)realloc(*def, data.mv_size);
    if (unlikely(schema == nullptr))
        return FPTA_ENOMEM;

    *def = (fpta_table_schema *)memcpy(schema, data.mv_data, data.mv_size);
    return FPTA_SUCCESS;
}

static void fpta_schema_free(fpta_table_schema *def)
{
    if (likely(def)) {
        def->signature = 0;
        def->checksum = ~def->checksum;
        def->count = 0;
        free(def);
    }
}

static int fpta_schema_read(fpta_txn *txn, uint64_t shove,
                            fpta_table_schema **def)
{
    assert(fpta_txn_validate(txn, fpta_read) && def);

    int rc;
    fpta_db *db = txn->db;
    if (db->schema_dbi < 1) {
        rc = fpta_schema_open(txn, false);
        if (rc != MDB_SUCCESS)
            return rc;
    }

    MDB_val data, key;
    key.mv_size = sizeof(shove);
    key.mv_data = &shove;
    rc = mdbx_get(txn->mdbx_txn, db->schema_dbi, &key, &data);
    if (rc != MDB_SUCCESS)
        return rc;

    if (!fpta_schema_validate(data))
        return FPTA_SCHEMA_CORRUPTED;

    return fpta_schema_dup(data, def);
}

//----------------------------------------------------------------------------

static int fpta_name_init(fpta_name *id, const char *name,
                          fpta_schema_item schema_item)
{
    if (unlikely(id == nullptr))
        return FPTA_EINVAL;

    memset(id, 0, sizeof(fpta_name));

    if (unlikely(!fpta_name_validate(name)))
        return FPTA_EINVAL;

    switch (schema_item) {
    default:
        return FPTA_EINVAL;
    case fpta_table:
        id->internal = fpta_shove_name(name, fpta_table);
        assert(fpta_shove2index(id->internal) ==
               (fpta_index_type)fpta_flag_table);
        // id->table.dbi = 0;
        id->table.pk = fpta_index_none | fptu_null;
        id->handle = nullptr;
        break;
    case fpta_column:
        id->internal = fpta_column_shove(fpta_shove_name(name, fpta_column),
                                         fptu_null, fpta_index_none);
        assert(fpta_shove2index(id->internal) !=
               (fpta_index_type)fpta_flag_table);
        id->column.num = -1;
        id->handle = id;
        break;
    }

    // id->version = 0;
    return FPTA_SUCCESS;
}

int fpta_table_init(fpta_name *table_id, const char *name)
{
    return fpta_name_init(table_id, name, fpta_table);
}

int fpta_column_init(const fpta_name *table_id, fpta_name *column_id,
                     const char *name)
{
    if (unlikely(!fpta_id_validate(table_id, fpta_table)))
        return FPTA_EINVAL;

    int rc = fpta_name_init(column_id, name, fpta_column);
    if (unlikely(rc != FPTA_SUCCESS))
        return rc;

    column_id->handle = (void *)table_id;
    return FPTA_SUCCESS;
}

void fpta_name_destroy(fpta_name *id)
{
    if (fpta_shove2index(id->internal) == (fpta_index_type)fpta_flag_table)
        fpta_schema_free((fpta_table_schema *)id->handle);
    memset(id, 0, sizeof(fpta_name));
}

int fpta_name_refresh(fpta_txn *txn, fpta_name *table_id,
                      fpta_name *column_id)
{
    if (unlikely(!fpta_id_validate(table_id, fpta_table)))
        return FPTA_EINVAL;
    if (column_id && unlikely(!fpta_id_validate(column_id, fpta_column)))
        return FPTA_EINVAL;
    if (unlikely(!fpta_txn_validate(txn, fpta_read)))
        return FPTA_EINVAL;

    if (unlikely(table_id->version > txn->schema_version))
        return FPTA_ETXNOUT;

    if (unlikely(table_id->version != txn->schema_version)) {
        table_id->mdbx_dbi = 0;

        int rc = fpta_schema_read(txn, table_id->internal,
                                  (fpta_table_schema **)&table_id->handle);
        if (unlikely(rc != FPTA_SUCCESS)) {
            if (rc != MDB_NOTFOUND)
                return rc;
            fpta_schema_free((fpta_table_schema *)table_id->handle);
            table_id->handle = nullptr;
        }

        fpta_table_schema *schema = (fpta_table_schema *)table_id->handle;
        assert(schema == nullptr || txn->schema_version == schema->version);
        table_id->version = txn->schema_version;

        table_id->table.pk =
            schema
                ? schema->columns[0] &
                      (fpta_column_typeid_mask | fpta_column_index_mask)
                : fpta_index_none | fptu_null;
    }

    if (unlikely(table_id->handle == nullptr))
        return MDB_NOTFOUND;

    fpta_table_schema *schema = (fpta_table_schema *)table_id->handle;
    if (unlikely(schema->signature != FTPA_SCHEMA_SIGNATURE))
        return FPTA_SCHEMA_CORRUPTED;

    assert(fpta_shove2index(table_id->internal) ==
           (fpta_index_type)fpta_flag_table);
    if (unlikely(schema->shove != table_id->internal))
        return FPTA_SCHEMA_CORRUPTED;
    assert(table_id->version == schema->version);

    if (column_id == nullptr)
        return FPTA_SUCCESS;

    assert(fpta_shove2index(column_id->internal) !=
           (fpta_index_type)fpta_flag_table);

    if (unlikely(column_id->handle != table_id)) {
        if (column_id->handle != column_id)
            return FPTA_EINVAL;
        column_id->handle = table_id;
    }

    if (unlikely(column_id->version > table_id->version))
        return FPTA_ETXNOUT;

    if (column_id->version != table_id->version) {
        column_id->column.num = -1;
        for (size_t i = 0; i < schema->count; ++i) {
            if (fpta_shove_eq(column_id->internal, schema->columns[i])) {
                column_id->internal = schema->columns[i];
                column_id->column.num = i;
                break;
            }
        }
        column_id->version = table_id->version;
    }

    if (unlikely(column_id->column.num < 0))
        return ENOENT;
    return FPTA_SUCCESS;
}

//----------------------------------------------------------------------------

int fpta_table_create(fpta_txn *txn, const char *table_name,
                      fpta_column_set *column_set)
{
    if (!fpta_txn_validate(txn, fpta_schema))
        return FPTA_EINVAL;
    if (!fpta_name_validate(table_name))
        return FPTA_EINVAL;

    int rc = fpta_column_set_validate(column_set);
    if (rc != FPTA_SUCCESS)
        return rc;

    fpta_db *db = txn->db;
    if (db->schema_dbi < 1) {
        int rc = fpta_schema_open(txn, true);
        if (rc != MDB_SUCCESS)
            return rc;
    }

    MDB_dbi dbi[fpta_max_indexes];
    memset(dbi, 0, sizeof(dbi));
    uint64_t table_shove = fpta_shove_name(table_name, fpta_table);

    for (size_t i = 0; i < column_set->count; ++i) {
        auto index = fpta_shove2index(column_set->internal[i]);
        if (index == fpta_index_none)
            break;
        assert(i < fpta_max_indexes);
        if (fpta_index_is_secondary(index))
            return FPTA_ENOIMP;
        int err = fpta_dbi_open(txn, fpta_dbi_shove(table_shove, i), &dbi[i]);
        if (err != MDB_NOTFOUND)
            return EEXIST;
    }

    for (size_t i = 0; i < column_set->count; ++i) {
        auto index = fpta_shove2index(column_set->internal[i]);
        if (index == fpta_index_none)
            break;
        unsigned dbi_flags =
            fpta_index_shove2dbiflags(column_set->internal[i]);
        rc = fpta_dbi_open(txn, fpta_dbi_shove(table_shove, i), &dbi[i],
                           dbi_flags);
        if (rc != MDB_SUCCESS)
            goto bailout;
    }

    fpta_table_schema def;
    MDB_val data;
    data.mv_data = &def;
    data.mv_size = fpta_table_schema_size(column_set->count);

    def.signature = FTPA_SCHEMA_SIGNATURE;
    def.count = column_set->count;
    def.version = txn->data_version;
    def.shove = table_shove;
    memcpy(def.columns, column_set->internal, sizeof(uint64_t) * def.count);
    def.checksum = t1ha(&def.signature, data.mv_size - sizeof(def.checksum),
                        FTPA_SCHEMA_CHECKSEED);

    MDB_val key;
    key.mv_size = sizeof(table_shove);
    key.mv_data = &table_shove;
    rc =
        mdbx_put(txn->mdbx_txn, db->schema_dbi, &key, &data, MDB_NOOVERWRITE);
    if (rc != MDB_SUCCESS)
        goto bailout;

    txn->schema_version = txn->data_version;
    return FPTA_SUCCESS;

bailout:
    for (size_t i = 0; i < fpta_max_indexes && dbi[i] > 0; ++i) {
        fpta_dbicache_remove(db, fpta_dbi_shove(table_shove, i));
        int err = mdbx_drop(txn->mdbx_txn, dbi[i], 1);
        if (err != MDB_SUCCESS) {
            mdbx_txn_abort(txn->mdbx_txn);
            txn->mdbx_txn = nullptr;
            break;
        }
    }
    return rc;
}

int fpta_table_drop(fpta_txn *txn, const char *table_name)
{
    if (!fpta_txn_validate(txn, fpta_schema))
        return FPTA_EINVAL;
    if (!fpta_name_validate(table_name))
        return FPTA_EINVAL;

    fpta_db *db = txn->db;
    if (db->schema_dbi < 1) {
        int rc = fpta_schema_open(txn, true);
        if (rc != MDB_SUCCESS)
            return rc;
    }

    MDB_dbi dbi[fpta_max_indexes];
    memset(dbi, 0, sizeof(dbi));
    uint64_t table_shove = fpta_shove_name(table_name, fpta_table);

    MDB_val data, key;
    key.mv_size = sizeof(table_shove);
    key.mv_data = &table_shove;
    int rc = mdbx_get(txn->mdbx_txn, db->schema_dbi, &key, &data);
    if (rc != MDB_SUCCESS)
        return rc;

    if (!fpta_schema_validate(data))
        return FPTA_SCHEMA_CORRUPTED;

    const fpta_table_schema *def = (const fpta_table_schema *)data.mv_data;
    for (size_t i = 0; i < def->count; ++i) {
        auto index = fpta_shove2index(def->columns[i]);
        if (index == fpta_index_none)
            break;
        assert(i < fpta_max_indexes);
        rc = fpta_dbi_open(txn, fpta_dbi_shove(table_shove, i), &dbi[i]);
        if (rc != MDB_SUCCESS && rc != MDB_NOTFOUND)
            return rc;
    }

    rc = mdbx_del(txn->mdbx_txn, db->schema_dbi, &key, nullptr);
    if (rc != MDB_SUCCESS)
        return rc;

    txn->schema_version = txn->data_version;
    for (size_t i = 0; i < fpta_max_indexes && dbi[i] > 0; ++i) {
        fpta_dbicache_remove(db, fpta_dbi_shove(table_shove, i));
        int err = mdbx_drop(txn->mdbx_txn, dbi[i], 1);
        if (err != MDB_SUCCESS) {
            mdbx_txn_abort(txn->mdbx_txn);
            txn->mdbx_txn = nullptr;
            break;
        }
    }

    return rc;
}

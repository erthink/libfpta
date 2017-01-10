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

static __inline fpta_shove_t fpta_dbi_shove(const fpta_shove_t table_shove,
                                            const unsigned index_id)
{
    assert(table_shove > fpta_flag_table);
    assert(index_id < fpta_max_indexes);

    fpta_shove_t dbi_shove = table_shove - fpta_flag_table;
    assert(0 ==
           (dbi_shove & (fpta_column_typeid_mask | fpta_column_index_mask)));
    dbi_shove += index_id;

    assert(fpta_shove_eq(table_shove, dbi_shove));
    return dbi_shove;
}

static __hot fpta_shove_t fpta_shove_name(const char *name,
                                          enum fpta_schema_item type)
{
    char uppercase[fpta_name_len_max];
    size_t i, len = strlen(name);

    for (i = 0; i < len && i < sizeof(uppercase); ++i)
        uppercase[i] = toupper(name[i]);

    fpta_shove_t shove = t1ha(uppercase, i, type) << fpta_name_hash_shift;
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

static void fpta_shove2str(fpta_shove_t shove, fpta_dbi_name *name)
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

static __hot MDB_dbi fpta_dbicache_lookup(fpta_db *db, fpta_shove_t shove)
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

static void fpta_dbicache_update(fpta_db *db, fpta_shove_t shove,
                                 MDB_dbi handle)
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

static void fpta_dbicache_remove(fpta_db *db, fpta_shove_t shove)
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

static __hot int fpta_dbi_open(fpta_txn *txn, fpta_shove_t shove,
                               MDB_dbi *handle, unsigned dbi_flags = 0)
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

int fpta_open_table(fpta_txn *txn, fpta_name *table_id)
{
    assert(fpta_id_validate(table_id, fpta_table));
    assert(table_id->mdbx_dbi < 1);

    fpta_shove_t dbi_shove = fpta_dbi_shove(table_id->shove, 0);
    int rc = fpta_dbi_open(txn, dbi_shove, &table_id->mdbx_dbi);
    if (unlikely(rc != FPTA_SUCCESS)) {
        assert(table_id->mdbx_dbi < 1);
        return rc;
    }

    assert(table_id->mdbx_dbi > 0);
    return mdbx_set_compare(txn->mdbx_txn, table_id->mdbx_dbi,
                            fpta_index_shove2comparator(table_id->table.pk));
}

int fpta_open_column(fpta_txn *txn, fpta_name *column_id)
{
    assert(fpta_id_validate(column_id, fpta_column));
    assert(column_id->mdbx_dbi < 1);

    fpta_name *table_id = column_id->column.table;
    if (unlikely(table_id->mdbx_dbi < 1)) {
        int rc = fpta_open_table(txn, table_id);
        if (unlikely(rc != FPTA_SUCCESS))
            return rc;
    }

    if (column_id->column.num == 0) {
        column_id->mdbx_dbi = table_id->mdbx_dbi;
        return FPTA_SUCCESS;
    }

    fpta_shove_t dbi_shove =
        fpta_dbi_shove(table_id->shove, column_id->column.num);
    int rc = fpta_dbi_open(txn, dbi_shove, &column_id->mdbx_dbi);
    if (unlikely(rc != FPTA_SUCCESS)) {
        assert(column_id->mdbx_dbi < 1);
        return rc;
    }

    assert(column_id->mdbx_dbi > 0);
    rc = mdbx_set_dupsort(txn->mdbx_txn, column_id->mdbx_dbi,
                          fpta_index_shove2comparator(table_id->table.pk));
    if (unlikely(rc != MDB_SUCCESS))
        return rc;
    return mdbx_set_compare(txn->mdbx_txn, column_id->mdbx_dbi,
                            fpta_index_shove2comparator(column_id->shove));
}

int fpta_open_secondaries(fpta_txn *txn, fpta_name *table_id,
                          MDB_dbi *dbi_array)
{
    assert(fpta_id_validate(table_id, fpta_table));
    assert(table_id->mdbx_dbi > 0);

    dbi_array[0] = table_id->mdbx_dbi;
    for (size_t i = 1; i < table_id->table.def->count; ++i) {
        unsigned index_shove = table_id->table.def->columns[i];
        if (fpta_shove2index(index_shove) == fpta_index_none)
            break;

        fpta_shove_t dbi_shove = fpta_dbi_shove(table_id->shove, i);
        int rc = fpta_dbi_open(txn, dbi_shove, &dbi_array[i]);
        if (unlikely(rc != FPTA_SUCCESS))
            return rc;

        assert(dbi_array[i] > 0);
        rc =
            mdbx_set_dupsort(txn->mdbx_txn, dbi_array[i],
                             fpta_index_shove2comparator(table_id->table.pk));
        if (unlikely(rc != MDB_SUCCESS))
            return rc;
        rc = mdbx_set_compare(txn->mdbx_txn, dbi_array[i],
                              fpta_index_shove2comparator(index_shove));
        if (unlikely(rc != MDB_SUCCESS))
            return rc;
    }

    return FPTA_SUCCESS;
}

//----------------------------------------------------------------------------

void fpta_column_set_init(fpta_column_set *column_set)
{
    column_set->count = 0;
    column_set->shoves[0] = 0;
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

    fpta_shove_t shove = fpta_column_shove(
        fpta_shove_name(column_name, fpta_column), data_type, index_type);
    assert(fpta_shove2index(shove) != (fpta_index_type)fpta_flag_table);

    for (size_t i = 0; i < column_set->count; ++i) {
        if (fpta_shove_eq(column_set->shoves[i], shove))
            return EEXIST;
    }

    if (index_type && fpta_index_is_primary(index_type)) {
        if (column_set->shoves[0])
            return EEXIST;
        column_set->shoves[0] = shove;
        if (column_set->count < 1)
            column_set->count = 1;
    } else {
        if (index_type != fpta_index_none && column_set->shoves[0] &&
            !fpta_index_is_unique(column_set->shoves[0]))
            return FPTA_EINVAL;
        size_t place = (column_set->count > 0) ? column_set->count : 1;
        column_set->shoves[place] = shove;
        column_set->count = place + 1;
    }

    return FPTA_SUCCESS;
}

static int fpta_column_def_validate(const fpta_shove_t *def, size_t count)
{
    if (unlikely(count < 1))
        return FPTA_EINVAL;
    if (unlikely(count > fpta_max_cols))
        return FPTA_TOOMANY;

    size_t index_count = 0;
    for (size_t i = 0; i < count; ++i) {
        fpta_shove_t shove = def[i];

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
        column_set->shoves + 1, column_set->shoves + column_set->count,
        [](const fpta_shove_t &left, const fpta_shove_t &right) {
            return fpta_shove2index(left) > fpta_shove2index(right);
        });

    return fpta_column_def_validate(column_set->shoves, column_set->count);
}

//----------------------------------------------------------------------------

bool fpta_schema_validate(const MDB_val def)
{
    if (unlikely(def.mv_size < fpta_table_schema_size(1)))
        return false;

    if (unlikely((def.mv_size - sizeof(fpta_table_schema)) %
                 sizeof(fpta_shove_t)))
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

static int fpta_schema_read(fpta_txn *txn, fpta_shove_t shove,
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
        id->shove = fpta_shove_name(name, fpta_table);
        id->table.pk = fpta_index_none | fptu_null;
        // id->table.def = nullptr;
        assert(fpta_id_validate(id, fpta_table));
        break;
    case fpta_column:
        id->shove = fpta_column_shove(fpta_shove_name(name, fpta_column),
                                      fptu_null, fpta_index_none);
        id->column.num = -1;
        id->column.table = id;
        assert(fpta_id_validate(id, fpta_column));
        break;
    }

    // id->mdbx_dbi = 0;
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

    column_id->column.table = const_cast<fpta_name *>(table_id);
    return FPTA_SUCCESS;
}

void fpta_name_destroy(fpta_name *id)
{
    if (fpta_id_validate(id, fpta_table))
        fpta_schema_free(id->table.def);
    memset(id, 0, sizeof(fpta_name));
}

int fpta_name_refresh(fpta_txn *txn, fpta_name *name_id)
{
    if (unlikely(name_id == nullptr))
        return FPTA_EINVAL;

    bool is_table =
        fpta_shove2index(name_id->shove) == (fpta_index_type)fpta_flag_table;
    if (is_table)
        return fpta_name_refresh_couple(txn, name_id, nullptr);

    return fpta_name_refresh_couple(txn, name_id->column.table, name_id);
}

int fpta_name_refresh_couple(fpta_txn *txn, fpta_name *table_id,
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

        int rc = fpta_schema_read(txn, table_id->shove, &table_id->table.def);
        if (unlikely(rc != FPTA_SUCCESS)) {
            if (rc != MDB_NOTFOUND)
                return rc;
            fpta_schema_free(table_id->table.def);
            table_id->table.def = nullptr;
        }

        fpta_table_schema *schema = table_id->table.def;
        assert(schema == nullptr || txn->schema_version == schema->version);
        table_id->version = txn->schema_version;

        table_id->table.pk =
            schema
                ? schema->columns[0] &
                      (fpta_column_typeid_mask | fpta_column_index_mask)
                : fpta_index_none | fptu_null;
    }

    if (unlikely(table_id->table.def == nullptr))
        return MDB_NOTFOUND;

    fpta_table_schema *schema = table_id->table.def;
    if (unlikely(schema->signature != FTPA_SCHEMA_SIGNATURE))
        return FPTA_SCHEMA_CORRUPTED;

    assert(fpta_shove2index(table_id->shove) ==
           (fpta_index_type)fpta_flag_table);
    if (unlikely(schema->shove != table_id->shove))
        return FPTA_SCHEMA_CORRUPTED;
    assert(table_id->version == schema->version);

    if (column_id == nullptr)
        return FPTA_SUCCESS;

    assert(fpta_shove2index(column_id->shove) !=
           (fpta_index_type)fpta_flag_table);

    if (unlikely(column_id->column.table != table_id)) {
        if (column_id->column.table != column_id)
            return FPTA_EINVAL;
        column_id->column.table = table_id;
    }

    if (unlikely(column_id->version > table_id->version))
        return FPTA_ETXNOUT;

    if (column_id->version != table_id->version) {
        column_id->column.num = -1;
        column_id->mdbx_dbi = 0;
        for (size_t i = 0; i < schema->count; ++i) {
            if (fpta_shove_eq(column_id->shove, schema->columns[i])) {
                column_id->shove = schema->columns[i];
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
    fpta_shove_t table_shove = fpta_shove_name(table_name, fpta_table);

    for (size_t i = 0; i < column_set->count; ++i) {
        auto index = fpta_shove2index(column_set->shoves[i]);
        if (index == fpta_index_none)
            break;
        assert(i < fpta_max_indexes);
        int err = fpta_dbi_open(txn, fpta_dbi_shove(table_shove, i), &dbi[i]);
        if (err != MDB_NOTFOUND)
            return EEXIST;
    }

    for (size_t i = 0; i < column_set->count; ++i) {
        auto index = fpta_shove2index(column_set->shoves[i]);
        if (index == fpta_index_none)
            break;
        unsigned dbi_flags =
            (i == 0)
                ? fpta_index_shove2primary_dbiflags(column_set->shoves[0])
                : fpta_index_shove2secondary_dbiflags(column_set->shoves[0],
                                                      column_set->shoves[i]);
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
    memcpy(def.columns, column_set->shoves, sizeof(fpta_shove_t) * def.count);
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
        if (unlikely(err != MDB_SUCCESS))
            return fpta_inconsistent_abort(txn, err);
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
    fpta_shove_t table_shove = fpta_shove_name(table_name, fpta_table);

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
        if (unlikely(err != MDB_SUCCESS))
            return fpta_inconsistent_abort(txn, err);
    }

    return rc;
}

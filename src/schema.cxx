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

__hot fpta_shove_t fpta_shove_name(const char *name,
                                   enum fpta_schema_item type) {
  char uppercase[fpta_name_len_max];
  size_t i, len = strlen(name);

  for (i = 0; i < len && i < sizeof(uppercase); ++i)
    uppercase[i] = (char)toupper(name[i]);

  fpta_shove_t shove = t1ha(uppercase, i, type) << fpta_name_hash_shift;
  if (type == fpta_table)
    shove |= fpta_flag_table;
  return shove;
}

template <bool first> static __inline bool is_valid_char4name(char c) {
  if (first ? isalpha(c) : isalnum(c))
    return true;
  if (c == '_')
    return true;
  if (FPTA_ALLOW_DOT4NAMES && c == '.')
    return true;

  return false;
}

bool fpta_validate_name(const char *name) {
  if (unlikely(name == nullptr))
    return false;

  if (unlikely(!is_valid_char4name<true>(name[0])))
    return false;

  size_t i = 1;
  while (name[i]) {
    if (unlikely(!is_valid_char4name<false>(name[i])))
      return false;
    if (unlikely(++i > fpta_name_len_max))
      return false;
  }

  if (unlikely(i < fpta_name_len_min))
    return false;

  return fpta_shove_name(name, fpta_column) > (1 << fpta_name_hash_shift);
}

//----------------------------------------------------------------------------

static int fpta_schema_open(fpta_txn *txn, bool create) {
  assert(fpta_txn_validate(txn, create ? fpta_schema : fpta_read) ==
         FPTA_SUCCESS);
  const fpta_shove_t key_shove =
      fpta_column_shove(0, fptu_uint64, fpta_primary_unique_ordered_obverse);
  const fpta_shove_t data_shove =
      fpta_column_shove(0, fptu_opaque, fpta_primary_unique_ordered_obverse);
  return fpta_dbi_open(txn, 0, txn->db->schema_dbi,
                       create ? MDBX_INTEGERKEY | MDBX_CREATE : MDBX_INTEGERKEY,
                       key_shove, data_shove, nullptr);
}

//----------------------------------------------------------------------------

static size_t fpta_schema_stored_size(fpta_column_set *column_set,
                                      const void *composites_end) {
  assert(column_set != nullptr);
  assert(column_set->count >= 1 && column_set->count <= fpta_max_cols);
  assert(&column_set->composites[0] <= composites_end &&
         FPT_ARRAY_END(column_set->composites) >= composites_end);

  return fpta_table_schema::header_size() +
         sizeof(fpta_shove_t) * column_set->count + (uintptr_t)composites_end -
         (uintptr_t)&column_set->composites[0];
}

static void fpta_schema_free(fpta_table_schema *def) {
  if (likely(def)) {
    def->_stored.signature = 0;
    def->_stored.checksum = ~def->_stored.checksum;
    def->_stored.count = 0;
    free(def);
  }
}

static int fpta_schema_clone(const fpta_shove_t schema_key,
                             const MDBX_val &schema_data,
                             fpta_table_schema **ptrdef) {
  assert(ptrdef != nullptr);
  const size_t payload_size =
      schema_data.iov_len - fpta_table_schema::header_size();

  const auto stored = (const fpta_table_stored_schema *)schema_data.iov_base;
  const size_t bytes =
      sizeof(fpta_table_schema) - sizeof(fpta_table_stored_schema::columns) +
      payload_size +
      stored->count * sizeof(fpta_table_schema::composite_item_t);

  fpta_table_schema *schema = (fpta_table_schema *)realloc(*ptrdef, bytes);
  if (unlikely(schema == nullptr))
    return FPTA_ENOMEM;

  *ptrdef = schema;
  memset(schema, ~0, bytes);
  memcpy(&schema->_stored, schema_data.iov_base, schema_data.iov_len);
  fpta_table_schema::composite_item_t *const offsets =
      (fpta_table_schema::composite_item_t *)((uint8_t *)schema + bytes) -
      schema->_stored.count;
  schema->_key = schema_key;
  schema->_composite_offsets = offsets;

  const auto composites_begin =
      (const fpta_table_schema::composite_item_t *)&schema->_stored
          .columns[schema->_stored.count];
  const auto composites_end = schema->_composite_offsets;
  auto composites = composites_begin;
  for (size_t i = 0; i < schema->_stored.count; ++i) {
    const fpta_shove_t column_shove = schema->_stored.columns[i];
    if (!fpta_is_indexed(column_shove))
      break;
    if (!fpta_is_composite(column_shove))
      continue;
    if (unlikely(composites >= composites_end || *composites == 0))
      return FPTA_EOOPS;

    const auto first = composites + 1;
    const auto last = first + *composites;
    if (unlikely(last > composites_end))
      return FPTA_EOOPS;

    const ptrdiff_t distance = composites - composites_begin;
    assert(distance >= 0 && distance <= fpta_max_cols);
    offsets[i] = (fpta_table_schema::composite_item_t)distance;
    composites = last;
  }
  return FPTA_SUCCESS;
}

/* int fpta_table_schema::composite_list(
    size_t number, fpta_table_schema::composite_iter_t &list_begin,
    fpta_table_schema::composite_iter_t &list_end) const {
  auto composites =
      (const composite_item_t *)&this->_stored.columns[this->_stored.count];
  const auto composites_end = this->_composite_offsets;
  for (size_t i = 0; i < column_count(); ++i) {
    const fpta_shove_t column_shove = this->_stored.columns[i];
    if (!fpta_is_indexed(column_shove))
      break;
    if (!fpta_is_composite(column_shove))
      continue;
    if (unlikely(composites >= composites_end || *composites == 0))
      return FPTA_SCHEMA_CORRUPTED;

    const auto first = composites + 1;
    const auto last = first + *composites;
    if (unlikely(last > composites_end))
      return FPTA_SCHEMA_CORRUPTED;

    if (i == number) {
      list_begin = first;
      list_end = last;
      return FPTA_SUCCESS;
    }
    composites = last;
  }

  return FPTA_EOOPS;
} */

static __inline int weight(const fpta_shove_t index) {
  if (fpta_is_indexed(index))
    return fpta_index_is_primary(index) ? 3 : 2;
  if (index & fpta_index_fnullable)
    return 1;
  return 0;
}

static bool compare(const fpta_shove_t &left, const fpta_shove_t &right) {
  const auto left_weight = weight(left);
  const auto rigth_weight = weight(right);
  return left_weight > rigth_weight ||
         (left_weight == rigth_weight && left < right);
}

static bool fpta_check_indextype(const fpta_index_type index_type) {
  switch (index_type) {
  default:
    return false;

  case fpta_primary_withdups_ordered_obverse:
  case fpta_primary_withdups_ordered_obverse_nullable:
  case fpta_primary_withdups_ordered_reverse:
  case fpta_primary_withdups_ordered_reverse_nullable:

  case fpta_primary_unique_ordered_obverse:
  case fpta_primary_unique_ordered_obverse_nullable:
  case fpta_primary_unique_ordered_reverse:
  case fpta_primary_unique_ordered_reverse_nullable:

  case fpta_primary_unique_unordered:
  case fpta_primary_unique_unordered_nullable_obverse:
  case fpta_primary_unique_unordered_nullable_reverse:

  case fpta_primary_withdups_unordered:
  case fpta_primary_withdups_unordered_nullable_obverse:
  /* fpta_primary_withdups_unordered_nullable_reverse = НЕДОСТУПЕН,
   * так как битовая коминация совпадает с fpta_noindex_nullable */

  case fpta_secondary_withdups_ordered_obverse:
  case fpta_secondary_withdups_ordered_obverse_nullable:
  case fpta_secondary_withdups_ordered_reverse:
  case fpta_secondary_withdups_ordered_reverse_nullable:

  case fpta_secondary_unique_ordered_obverse:
  case fpta_secondary_unique_ordered_obverse_nullable:
  case fpta_secondary_unique_ordered_reverse:
  case fpta_secondary_unique_ordered_reverse_nullable:

  case fpta_secondary_unique_unordered:
  case fpta_secondary_unique_unordered_nullable_obverse:
  case fpta_secondary_unique_unordered_nullable_reverse:

  case fpta_secondary_withdups_unordered:
  case fpta_secondary_withdups_unordered_nullable_obverse:
  case fpta_secondary_withdups_unordered_nullable_reverse:
  // fall through
  case fpta_index_none:
  case fpta_noindex_nullable:
    return true;
  }
}

static int fpta_schema_validate(
    const fpta_shove_t *shoves, size_t shoves_count,
    const fpta_table_schema::composite_item_t *const composites_begin,
    const fpta_table_schema::composite_item_t *const composites_detent,
    const void **composites_eof = nullptr) {
  if (unlikely(shoves_count < 1))
    return FPTA_EINVAL;
  if (unlikely(shoves_count > fpta_max_cols))
    return FPTA_SCHEMA_CORRUPTED;

  if (unlikely(composites_begin > composites_detent ||
               fpta_is_intersected(shoves, shoves + shoves_count,
                                   composites_begin, composites_detent)))
    return FPTA_SCHEMA_CORRUPTED;

  size_t index_count = 0;
  auto composites = composites_begin;
  for (size_t i = 0; i < shoves_count; ++i) {
    const fpta_shove_t shove = shoves[i];
    const fpta_index_type index_type = fpta_shove2index(shove);
    if (!fpta_check_indextype(index_type))
      return FPTA_EINVAL;

    if ((i == 0) !=
        (fpta_is_indexed(index_type) && fpta_index_is_primary(index_type)))
      /* первичный индекс обязан быть, только один и только в самом начале */
      return FPTA_EINVAL;

    if (fpta_index_is_secondary(index_type) && !fpta_index_is_unique(shoves[0]))
      /* для вторичных индексов первичный ключ должен быть уникальным */
      return FPTA_EINVAL;

    if (fpta_is_indexed(index_type) && ++index_count > fpta_max_indexes)
      return FPTA_TOOMANY;
    assert((index_type & fpta_column_index_mask) == index_type);
    assert(index_type != (fpta_index_type)fpta_flag_table);

    const fptu_type data_type = fpta_shove2type(shove);
    if (data_type > fptu_nested) {
      if (data_type == (fptu_null | fptu_farray))
        return FPTA_ETYPE;
      /* support indexes for arrays */
      if (fpta_is_indexed(index_type))
        return FPTA_EINVAL;
    } else {
      if (data_type == /* composite */ fptu_null) {
        if (unlikely(!fpta_is_indexed(index_type)))
          return FPTA_EINVAL;
        if (unlikely(composites >= composites_detent || *composites == 0))
          return FPTA_SCHEMA_CORRUPTED;

        const auto first = composites + 1;
        const auto last = first + *composites;
        if (unlikely(last > composites_detent))
          return FPTA_SCHEMA_CORRUPTED;

        composites = last;
        int rc = fpta_composite_index_validate(index_type, first, last, shoves,
                                               shoves_count, composites_begin,
                                               composites_detent, shove);
        if (rc != FPTA_SUCCESS)
          return rc;
      } else {
        if (unlikely(data_type < fptu_uint16 || data_type > fptu_nested))
          return FPTA_EINVAL;
        if (fpta_is_indexed(index_type) && fpta_index_is_reverse(index_type) &&
            (fpta_index_is_unordered(index_type) || data_type < fptu_96) &&
            !(fpta_index_is_nullable(index_type) &&
              fpta_nullable_reverse_sensitive(data_type)))
          return FPTA_EINVAL;
      }
    }

    for (size_t j = 0; j < i; ++j)
      if (fpta_shove_eq(shove, shoves[j]))
        return FPTA_EEXIST;
  }

  if (composites_eof)
    *composites_eof = composites;

  return FPTA_SUCCESS;
}

static int fpta_schema_sort(fpta_column_set *column_set) {
  assert(column_set != nullptr && column_set->count > 0 &&
         column_set->count <= fpta_max_cols);
  if (std::is_sorted(column_set->shoves, column_set->shoves + column_set->count,
                     [](const fpta_shove_t &left, const fpta_shove_t &right) {
                       return compare(left, right);
                     }))
    return FPTA_SUCCESS;

  std::vector<fpta_shove_t> sorted(column_set->shoves,
                                   column_set->shoves + column_set->count);
  /* sort descriptions of columns, so that a non-indexed was at the end */
  std::sort(sorted.begin(), sorted.end(),
            [](const fpta_shove_t &left, const fpta_shove_t &right) {
              return compare(left, right);
            });

  /* fixup composites after sort */
  std::vector<fpta_table_schema::composite_item_t> fixup;
  fixup.reserve(column_set->count);
  auto composites = column_set->composites;
  for (size_t i = 0; i < column_set->count; ++i) {
    const fpta_shove_t column_shove = column_set->shoves[i];
    if (!fpta_is_composite(column_shove))
      continue;
    if (unlikely(!fpta_is_indexed(column_shove) ||
                 composites >= FPT_ARRAY_END(column_set->composites) ||
                 *composites == 0))
      return FPTA_SCHEMA_CORRUPTED;

    const auto first = composites + 1;
    const auto last = first + *composites;
    if (unlikely(last > FPT_ARRAY_END(column_set->composites)))
      return FPTA_SCHEMA_CORRUPTED;

    fixup.push_back(*composites);
    composites = last;
    for (auto scan = first; scan < last; ++scan) {
      const size_t column_number = *scan;
      if (unlikely(column_number >= column_set->count))
        return FPTA_SCHEMA_CORRUPTED;
      if (unlikely(std::find(first, scan, column_number) != scan))
        return FPTA_EEXIST;

      const auto renum = std::distance(
          sorted.begin(), std::find(sorted.begin(), sorted.end(),
                                    column_set->shoves[column_number]));
      if (unlikely(renum < 0 || renum >= column_set->count))
        return FPTA_EOOPS;

      fixup.push_back(static_cast<fpta_table_schema::composite_item_t>(renum));
    }
  }

  /* put sorted arrays */
  memset(column_set->shoves, 0, sizeof(column_set->shoves));
  memset(column_set->composites, 0, sizeof(column_set->composites));
  std::copy(sorted.begin(), sorted.end(), column_set->shoves);
  std::copy(fixup.begin(), fixup.end(), column_set->composites);

  /* final checking */
  return fpta_schema_validate(column_set->shoves, column_set->count,
                              column_set->composites,
                              FPT_ARRAY_END(column_set->composites));
}

int fpta_schema_add(fpta_column_set *column_set, const char *id_name,
                    fptu_type data_type, fpta_index_type index_type) {
  assert(id_name != nullptr);
  if (!fpta_check_indextype(index_type))
    return FPTA_EINVAL;

  assert((index_type & fpta_column_index_mask) == index_type);
  assert(index_type != (fpta_index_type)fpta_flag_table);

  if (unlikely(column_set == nullptr || column_set->count > fpta_max_cols))
    return FPTA_EINVAL;

  const fpta_shove_t shove = fpta_column_shove(
      fpta_shove_name(id_name, fpta_column), data_type, index_type);
  assert(fpta_shove2index(shove) != (fpta_index_type)fpta_flag_table);

  for (size_t i = 0; i < column_set->count; ++i) {
    if (fpta_shove_eq(column_set->shoves[i], shove))
      return FPTA_EEXIST;
  }

  if (fpta_is_indexed(index_type) && fpta_index_is_primary(index_type)) {
    if (column_set->shoves[0])
      return FPTA_EEXIST;
    column_set->shoves[0] = shove;
    if (column_set->count < 1)
      column_set->count = 1;
  } else {
    if (fpta_index_is_secondary(index_type) && column_set->shoves[0] &&
        !fpta_index_is_unique(column_set->shoves[0]))
      return FPTA_EINVAL;
    if (unlikely(column_set->count == fpta_max_cols))
      return FPTA_TOOMANY;
    size_t place = (column_set->count > 0) ? column_set->count : 1;
    column_set->shoves[place] = shove;
    column_set->count = (unsigned)place + 1;
  }

  return FPTA_SUCCESS;
}

static bool fpta_schema_validate(const fpta_shove_t schema_key,
                                 const MDBX_val &schema_data) {
  if (unlikely(schema_data.iov_len < sizeof(fpta_table_stored_schema)))
    return false;

  if (unlikely((schema_data.iov_len - sizeof(fpta_table_stored_schema)) %
               std::min(sizeof(fpta_shove_t),
                        sizeof(fpta_table_schema::composite_item_t))))
    return false;

  const fpta_table_stored_schema *schema =
      (const fpta_table_stored_schema *)schema_data.iov_base;
  if (unlikely(schema->signature != FTPA_SCHEMA_SIGNATURE))
    return false;

  if (unlikely(schema->count < 1 || schema->count > fpta_max_cols))
    return false;

  if (unlikely(schema_data.iov_len < fpta_table_schema::header_size() +
                                         sizeof(fpta_shove_t) * schema->count))
    return false;

  if (unlikely(schema->csn == 0))
    return false;

  if (unlikely(fpta_shove2index(schema_key) !=
               (fpta_index_type)fpta_flag_table))
    return false;

  uint64_t checksum =
      t1ha(&schema->signature, schema_data.iov_len - sizeof(checksum),
           FTPA_SCHEMA_CHECKSEED);
  if (unlikely(checksum != schema->checksum))
    return false;

  const void *const composites_begin = schema->columns + schema->count;
  const void *const composites_end =
      (uint8_t *)schema_data.iov_base + schema_data.iov_len;
  if (FPTA_SUCCESS !=
      fpta_schema_validate(
          schema->columns, schema->count,
          (const fpta_table_schema::composite_item_t *)composites_begin,
          (const fpta_table_schema::composite_item_t *)composites_end))
    return false;

  return std::is_sorted(
      schema->columns, schema->columns + schema->count,
      [](const fpta_shove_t &left, const fpta_shove_t &right) {
        return compare(left, right);
      });
}

static int fpta_schema_read(fpta_txn *txn, fpta_shove_t schema_key,
                            fpta_table_schema **def) {
  assert(fpta_txn_validate(txn, fpta_read) == FPTA_SUCCESS && def);

  int rc;
  fpta_db *db = txn->db;
  if (db->schema_dbi < 1) {
    rc = fpta_schema_open(txn, false);
    if (rc != MDBX_SUCCESS)
      return rc;
  }

  MDBX_val mdbx_data, mdbx_key;
  mdbx_key.iov_len = sizeof(schema_key);
  mdbx_key.iov_base = &schema_key;
  rc = mdbx_get(txn->mdbx_txn, db->schema_dbi, &mdbx_key, &mdbx_data);
  if (rc != MDBX_SUCCESS)
    return rc;

  if (!fpta_schema_validate(schema_key, mdbx_data))
    return FPTA_SCHEMA_CORRUPTED;

  return fpta_schema_clone(schema_key, mdbx_data, def);
}

//----------------------------------------------------------------------------

int fpta_column_describe(const char *column_name, fptu_type data_type,
                         fpta_index_type index_type,
                         fpta_column_set *column_set) {
  if (unlikely(!fpta_validate_name(column_name)))
    return FPTA_EINVAL;

  if (unlikely(data_type < fptu_uint16 || data_type > fptu_nested))
    return FPTA_EINVAL;

  if (fpta_is_indexed(index_type) && fpta_index_is_reverse(index_type) &&
      (fpta_index_is_unordered(index_type) || data_type < fptu_96) &&
      !(fpta_index_is_nullable(index_type) &&
        fpta_nullable_reverse_sensitive(data_type)))
    return FPTA_EINVAL;

  return fpta_schema_add(column_set, column_name, data_type, index_type);
}

int fpta_column_set_validate(fpta_column_set *column_set) {
  if (column_set == nullptr)
    return FPTA_EINVAL;

  return fpta_schema_validate(column_set->shoves, column_set->count,
                              column_set->composites,
                              FPT_ARRAY_END(column_set->composites));
}

int fpta_schema_fetch(fpta_txn *txn, fpta_schema_info *info) {
  if (!info)
    return FPTA_EINVAL;
  memset(info, 0, sizeof(fpta_schema_info));

  int rc = fpta_txn_validate(txn, fpta_read);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  fpta_db *db = txn->db;
  if (db->schema_dbi < 1) {
    rc = fpta_schema_open(txn, false);
    if (rc != MDBX_SUCCESS)
      return rc;
  }

  MDBX_cursor *mdbx_cursor;
  rc = mdbx_cursor_open(txn->mdbx_txn, db->schema_dbi, &mdbx_cursor);
  if (rc != MDBX_SUCCESS)
    return rc;

  MDBX_val mdbx_data, mdbx_key;
  rc = mdbx_cursor_get(mdbx_cursor, &mdbx_key, &mdbx_data, MDBX_FIRST);
  while (rc == MDBX_SUCCESS) {
    if (info->tables_count >= fpta_tables_max) {
      rc = FPTA_SCHEMA_CORRUPTED;
      break;
    }

    fpta_name *id = &info->tables_names[info->tables_count];
    if (mdbx_key.iov_len != sizeof(fpta_shove_t)) {
      rc = FPTA_SCHEMA_CORRUPTED;
      break;
    }

    memcpy(&id->shove, mdbx_key.iov_base, sizeof(id->shove));
    // id->table_schema = nullptr; /* done by memset() */
    assert(id->table_schema == nullptr);

    rc = fpta_id_validate(id, fpta_table);
    if (rc != FPTA_SUCCESS)
      break;

    if (!fpta_schema_validate(id->shove, mdbx_data)) {
      rc = FPTA_SCHEMA_CORRUPTED;
      break;
    }

    info->tables_count += 1;
    rc = mdbx_cursor_get(mdbx_cursor, &mdbx_key, &mdbx_data, MDBX_NEXT);
  }

  mdbx_cursor_close(mdbx_cursor);
  return (rc == MDBX_NOTFOUND) ? (int)FPTA_SUCCESS : rc;
}

int fpta_schema_destroy(fpta_schema_info *info) {
  if (unlikely(info == nullptr || info->tables_count == FPTA_DEADBEEF))
    return FPTA_EINVAL;

  for (size_t i = 0; i < info->tables_count; i++)
    fpta_name_destroy(info->tables_names + i);
  info->tables_count = (unsigned)FPTA_DEADBEEF;

  return FPTA_SUCCESS;
}

//----------------------------------------------------------------------------

static int fpta_name_init(fpta_name *id, const char *name,
                          fpta_schema_item schema_item) {
  if (unlikely(id == nullptr))
    return FPTA_EINVAL;

  memset(id, 0, sizeof(fpta_name));
  if (unlikely(!fpta_validate_name(name)))
    return FPTA_EINVAL;

  switch (schema_item) {
  default:
    return FPTA_EINVAL;
  case fpta_table:
    id->shove = fpta_shove_name(name, fpta_table);
    // id->table_schema = nullptr; /* done by memset() */
    assert(id->table_schema == nullptr);
    assert(fpta_id_validate(id, fpta_table) == FPTA_SUCCESS);
    break;
  case fpta_column:
    id->shove = fpta_column_shove(fpta_shove_name(name, fpta_column), fptu_null,
                                  fpta_index_none);
    id->column.num = ~0u;
    id->column.table = id;
    assert(fpta_id_validate(id, fpta_column) == FPTA_SUCCESS);
    break;
  }

  // id->version = 0; /* done by memset() */
  return FPTA_SUCCESS;
}

int fpta_table_init(fpta_name *table_id, const char *name) {
  return fpta_name_init(table_id, name, fpta_table);
}

int fpta_column_init(const fpta_name *table_id, fpta_name *column_id,
                     const char *name) {
  int rc = fpta_id_validate(table_id, fpta_table);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  rc = fpta_name_init(column_id, name, fpta_column);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  column_id->column.table = const_cast<fpta_name *>(table_id);
  return FPTA_SUCCESS;
}

void fpta_name_destroy(fpta_name *id) {
  if (fpta_id_validate(id, fpta_table) == FPTA_SUCCESS)
    fpta_schema_free(id->table_schema);
  memset(id, 0, sizeof(fpta_name));
}

int fpta_name_refresh(fpta_txn *txn, fpta_name *name_id) {
  if (unlikely(name_id == nullptr))
    return FPTA_EINVAL;

  const bool is_table =
      fpta_shove2index(name_id->shove) == (fpta_index_type)fpta_flag_table;
  if (is_table)
    return fpta_name_refresh_couple(txn, name_id, nullptr);

  return fpta_name_refresh_couple(txn, name_id->column.table, name_id);
}

int fpta_name_refresh_couple(fpta_txn *txn, fpta_name *table_id,
                             fpta_name *column_id) {
  int rc = fpta_id_validate(table_id, fpta_table);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;
  if (column_id) {
    rc = fpta_id_validate(column_id, fpta_column);
    if (unlikely(rc != FPTA_SUCCESS))
      return rc;
  }
  rc = fpta_txn_validate(txn, fpta_read);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (unlikely(table_id->version > txn->schema_version()))
    return FPTA_SCHEMA_CHANGED;

  if (unlikely(table_id->version != txn->schema_version())) {
    if (table_id->table_schema) {
      rc = fpta_dbi_close(txn, table_id->shove,
                          &table_id->table_schema->handle_cache(0));
      if (unlikely(rc != FPTA_SUCCESS))
        return rc;
      for (size_t i = 1; i < table_id->table_schema->column_count(); ++i) {
        const fpta_shove_t shove = table_id->table_schema->column_shove(i);
        if (!fpta_is_indexed(shove))
          break;

        const fpta_shove_t dbi_shove = fpta_dbi_shove(table_id->shove, i);
        rc = fpta_dbi_close(txn, dbi_shove,
                            &table_id->table_schema->handle_cache(i));
        if (unlikely(rc != FPTA_SUCCESS))
          return rc;
      }
    }

    rc = fpta_schema_read(txn, table_id->shove, &table_id->table_schema);
    if (unlikely(rc != FPTA_SUCCESS)) {
      if (rc != MDBX_NOTFOUND)
        return rc;
      fpta_schema_free(table_id->table_schema);
      table_id->table_schema = nullptr;
    }

    assert(table_id->table_schema == nullptr ||
           txn->schema_version() >= table_id->table_schema->version_csn());
    table_id->version = txn->schema_version();
  }

  if (unlikely(table_id->table_schema == nullptr))
    return MDBX_NOTFOUND;

  fpta_table_schema *schema = table_id->table_schema;
  if (unlikely(schema->signature() != FTPA_SCHEMA_SIGNATURE))
    return FPTA_SCHEMA_CORRUPTED;

  assert(fpta_shove2index(table_id->shove) == (fpta_index_type)fpta_flag_table);
  if (unlikely(schema->table_shove() != table_id->shove))
    return FPTA_SCHEMA_CORRUPTED;

  assert(table_id->version >= schema->version_csn());
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
    return FPTA_SCHEMA_CHANGED;

  if (column_id->version != table_id->version) {
    column_id->column.num = ~0u;
    for (size_t i = 0; i < schema->column_count(); ++i) {
      if (fpta_shove_eq(column_id->shove, schema->column_shove(i))) {
        column_id->shove = schema->column_shove(i);
        column_id->column.num = (unsigned)i;
        break;
      }
    }
    column_id->version = table_id->version;
  }

  if (unlikely(column_id->column.num > fpta_max_cols))
    return FPTA_ENOENT;
  return FPTA_SUCCESS;
}

//----------------------------------------------------------------------------

int fpta_table_create(fpta_txn *txn, const char *table_name,
                      fpta_column_set *column_set) {
  int rc = fpta_txn_validate(txn, fpta_schema);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;
  if (!fpta_validate_name(table_name))
    return FPTA_EINVAL;

  const void *composites_eof = nullptr;
  rc = fpta_schema_validate(
      column_set->shoves, column_set->count, column_set->composites,
      FPT_ARRAY_END(column_set->composites), &composites_eof);
  if (rc != FPTA_SUCCESS)
    return rc;
  const size_t bytes = fpta_schema_stored_size(column_set, composites_eof);

  rc = fpta_schema_sort(column_set);
  if (rc != FPTA_SUCCESS)
    return rc;

  fpta_db *db = txn->db;
  if (db->schema_dbi < 1) {
    rc = fpta_schema_open(txn, true);
    if (rc != MDBX_SUCCESS)
      return rc;
  }

  MDBX_dbi dbi[fpta_max_indexes];
  memset(dbi, 0, sizeof(dbi));
  fpta_shove_t table_shove = fpta_shove_name(table_name, fpta_table);

  for (size_t i = 0; i < column_set->count; ++i) {
    const auto shove = column_set->shoves[i];
    if (!fpta_is_indexed(shove))
      break;
    assert(i < fpta_max_indexes);

    const unsigned dbi_flags = fpta_dbi_flags(column_set->shoves, i);
    const fpta_shove_t data_shove = fpta_data_shove(column_set->shoves, i);
    int err = fpta_dbi_open(txn, fpta_dbi_shove(table_shove, i), dbi[i],
                            dbi_flags, shove, data_shove, nullptr);
    if (err != MDBX_NOTFOUND)
      return FPTA_EEXIST;
  }

  for (size_t i = 0; i < column_set->count; ++i) {
    const auto shove = column_set->shoves[i];
    if (!fpta_is_indexed(shove))
      break;
    assert(i < fpta_max_indexes);

    const unsigned dbi_flags =
        MDBX_CREATE | fpta_dbi_flags(column_set->shoves, i);
    const fpta_shove_t data_shove = fpta_data_shove(column_set->shoves, i);
    rc = fpta_dbi_open(txn, fpta_dbi_shove(table_shove, i), dbi[i], dbi_flags,
                       shove, data_shove, nullptr);
    if (rc != MDBX_SUCCESS)
      goto bailout;
  }

  MDBX_val key, data;
  key.iov_len = sizeof(table_shove);
  key.iov_base = &table_shove;
  data.iov_base = nullptr;
  data.iov_len = bytes;
  rc = mdbx_put(txn->mdbx_txn, db->schema_dbi, &key, &data,
                MDBX_NOOVERWRITE | MDBX_RESERVE);
  if (rc == MDBX_SUCCESS) {
    fpta_table_stored_schema *const record =
        (fpta_table_stored_schema *)data.iov_base;
    record->signature = FTPA_SCHEMA_SIGNATURE;
    record->count = column_set->count;
    record->csn = txn->db_version;
    memcpy(record->columns, column_set->shoves,
           sizeof(fpta_shove_t) * record->count);

    fpta_table_schema::composite_item_t *ptr =
        (fpta_table_schema::composite_item_t *)&record->columns[record->count];
    const size_t composites_bytes =
        (uintptr_t)composites_eof - (uintptr_t)&column_set->composites[0];
    memcpy(ptr, column_set->composites, composites_bytes);
    assert((uint8_t *)ptr + composites_bytes == (uint8_t *)record + bytes);

    record->checksum =
        t1ha(&record->signature, bytes - sizeof(record->checksum),
             FTPA_SCHEMA_CHECKSEED);
    assert(fpta_schema_validate(table_shove, data));

    txn->schema_version() = txn->db_version;
    return FPTA_SUCCESS;
  }

bailout:
  for (size_t i = 0; i < fpta_max_indexes && dbi[i] > 0; ++i) {
    fpta_dbicache_remove(db, fpta_dbi_shove(table_shove, i));
    int err = mdbx_drop(txn->mdbx_txn, dbi[i], true);
    if (unlikely(err != MDBX_SUCCESS))
      return fpta_internal_abort(txn, err);
  }
  return rc;
}

int fpta_table_drop(fpta_txn *txn, const char *table_name) {
  int rc = fpta_txn_validate(txn, fpta_schema);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;
  if (!fpta_validate_name(table_name))
    return FPTA_EINVAL;

  fpta_db *db = txn->db;
  if (db->schema_dbi < 1) {
    rc = fpta_schema_open(txn, true);
    if (rc != MDBX_SUCCESS)
      return rc;
  }

  MDBX_dbi dbi[fpta_max_indexes];
  memset(dbi, 0, sizeof(dbi));
  fpta_shove_t schema_key = fpta_shove_name(table_name, fpta_table);

  MDBX_val data, key;
  key.iov_len = sizeof(schema_key);
  key.iov_base = &schema_key;
  rc = mdbx_get(txn->mdbx_txn, db->schema_dbi, &key, &data);
  if (rc != MDBX_SUCCESS)
    return rc;

  if (!fpta_schema_validate(schema_key, data))
    return FPTA_SCHEMA_CORRUPTED;

  const fpta_table_stored_schema *const stored_schema =
      (const fpta_table_stored_schema *)data.iov_base;
  for (size_t i = 0; i < stored_schema->count; ++i) {
    const auto shove = stored_schema->columns[i];
    if (!fpta_is_indexed(shove))
      break;
    assert(i < fpta_max_indexes);

    const unsigned dbi_flags = fpta_dbi_flags(stored_schema->columns, i);
    const fpta_shove_t data_shove = fpta_data_shove(stored_schema->columns, i);
    rc = fpta_dbi_open(txn, fpta_dbi_shove(schema_key, i), dbi[i], dbi_flags,
                       shove, data_shove, nullptr);
    if (rc != MDBX_SUCCESS && rc != MDBX_NOTFOUND)
      return rc;
  }

  rc = mdbx_del(txn->mdbx_txn, db->schema_dbi, &key, nullptr);
  if (rc != MDBX_SUCCESS)
    return rc;

  txn->schema_version() = txn->db_version;
  for (size_t i = 0; i < stored_schema->count; ++i) {
    if (dbi[i] > 0) {
      fpta_dbicache_remove(db, fpta_dbi_shove(schema_key, i));
      int err = mdbx_drop(txn->mdbx_txn, dbi[i], true);
      if (unlikely(err != MDBX_SUCCESS))
        return fpta_internal_abort(txn, err);
    }
  }

  return rc;
}

//----------------------------------------------------------------------------

int fpta_table_column_count_ex(const fpta_name *table_id,
                               unsigned *total_columns,
                               unsigned *composite_count) {
  int rc = fpta_id_validate(table_id, fpta_table_with_schema);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  const fpta_table_schema *schema = table_id->table_schema;
  if (likely(total_columns))
    *total_columns = schema->column_count();
  if (composite_count) {
    unsigned count = 0;
    for (size_t i = 0; i < schema->column_count(); ++i) {
      const auto shove = schema->column_shove(i);
      assert(i < fpta_max_indexes);
      if (!fpta_index_is_secondary(shove))
        break;
      if (fpta_is_composite(shove))
        ++count;
    }
    *composite_count = count;
  }

  return FPTA_SUCCESS;
}

int fpta_table_column_get(const fpta_name *table_id, unsigned column,
                          fpta_name *column_id) {
  if (unlikely(column_id == nullptr))
    return FPTA_EINVAL;
  memset(column_id, 0, sizeof(fpta_name));

  int rc = fpta_id_validate(table_id, fpta_table_with_schema);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  const fpta_table_schema *schema = table_id->table_schema;
  if (column >= schema->column_count())
    return FPTA_NODATA;
  column_id->column.table = const_cast<fpta_name *>(table_id);
  column_id->shove = schema->column_shove(column);
  column_id->column.num = column;
  column_id->version = table_id->version;

  assert(fpta_id_validate(column_id, fpta_column_with_schema) == FPTA_SUCCESS);
  return FPTA_SUCCESS;
}

int fpta_name_reset(fpta_name *name_id) {
  if (unlikely(name_id == nullptr))
    return FPTA_EINVAL;

  name_id->version = 0;
  return FPTA_SUCCESS;
}

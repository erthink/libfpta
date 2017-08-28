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
#include <cstdarg>

// #define DONT_USE_BITSET
#ifndef DONT_USE_BITSET
#include <bitset>
#endif

static __inline uint64_t add_rotate_xor(uint64_t base, uint64_t addend,
                                        unsigned shift = 19) {
  uint64_t rotated = (base << shift) | (base >> (64 - shift));
  return (base + addend) ^ rotated;
}

typedef int (*concat_column_t)(fpta_key &key, const bool alternale_nils,
                               const fpta_table_schema *const schema,
                               const fptu_ro &row, unsigned column);

static int concat_unordered(fpta_key &key, const bool alternale_nils,
                            const fpta_table_schema *const schema,
                            const fptu_ro &row, unsigned column) {
  const uint64_t MARKER_ABSENT = UINT64_C(0x974BC764BAC4C7F);
  uint64_t *const hash = (uint64_t *)key.mdbx.iov_base;
  const fpta_shove_t shove = schema->column_shove(column);
  const fptu_type type = fpta_shove2type(shove);
  const fptu_field *field = fptu_lookup_ro(row, column, type);
  if (unlikely(field == nullptr)) {
    const fpta_index_type index = fpta_shove2index(shove);
    if (unlikely(!fpta_index_is_nullable(index)))
      return FPTA_COLUMN_MISSING;
    if (!alternale_nils)
      /* add absent-marker to resulting hash */
      *hash = add_rotate_xor(*hash, MARKER_ABSENT);
  } else {
    const struct iovec iov = fptu_field_as_iovec(field);
    /* add value to resulting hash */
    *hash = t1ha1_le(iov.iov_base, iov.iov_len, *hash + field->ct);
  }

  return FPTA_SUCCESS;
}

static int concat_bytes(fpta_key &key, const void *data, size_t length) {
  uint64_t *const hash = (uint64_t *)key.mdbx.iov_base;
  assert(hash == &key.place.longkey_obverse.tailhash ||
         hash == &key.place.longkey_reverse.headhash);
  const bool obverse = hash == &key.place.longkey_obverse.tailhash;

  if (key.mdbx.iov_len < fpta_max_keylen) {
    const size_t left = fpta_max_keylen - key.mdbx.iov_len;
    const size_t chunk = (left >= length) ? length : left;

    if (obverse) {
      /* append bytes to the key */
      uint8_t *obverse_append =
          ((uint8_t *)&key.place.longkey_obverse.head) + key.mdbx.iov_len;
      memcpy(obverse_append, data, chunk);
      /* update pointer to the end of a chunk */
      data = (const uint8_t *)data + chunk;
    } else {
      /* put bytes ahead of the key */
      uint8_t *reverse_ahead = ((uint8_t *)&key.place.longkey_reverse.tail) +
                               sizeof(key.place.longkey_reverse.tail) -
                               key.mdbx.iov_len;
      memcpy(reverse_ahead - chunk, (const uint8_t *)data + length - chunk,
             chunk);
    }

    length -= chunk;
    if (length == 0) {
      key.mdbx.iov_len += chunk;
      return FPTA_SUCCESS;
    } else {
      /* Limit for key-size reached,
       * continue hashing all of the rest.
       * Initialize hash value */
      *hash = 0;
    }

    /* Now key includes hash-value. */
    key.mdbx.iov_len = sizeof(key.place);
  }

  assert(key.mdbx.iov_len == fpta_max_keylen + 8);
  /* add bytes to hash */
  *hash = t1ha1_le(data, length, *hash);
  return FPTA_SUCCESS;
}

static int concat_ordered(fpta_key &key, const bool alternale_nils,
                          const fpta_table_schema *const schema,
                          const fptu_ro &row, unsigned column) {
  const fpta_shove_t shove = schema->column_shove(column);
  const fptu_type type = fpta_shove2type(shove);
  const fpta_index_type index = fpta_shove2index(shove);
  const fptu_field *field = fptu_lookup_ro(row, column, type);

  const uint8_t prefix_absent = 0;
  const uint8_t prefix_present = 42;
  const bool obverse = key.mdbx.iov_base == &key.place.longkey_obverse.tailhash;

  if (unlikely(field == nullptr)) {
    if (unlikely(!fpta_index_is_nullable(index)))
      return FPTA_COLUMN_MISSING;
    if (alternale_nils || type >= fptu_cstr)
      /* just concatenate absent-marker to the resulting key */
      return concat_bytes(key, &prefix_absent, 1);

    switch (type) {
    default: {
      if (unlikely(type < fptu_96))
        return FPTA_EOOPS;

      assert(type >= fptu_96 && type <= fptu_256);
      const size_t length = fptu_internal_map_t2b[type];
      uint8_t stub[256 / 8];
      assert(length <= sizeof(stub));
      /* prepare a denil value */
      const int fillbyte = fpta_index_is_obverse(index)
                               ? FPTA_DENIL_FIXBIN_OBVERSE
                               : FPTA_DENIL_FIXBIN_REVERSE;
      memset(&stub, fillbyte, length);
      /* concatenate to the resulting key */
      return concat_bytes(key, stub, length);
    }

    case fptu_datetime: {
      fptu_time stub;
      /* convert byte order for proper comparison result in a index kind. */
      stub.fixedpoint = obverse ? htobe(FPTA_DENIL_DATETIME_BIN)
                                : htole(FPTA_DENIL_DATETIME_BIN);
      /* concatenate to the resulting key */
      return concat_bytes(key, &stub, sizeof(stub));
    }

    case fptu_uint16: {
      uint16_t stub = (uint16_t)numeric_traits<fptu_uint16>::denil(index);
      /* convert byte order for proper comparison result in a index kind. */
      stub = obverse ? htobe(stub) : htole(stub);
      /* concatenate to the resulting key */
      return concat_bytes(key, &stub, sizeof(stub));
    }

    case fptu_uint32: {
      uint32_t stub = (uint32_t)numeric_traits<fptu_uint32>::denil(index);
      /* convert byte order for proper comparison result in a index kind. */
      stub = obverse ? htobe(stub) : htole(stub);
      /* concatenate to the resulting key */
      return concat_bytes(key, &stub, sizeof(stub));
    }

    case fptu_uint64: {
      uint64_t stub = (uint64_t)numeric_traits<fptu_uint64>::denil(index);
      /* convert byte order for proper comparison result in a index kind. */
      stub = obverse ? htobe(stub) : htole(stub);
      /* concatenate to the resulting key */
      return concat_bytes(key, &stub, sizeof(stub));
    }

    case fptu_int32: {
      int32_t stub = (int32_t)numeric_traits<fptu_int32>::denil(index);
      stub -= INT32_MIN /* rebase signed min-value to binary all-zeros */;
      /* convert byte order for proper comparison result in a index kind. */
      stub = obverse ? htobe(stub) : htole(stub);
      /* concatenate to the resulting key */
      return concat_bytes(key, &stub, sizeof(stub));
    }

    case fptu_int64: {
      int64_t stub = (int64_t)numeric_traits<fptu_int64>::denil(index);
      stub -= INT64_MIN /* rebase signed min-value to binary all-zeros */;
      /* convert byte order for proper comparison result in a index kind. */
      stub = obverse ? htobe(stub) : htole(stub);
      /* concatenate to the resulting key */
      return concat_bytes(key, &stub, sizeof(stub));
    }

    case fptu_fp32: {
      union {
        float fp32;
        uint32_t u32;
        int32_t i32;
      } stub;
      stub.fp32 = (float)numeric_traits<fptu_fp32>::denil(index);
      /* convert to binary-comparable value in the range 0..UINT32_MAX */
      stub.u32 = (stub.i32 < 0) ? UINT32_C(0xffffFFFF) - stub.u32
                                : stub.u32 + UINT32_C(0x80000000);
      /* convert byte order for proper comparison result in a index kind. */
      stub.u32 = obverse ? htobe(stub.u32) : htole(stub.u32);
      /* concatenate to the resulting key */
      return concat_bytes(key, &stub, sizeof(stub));
    }

    case fptu_fp64: {
      union {
        double fp64;
        uint64_t u64;
        int64_t i64;
      } stub;
      stub.fp64 = (double)numeric_traits<fptu_fp64>::denil(index);
      /* convert to binary-comparable value in the range 0..UINT64_MAX */
      stub.u64 = (stub.i64 < 0) ? UINT64_C(0xffffFFFFffffFFFF) - stub.u64
                                : stub.u64 + UINT64_C(0x8000000000000000);
      /* convert byte order for proper comparison result in a index kind. */
      stub.u64 = obverse ? htobe(stub.u64) : htole(stub.u64);
      /* concatenate to the resulting key */
      return concat_bytes(key, &stub, sizeof(stub));
    }
    }
  }

  if (fpta_index_is_nullable(index) && (alternale_nils || type >= fptu_cstr))
    /* add present-marker */
    concat_bytes(key, &prefix_present, 1);

  switch (type) {
  default: {
    if (unlikely(type < fptu_96 || type > fptu_opaque)) {
      /* LY: fptu_farray and fptu_nested cases - curently fpta don't
       * provide indexing such columns. */
      return FPTA_EOOPS;
    }

    const struct iovec iov = fptu_field_as_iovec(field);
    /* don't need byteorder conversion for string/binary data */
    return concat_bytes(key, iov.iov_base, iov.iov_len);
  }

  case fptu_datetime: {
    fptu_time value = field->payload()->dt;
    /* convert byte order for proper comparison result in a index kind. */
    value.fixedpoint =
        obverse ? htobe(value.fixedpoint) : htole(value.fixedpoint);
    /* concatenate to the resulting key */
    return concat_bytes(key, &value, sizeof(value));
  }

  case fptu_uint16: {
    uint16_t value = (uint16_t)field->get_payload_uint16();
    /* convert byte order for proper comparison result in a index kind. */
    value = obverse ? htobe(value) : htole(value);
    /* concatenate to the resulting key */
    return concat_bytes(key, &value, sizeof(value));
  }

  case fptu_uint32: {
    uint32_t value = field->payload()->u32;
    /* convert byte order for proper comparison result in a index kind. */
    value = obverse ? htobe(value) : htole(value);
    /* concatenate to the resulting key */
    return concat_bytes(key, &value, sizeof(value));
  }

  case fptu_uint64: {
    uint64_t value = field->payload()->u64;
    /* convert byte order for proper comparison result in a index kind. */
    value = obverse ? htobe(value) : htole(value);
    /* concatenate to the resulting key */
    return concat_bytes(key, &value, sizeof(value));
  }

  case fptu_int32: {
    int32_t value = field->payload()->i32;
    value -= INT32_MIN /* rebase signed min-value to binary all-zeros */;
    /* convert byte order for proper comparison result in a index kind. */
    value = obverse ? htobe(value) : htole(value);
    /* concatenate to the resulting key */
    return concat_bytes(key, &value, sizeof(value));
  }

  case fptu_int64: {
    int64_t value = field->payload()->i64;
    value -= INT64_MIN /* rebase signed min-value to binary all-zeros */;
    /* convert byte order for proper comparison result in a index kind. */
    value = obverse ? htobe(value) : htole(value);
    /* concatenate to the resulting key */
    return concat_bytes(key, &value, sizeof(value));
  }

  case fptu_fp32: {
    union {
      float fp32;
      uint32_t u32;
      int32_t i32;
    } value;
    value.u32 = field->payload()->u32 /* copy fp32 as-is */;
    /* convert to binary-comparable value in the range 0..UINT32_MAX */
    value.u32 = (value.i32 < 0) ? UINT32_C(0xffffFFFF) - value.u32
                                : value.u32 + UINT32_C(0x80000000);
    /* convert byte order for proper comparison result in a index kind. */
    value.u32 = obverse ? htobe(value.u32) : htole(value.u32);
    /* concatenate to the resulting key */
    return concat_bytes(key, &value, sizeof(value));
  }

  case fptu_fp64: {
    union {
      double fp64;
      uint64_t u64;
      int64_t i64;
    } value;
    value.u64 = field->payload()->u64 /* copy fp64 as-is */;
    /* convert to binary-comparable value in the range 0..UINT64_MAX */
    value.u64 = (value.i64 < 0) ? UINT64_C(0xffffFFFFffffFFFF) - value.u64
                                : value.u64 + UINT64_C(0x8000000000000000);
    /* convert byte order for proper comparison result in a index kind. */
    value.u64 = obverse ? htobe(value.u64) : htole(value.u64);
    /* concatenate to the resulting key */
    return concat_bytes(key, &value, sizeof(value));
  }
  }
}

int fpta_composite_row2key(const fpta_table_schema *const schema, size_t column,
                           const fptu_ro &row, fpta_key &key) {
#ifndef NDEBUG
  fpta_pollute(&key, sizeof(key), 0);
#endif
  assert(column < schema->column_count());
  const fpta_shove_t shove = schema->column_shove(column);
  const fpta_index_type index = fpta_shove2index(shove);
  if (unlikely(!fpta_is_composite(shove) || !fpta_is_indexed(index)))
    return FPTA_EOOPS;

  /* get list of the composed columns */
  fpta_table_schema::composite_iter_t begin, end;
  int rc = schema->composite_list(column, begin, end);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  concat_column_t concat =
      fpta_index_is_ordered(index) ? concat_ordered : concat_unordered;

  if (likely(fpta_index_is_unordered(index))) {
    key.mdbx.iov_base = &key.place.u64;
    key.mdbx.iov_len = 8;
    key.place.u64 = 0;
    concat = concat_unordered;
  } else {
    concat = concat_ordered;
    key.mdbx.iov_len = 0;
    key.mdbx.iov_base = fpta_index_is_obverse(index)
                            ? &key.place.longkey_obverse.tailhash
                            : &key.place.longkey_reverse.headhash;
  }

  const bool alternale_nils = fpta_index_is_nullable(index);
  if (fpta_index_is_obverse(index)) {
    for (auto i = begin; i != end; ++i) {
      const unsigned column = *i;
      rc = concat(key, alternale_nils, schema, row, column);
      if (unlikely(rc != FPTA_SUCCESS))
        return rc;
    }
  } else {
    for (auto i = end; i != begin;) {
      const unsigned column = *--i;
      rc = concat(key, alternale_nils, schema, row, column);
      if (unlikely(rc != FPTA_SUCCESS))
        return rc;
    }
  }

  if (unlikely(fpta_index_is_ordered(index))) {
    assert(key.mdbx.iov_len <= sizeof(key.place));
    /* setup pointer for an ordered (variable size) key */
    uint8_t *ptr = (uint8_t *)&key.place;
    if (fpta_index_is_reverse(index))
      ptr += sizeof(key.place) - key.mdbx.iov_len;
    key.mdbx.iov_base = ptr;
  }

  return FPTA_SUCCESS;
}

//----------------------------------------------------------------------------

int fpta_composite_column_count_ex(const fpta_name *composite_id,
                                   unsigned *count) {
  if ((unlikely(count == nullptr)))
    return FPTA_EINVAL;
  *count = 0;

  int rc = fpta_id_validate(composite_id, fpta_column_with_schema);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;
  if (unlikely(!fpta_column_is_composite(composite_id)))
    return FPTA_EINVAL;

  fpta_table_schema::composite_iter_t begin, end;
  const fpta_table_schema *table_schema =
      composite_id->column.table->table_schema;
  rc = table_schema->composite_list(composite_id->column.num, begin, end);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  assert(end > begin && end - begin < fpta_max_cols);
  *count = (unsigned)(end - begin);
  return FPTA_SUCCESS;
}

int fpta_composite_column_get(const fpta_name *composite_id, unsigned item,
                              fpta_name *column_id) {
  if (unlikely(column_id == nullptr))
    return FPTA_EINVAL;
  memset(column_id, 0, sizeof(fpta_name));

  int rc = fpta_id_validate(composite_id, fpta_column_with_schema);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;
  if (unlikely(!fpta_column_is_composite(composite_id)))
    return FPTA_EINVAL;

  fpta_table_schema::composite_iter_t begin, end;
  const fpta_table_schema *table_schema =
      composite_id->column.table->table_schema;
  rc = table_schema->composite_list(composite_id->column.num, begin, end);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  assert(end > begin && end - begin < fpta_max_cols);
  if (end - begin < item)
    return FPTA_NODATA;

  const unsigned column = begin[item];
  column_id->column.table = composite_id->column.table;
  column_id->shove = table_schema->column_shove(column);
  column_id->column.num = column;
  column_id->version = composite_id->version;

  assert(fpta_id_validate(column_id, fpta_column) == FPTA_SUCCESS);
  return FPTA_SUCCESS;
}

//----------------------------------------------------------------------------

int fpta_composite_index_validate(
    const fpta_index_type index_type,
    const fpta_table_schema::composite_item_t *const items_begin,
    const fpta_table_schema::composite_item_t *const items_end,
    const fpta_shove_t *const columns_shoves, const size_t column_count,
    const fpta_table_schema::composite_item_t *const composites_begin,
    const fpta_table_schema::composite_item_t *const composites_end,
    const fpta_shove_t skipself) {

  if (unlikely(items_end < items_begin))
    return FPTA_SCHEMA_CORRUPTED;
  if (unlikely(items_end - items_begin < 2))
    return FPTA_EINVAL;

/* scan columns of the given composite index */
#ifndef DONT_USE_BITSET
  std::bitset<fpta_max_cols> items;
#else
  std::vector<fpta_table_schema::composite_item_t> items;
#endif
  for (auto scan = items_begin; scan != items_end; ++scan) {
    const size_t column_number = *scan;
    if (unlikely(column_number >= column_count))
      return FPTA_SCHEMA_CORRUPTED;

#ifndef DONT_USE_BITSET
    if (unlikely(items.test(column_number)))
      return FPTA_EEXIST;
    items.set(column_number);
#else
    if (unlikely(std::find(items_begin, scan, column_number) != scan))
      return FPTA_EEXIST;
#endif

    const fpta_shove_t column_shove = columns_shoves[column_number];
    if (unlikely(fpta_is_composite(column_shove)))
      /* reject composite indexes/columns */
      return FPTA_ETYPE;

    if (unlikely(fpta_shove2type(column_shove) >= fptu_nested))
      /* reject arrays and nested tuples */
      return FPTA_ETYPE;

    if (!fpta_is_indexed(column_shove))
      continue;

    if (unlikely(fpta_index_is_unique(column_shove) &&
                 fpta_index_is_unique(index_type)))
      /* uniqueness of an one element is sufficient
       * for uniqueness of the entire composite key */
      return FPTA_SIMILAR_INDEX;

    /* more cases for ordered */
    if (fpta_index_is_ordered(column_shove) &&
        fpta_index_is_ordered(index_type)) {
      if (scan == items_begin && fpta_index_is_obverse(column_shove) &&
          fpta_index_is_obverse(index_type))
        /* obverse-sorting by a composite key
         * outmatches sorting by the first element */
        return FPTA_SIMILAR_INDEX;

      if (scan + 1 == items_end && fpta_index_is_reverse(column_shove) &&
          fpta_index_is_reverse(index_type))
        /* reverse-sorting by a composite key outmatches
         * sorting by the last (first in reverse) element */
        return FPTA_SIMILAR_INDEX;
    }
  }

  /* scan other composites */
  auto composites = composites_begin;
  for (size_t i = 0; i < column_count; ++i) {
    const fpta_shove_t column_shove = columns_shoves[i];
    if (column_shove == 0) {
      /* zero slot is empty while PK undefined,
       * skip it in such case */
      assert(i == 0);
      continue;
    }
    if (!fpta_is_composite(column_shove))
      continue;

    if (unlikely(!fpta_is_indexed(column_shove) ||
                 composites >= composites_end || *composites == 0))
      return FPTA_SCHEMA_CORRUPTED;

    const auto present_first = composites + 1;
    const auto present_last = present_first + *composites;
    if (unlikely(present_last > composites_end))
      return FPTA_SCHEMA_CORRUPTED;

    composites = present_last;
    if (skipself == column_shove)
      continue;

#ifndef DONT_USE_BITSET
    std::bitset<fpta_max_cols> present;
    std::for_each(
        present_first, present_last,
        [&present](unsigned column_number) { present.set(column_number); });
#else
    if (items.empty()) {
      items.assign(items_begin, items_end);
      std::sort(items.begin(), items.end());
      assert(!items.empty());
    }
    std::vector<fpta_table_schema::composite_item_t> present(present_first,
                                                             present_last);
    std::sort(present.begin(), present.end());
#endif

    if (unlikely(present == items)) {
      /* sets of columns are matched.
       *
       * so, only one case it is valid:
       *  - both indexes are ordered;
       *  - and one of them is obverse and the other is reverse. */
      if (fpta_index_is_unordered(column_shove) ||
          fpta_index_is_unordered(index_type) ||
          fpta_index_is_obverse(column_shove) ==
              fpta_index_is_obverse(index_type))
        return FPTA_SIMILAR_INDEX;
    }

    if (fpta_index_is_unique(index_type) &&
        fpta_index_is_unique(column_shove)) {
      /* both indexes requires uniqueness, one should
       * not be a subset of the another */
      bool subset;
#ifndef DONT_USE_BITSET
      const auto intersection = items & present;
      subset = (intersection == items || intersection == present);
#else
      auto outer_begin = items.begin();
      auto outer_end = items.end();
      auto inner_begin = present.begin();
      auto inner_end = present.end();
      if (outer_end - outer_begin < inner_end - inner_begin) {
        std::swap(outer_begin, inner_begin);
        std::swap(outer_end, inner_end);
      }

      assert((outer_end - outer_begin >= inner_end - inner_begin));
      subset = std::includes(outer_begin, outer_end, inner_begin, inner_end);
#endif
      if (subset)
        /* one set of columns includes the another */
        return FPTA_SIMILAR_INDEX;
    }

    if (fpta_index_is_ordered(index_type) &&
        fpta_index_is_ordered(column_shove)) {
      /* both indexes are ordered, one should
       * not be a prefix/beginning of the another. */
      const size_t shortest = std::min(items.size(), present.size());
      const auto left = fpta_index_is_obverse(index_type)
                            ? items_begin
                            : items_end - shortest;
      const auto right = fpta_index_is_obverse(column_shove)
                             ? present_first
                             : present_last - shortest;
      assert(left >= items_begin && left + shortest <= items_end);
      assert(right >= present_first && right + shortest <= present_last);
      if (std::equal(left, left + shortest, right))
        /* one set of columns is a prefix/beginning of the another,
         * in an order of comparison of index. */
        return FPTA_SIMILAR_INDEX;
    }
  }
  return FPTA_SUCCESS;
}

int fpta_describe_composite_index(const char *composite_name,
                                  fpta_index_type index_type,
                                  fpta_column_set *column_set,
                                  const char *const column_names_array[],
                                  size_t column_names_count) {
  if (unlikely(!fpta_validate_name(composite_name)))
    return FPTA_ENAME;

  if (unlikely(!fpta_is_indexed(index_type)))
    return FPTA_EFLAG;

  if (unlikely(column_set == nullptr))
    return FPTA_EINVAL;

  if (unlikely(column_names_count < 2 ||
               column_names_count > column_set->count))
    return FPTA_EINVAL;

  if (unlikely(column_names_array == nullptr))
    return FPTA_EINVAL;

  std::vector<fpta_table_schema::composite_item_t> items;
  items.reserve(column_names_count);
  for (size_t i = 0; i < column_names_count; ++i) {
    const char *column_name = column_names_array[i];
    if (unlikely(!fpta_validate_name(column_name)))
      return FPTA_ENAME;

    const fpta_shove_t shove = fpta_shove_name(column_name, fpta_column);
    for (size_t n = 0; n < column_set->count; ++n) {
      const fpta_shove_t column_shove = column_set->shoves[n];
      if (column_shove == 0 && n == 0)
        /* zero slot is empty while PK undefined,
         * skip it in such case */
        continue;

      if (fpta_shove_eq(column_set->shoves[n], shove)) {
        items.push_back((fpta_table_schema::composite_item_t)n);
        break;
      }
    }
  }
  if (unlikely(items.size() != column_names_count))
    return FPTA_COLUMN_MISSING;

  int rc = fpta_composite_index_validate(
      index_type, items.data(), items.data() + items.size(), column_set->shoves,
      column_set->count, column_set->composites,
      FPT_ARRAY_END(column_set->composites), 0);
  if (rc != FPTA_SUCCESS)
    return rc;

  if (unlikely(column_set->count > fpta_max_cols))
    return FPTA_TOOMANY;

  fpta_table_schema::composite_item_t *const begin = column_set->composites;
  fpta_table_schema::composite_item_t *const end =
      FPT_ARRAY_END(column_set->composites);
  fpta_table_schema::composite_item_t *tail;
  for (tail = begin; *tail;) {
    tail += *tail + 1;
    if (unlikely(tail >= end))
      return (tail == end) ? FPTA_TOOMANY : FPTA_SCHEMA_CORRUPTED;
  }
  if (end - tail <= (ptrdiff_t)column_names_count)
    return FPTA_TOOMANY;

  /* add name to column's shoves */
  rc = fpta_schema_add(column_set, composite_name, /* composite */ fptu_null,
                       index_type);
  if (rc != FPTA_SUCCESS)
    return rc;

  /* append index's items to composites */
  *tail++ = (fpta_table_schema::composite_item_t)column_names_count;
  for (auto n : items)
    *tail++ = n;
  assert(tail <= end);

  return FPTA_SUCCESS;
}

int fpta_describe_composite_index_va(const char *composite_name,
                                     fpta_index_type index_type,
                                     fpta_column_set *column_set,
                                     const char *first, const char *second,
                                     const char *third, ...) {

  if (unlikely(first == nullptr || second == nullptr))
    return FPTA_EINVAL;

  size_t count = 2;
  va_list ap;
#ifdef _MSC_VER /* avoid mad warnings from MSVC */
  va_start(ap, third);
#endif

  if (third) {
#ifndef _MSC_VER
    va_start(ap, third);
#endif

    va_list ap_count;
    va_copy(ap_count, ap);
    const char *last;
    do {
      ++count;
      last = va_arg(ap_count, const char *);
    } while (count <= fpta_max_cols && last != nullptr);
    va_end(ap_count);
  }

  int rc = FPTA_TOOMANY;
  if (count <= fpta_max_cols) {
    const auto vector = (const char **)alloca(count * sizeof(const char *));
    vector[0] = first;
    vector[1] = second;
    if (third) {
      vector[2] = third;
      for (size_t i = 3; i < count; ++i)
        vector[i] = va_arg(ap, const char *);
      va_end(ap);
    }
    rc = fpta_describe_composite_index(composite_name, index_type, column_set,
                                       vector, count);
  } else if (third)
    va_end(ap);

  return rc;
}

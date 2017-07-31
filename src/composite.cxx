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

#define MARKER_ABSENT UINT64_C(0x974BC764BAC4C7F)

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
  const fptu_field *field = fptu_lookup_ro(row, column, type);

  const uint8_t prefix_absent = 0;
  const uint8_t prefix_present = 42;
  const bool obverse = key.mdbx.iov_base == &key.place.longkey_obverse.tailhash;

  if (unlikely(field == nullptr)) {
    const fpta_index_type index = fpta_shove2index(shove);
    if (unlikely(!fpta_index_is_nullable(index)))
      return FPTA_COLUMN_MISSING;
    if (alternale_nils)
      /* just concatenate absent-marker to the resulting key */
      return concat_bytes(key, &prefix_absent, 1);

    switch (type) {
    default: {
      if (unlikely(type < fptu_96))
        return FPTA_EOOPS;
      if (type >= fptu_cstr)
        /* just concatenate absent-marker to the resulting key */
        return concat_bytes(key, &prefix_absent, 1);

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
        float fp64;
        uint32_t u64;
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

  if (alternale_nils || type >= fptu_cstr)
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
      float fp64;
      uint32_t u64;
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
  if (unlikely(fpta_shove2type(shove) != fptu_null ||
               !fpta_index_is_secondary(index)))
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

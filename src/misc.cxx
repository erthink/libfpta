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

#define FIXME "FIXME: " __FILE__ ", " FPT_STRINGIFY(__LINE__)

namespace std {

__cold string to_string(fpta_error) { return FIXME; }

__cold string to_string(const fptu_time &) { return FIXME; }

__cold string to_string(fpta_value_type type) {
  switch (type) {
  default:
    return "invalid(fpta_value_type)" + to_string((int)type);

  case fpta_null:
    return "null";
  case fpta_signed_int:
    return "signed_int";
  case fpta_unsigned_int:
    return "unsigned_int";
  /* case  fpta_datetime:
      return "datetime"; */
  case fpta_float_point:
    return "float_point";
  case fpta_string:
    return "string";
  case fpta_binary:
    return "binary";
  case fpta_shoved:
    return "shoved";
  case fpta_begin:
    return "<begin>";
  case fpta_end:
    return "<end>";
  }
}

__cold string to_string(const fpta_value) { return FIXME; }

__cold string to_string(fpta_durability) { return FIXME; }

__cold string to_string(fpta_level) { return FIXME; }

__cold string to_string(const fpta_db *) { return FIXME; }

__cold string to_string(const fpta_txn *) { return FIXME; }

__cold string to_string(fpta_index_type index) {
  switch (index) {
  default:
    return "invalid(fpta_index_type)" + to_string((int)index);

  case fpta_index_none:
    return "none";

  case fpta_primary_withdups:
    return "primary-withdups-obverse";
  case fpta_primary_unique:
    return "primary-unique-obverse";
  case fpta_primary_withdups_unordered:
    return "primary-withdups-unordered";
  case fpta_primary_unique_unordered:
    return "primary-unique-unordered";
  case fpta_primary_withdups_reversed:
    return "primary-withdups-reverse";
  case fpta_primary_unique_reversed:
    return "primary-unique-reverse";

  case fpta_secondary_withdups:
    return "secondary-withdups-obverse";
  case fpta_secondary_unique:
    return "secondary-unique-obverse";
  case fpta_secondary_withdups_unordered:
    return "secondary-withdups-unordered";
  case fpta_secondary_unique_unordered:
    return "secondary-unique-unordered";
  case fpta_secondary_withdups_reversed:
    return "secondary-withdups-reverse";
  case fpta_secondary_unique_reversed:
    return "secondary-unique-reverse";
  }
}

__cold string to_string(const fpta_column_set &) { return FIXME; }

__cold string to_string(const fpta_name &) { return FIXME; }

__cold string to_string(fpta_schema_item) { return FIXME; }

__cold string to_string(fpta_filter_bits) { return FIXME; }

__cold string to_string(const fpta_filter &) { return FIXME; }

__cold string to_string(fpta_cursor_options op) {
  switch (op) {
  default:
    return "invalid(fpta_cursor_options)" + to_string((int)op);
  case fpta_unsorted:
    return "unsorted";
  case fpta_ascending:
    return "ascending";
  case fpta_descending:
    return "descending";
  case fpta_unsorted_dont_fetch:
    return "unsorted-dont-fetch";
  case fpta_ascending_dont_fetch:
    return "ascending-dont-fetch";
  case fpta_descending_dont_fetch:
    return "descending-dont-fetch";
  }
}

__cold string to_string(const fpta_cursor *) { return FIXME; }

__cold string to_string(fpta_seek_operations) { return FIXME; }

__cold string to_string(fpta_put_options) { return FIXME; }
}

void fpta_pollute(void *ptr, size_t bytes, uintptr_t xormask) {
  if (xormask) {
    while (bytes >= sizeof(uintptr_t)) {
      *((uintptr_t *)ptr) ^= xormask;
      ptr = (char *)ptr + sizeof(uintptr_t);
      bytes -= sizeof(uintptr_t);
    }

    if (bytes) {
      uintptr_t tail;
      memcpy(&tail, ptr, bytes);
      tail ^= xormask;
      memcpy(ptr, &tail, bytes);
    }
  } else {
    while (bytes >= sizeof(uint32_t)) {
      *((uint32_t *)ptr) = mrand48();
      ptr = (char *)ptr + sizeof(uint32_t);
      bytes -= sizeof(uint32_t);
    }

    if (bytes) {
      uint32_t tail = mrand48();
      memcpy(ptr, &tail, bytes);
    }
  }
}

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
#include <cinttypes> // for PRId64, PRIu64

#define FIXME "FIXME: " __FILE__ ", " FPT_STRINGIFY(__LINE__)

namespace std {

__cold string to_string(const fpta_error errnum) {
  /* FIXME: use fpta_strerror_r() ? */
  return fpta_strerror(errnum);
}

__cold string to_string(const fpta_value_type type) {
  switch (type) {
  default:
    return fptu::format("invalid(fpta_value_type)%i", (int)type);

  case fpta_null:
    return "null";
  case fpta_signed_int:
    return "signed_int";
  case fpta_unsigned_int:
    return "unsigned_int";
  case fpta_datetime:
    return "datetime";
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

__cold string to_string(const fpta_value &value) {
  switch (value.type) {
  default:
    return fptu::format("invalid(fpta_value_type)%i", (int)value.type);

  case fpta_null:
    return "null";
  case fpta_begin:
    return "<begin>";
  case fpta_end:
    return "<end>";

  case fpta_signed_int:
    return fptu::format("%-" PRId64, value.sint);

  case fpta_unsigned_int:
    return fptu::format("%" PRIu64, value.sint);

  case fpta_datetime:
    return to_string(value.datetime);

  case fpta_float_point:
    return fptu::format("%.10g", value.fp);

  case fpta_string:
    return fptu::format("\"%.*s\"", value.binary_length,
                        (const char *)value.binary_data);

  case fpta_binary:
    return fptu::hexadecimal(value.binary_data, value.binary_length);

  case fpta_shoved:
    return "@" + fptu::hexadecimal(value.binary_data, value.binary_length);
  }
}

__cold string to_string(const fpta_durability durability) {
  switch (durability) {
  default:
    return fptu::format("invalid(fpta_durability)%i", (int)durability);
  case fpta_readonly:
    return "mode-readonly";
  case fpta_sync:
    return "mode-sync";
  case fpta_lazy:
    return "mode-lazy";
  case fpta_async:
    return "mode-async";
  }
}

__cold string to_string(const fpta_level level) {
  switch (level) {
  default:
    return fptu::format("invalid(fpta_level)%i", (int)level);
  case fpta_read:
    return "level-read";
  case fpta_write:
    return "level-write";
  case fpta_schema:
    return "level-schema";
  }
}

__cold string to_string(const fpta_index_type index) {
  switch (index) {
  default:
    return fptu::format("invalid(fpta_index_type)%i", (int)index);

  case fpta_index_none:
    return "index-none";

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

__cold string to_string(const fpta_schema_item item) {
  switch (item) {
  default:
    return fptu::format("invalid(fpta_schema_item)%i", (int)item);
  case fpta_table:
    return "table";
  case fpta_column:
    return "column";
  }
}

__cold string to_string(const fpta_filter_bits bits) {
  switch (bits) {
  default:
    return fptu::format("invalid(fpta_filter_bits)%i", (int)bits);
  case fpta_node_not:
    return "NOT";
  case fpta_node_or:
    return "OR";
  case fpta_node_and:
    return "AND";
  case fpta_node_fn:
    return "FN()";
  case fpta_node_lt:
  case fpta_node_gt:
  case fpta_node_le:
  case fpta_node_ge:
  case fpta_node_eq:
  case fpta_node_ne:
    return to_string((fptu_lge)bits);
  }
}

__cold string to_string(const fpta_cursor_options op) {
  switch (op) {
  default:
    return fptu::format("invalid(fpta_cursor_options)%i", (int)op);
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

__cold string to_string(const fpta_seek_operations op) {
  switch (op) {
  default:
    return fptu::format("invalid(fpta_seek_operations)%i", (int)op);
  case fpta_first:
    return "row.first";
  case fpta_last:
    return "row.last";
  case fpta_next:
    return "row.next";
  case fpta_prev:
    return "row.prev";
  case fpta_dup_first:
    return "dup.first";
  case fpta_dup_last:
    return "dup.last";
  case fpta_dup_next:
    return "dup.next";
  case fpta_dup_prev:
    return "dup.prev";
  case fpta_key_next:
    return "key.next";
  case fpta_key_prev:
    return "key.prev";
  }
}

__cold string to_string(const fpta_put_options op) {
  switch (op) {
  default:
    return fptu::format("invalid(fpta_put_options)%i", (int)op);
  case fpta_insert:
    return "insert";
  case fpta_update:
    return "update";
  case fpta_upsert:
    return "upsert";
  }
}

__cold string to_string(const fpta_name &) { return FIXME; }

__cold string to_string(const fpta_column_set &) { return FIXME; }

__cold string to_string(const fpta_filter &) { return FIXME; }

__cold string to_string(const fpta_db *) { return FIXME; }

__cold string to_string(const fpta_txn *) { return FIXME; }

__cold string to_string(const fpta_cursor *) { return FIXME; }
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
      *((uint32_t *)ptr) = (uint32_t)mrand48();
      ptr = (char *)ptr + sizeof(uint32_t);
      bytes -= sizeof(uint32_t);
    }

    if (bytes) {
      uint32_t tail = (uint32_t)mrand48();
      memcpy(ptr, &tail, bytes);
    }
  }
}

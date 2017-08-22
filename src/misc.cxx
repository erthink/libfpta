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

#if defined(_WIN32) || defined(_WIN64)
#ifdef _MSC_VER
#pragma warning(push, 1)
#endif
#include <windows.h>
#ifdef _MSC_VER
#pragma warning(pop)
#endif
#endif /* must die */

#define FIXME "FIXME: " __FILE__ ", " FPT_STRINGIFY(__LINE__)

static const char *__fpta_errstr(int errnum) {
#if defined(_WIN32) || defined(_WIN64)
  static_assert(FPTA_ENOMEM == ERROR_OUTOFMEMORY, "error code mismatch");
  static_assert(FPTA_ENOIMP == ERROR_NOT_SUPPORTED, "error code mismatch");
  static_assert(FPTA_EVALUE == ERROR_INVALID_DATA, "error code mismatch");
  static_assert(FPTA_OVERFLOW == ERROR_ARITHMETIC_OVERFLOW,
                "error code mismatch");
  static_assert(FPTA_EEXIST == ERROR_ALREADY_EXISTS, "error code mismatch");
  static_assert(FPTA_ENOENT == ERROR_NOT_FOUND, "error code mismatch");
  static_assert(FPTA_EPERM == ERROR_INVALID_FUNCTION, "error code mismatch");
  static_assert(FPTA_EBUSY == ERROR_BUSY, "error code mismatch");
  static_assert(FPTA_ENAME == ERROR_INVALID_NAME, "error code mismatch");
  static_assert(FPTA_EFLAG == ERROR_INVALID_FLAG_NUMBER, "error code mismatch");
#endif /* static_asserts for Windows */

  switch (errnum) {
  default:
    return NULL;

  case FPTA_SUCCESS:
    return "FPTA: Success";

  case FPTA_EOOPS:
    return "FPTA: Internal unexpected Oops";

  case FPTA_SCHEMA_CORRUPTED:
    return "FPTA: Schema is invalid or corrupted";

  case FPTA_ETYPE:
    return "FPTA: Type mismatch (given value vs column/field or index)";

  case FPTA_DATALEN_MISMATCH:
    return "FPTA: Data length mismatch (given value vs data type)";

  case FPTA_KEY_MISMATCH:
    return "FPTA: Key mismatch while updating row via cursor";

  case FPTA_COLUMN_MISSING:
    return "FPTA: Required column missing";

  case FPTA_INDEX_CORRUPTED:
    return "FPTA: Index is inconsistent or corrupted";

  case FPTA_NO_INDEX:
    return "FPTA: No (such) index for given column";

  case FPTA_SCHEMA_CHANGED:
    return "FPTA: Schema changed (transaction should be restared)";

  case FPTA_ECURSOR:
    return "FPTA: Cursor is not positioned";

  case FPTA_TOOMANY:
    return "FPTA: Too many columns or indexes (limit reached)";

  case FPTA_WANNA_DIE:
    return "FPTA: Failure while transaction rollback (wanna die)";

  case FPTA_TXN_CANCELLED:
    return "Transaction already cancelled";

  case FPTA_SIMILAR_INDEX:
    return "Adding index which is similar to one of the existing";

  case FPTA_NODATA /* -1, EOF */:
    return "FPTA: No data or EOF was reached";
  }
}

const char *fpta_strerror(int errnum) {
  const char *msg = __fpta_errstr(errnum);
  return msg ? msg : mdbx_strerror(errnum);
}

const char *fpta_strerror_r(int errnum, char *buf, size_t buflen) {
  const char *msg = __fpta_errstr(errnum);
  return msg ? msg : mdbx_strerror_r(errnum, buf, buflen);
}

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

__cold string to_string(const fpta_value *value) {
  if (value == nullptr)
    return "nullptr";

  switch (value->type) {
  default:
    return fptu::format("invalid(fpta_value_type)%i", (int)value->type);

  case fpta_null:
    return "null";
  case fpta_begin:
    return "<begin>";
  case fpta_end:
    return "<end>";

  case fpta_signed_int:
    return fptu::format("%-" PRId64, value->sint);

  case fpta_unsigned_int:
    return fptu::format("%" PRIu64, value->sint);

  case fpta_datetime:
    return to_string(value->datetime);

  case fpta_float_point:
    return fptu::format("%.10g", value->fp);

  case fpta_string:
    return fptu::format("\"%.*s\"", value->binary_length,
                        (const char *)value->binary_data);

  case fpta_binary:
    return fptu::hexadecimal(value->binary_data, value->binary_length);

  case fpta_shoved:
    return "@" + fptu::hexadecimal(value->binary_data, value->binary_length);
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
  case fpta_noindex_nullable:
    return "index-none.nullable";

  case fpta_primary_withdups_ordered_obverse:
    return "primary-withdups-ordered.obverse";
  case fpta_primary_withdups_ordered_obverse_nullable:
    return "primary-withdups-ordered.obverse-nullable";
  case fpta_primary_withdups_ordered_reverse:
    return "primary-withdups-ordered.reverse";
  case fpta_primary_withdups_ordered_reverse_nullable:
    return "primary-withdups-ordered.reverse-nullable";

  case fpta_primary_unique_ordered_obverse:
    return "primary-unique-ordered.obverse";
  case fpta_primary_unique_ordered_obverse_nullable:
    return "primary-unique-ordered.obverse-nullable";
  case fpta_primary_unique_ordered_reverse:
    return "primary-unique-ordered.reverse";
  case fpta_primary_unique_ordered_reverse_nullable:
    return "primary-unique-ordered.reverse-nullable";

  case fpta_primary_unique_unordered:
    return "primary-unique-unordered";
  case fpta_primary_unique_unordered_nullable_obverse:
    return "primary-unique-unordered.nullable-obverse";
  case fpta_primary_unique_unordered_nullable_reverse:
    return "primary-unique-unordered.nullable-reverse";

  case fpta_primary_withdups_unordered:
    return "primary-withdups-unordered";
  case fpta_primary_withdups_unordered_nullable_obverse:
    return "primary-withdups-unordered.nullable-obverse";

  case fpta_secondary_withdups_ordered_obverse:
    return "secondary-withdups-ordered.obverse";
  case fpta_secondary_withdups_ordered_obverse_nullable:
    return "secondary-withdups-ordered.obverse-nullable";
  case fpta_secondary_withdups_ordered_reverse:
    return "secondary-withdups-ordered.reverse";
  case fpta_secondary_withdups_ordered_reverse_nullable:
    return "secondary-withdups-ordered.reverse-nullable";

  case fpta_secondary_unique_ordered_obverse:
    return "secondary-unique-ordered.obverse";
  case fpta_secondary_unique_ordered_obverse_nullable:
    return "secondary-unique-ordered.obverse-nullable";
  case fpta_secondary_unique_ordered_reverse:
    return "secondary-unique-ordered.reverse";
  case fpta_secondary_unique_ordered_reverse_nullable:
    return "secondary-unique-ordered.reverse-nullable";

  case fpta_secondary_unique_unordered:
    return "secondary-unique-unordered";
  case fpta_secondary_unique_unordered_nullable_obverse:
    return "secondary-unique-unordered.nullable-obverse";
  case fpta_secondary_unique_unordered_nullable_reverse:
    return "secondary-unique-unordered.nullable-reverse";

  case fpta_secondary_withdups_unordered:
    return "secondary-withdups-unordered";
  case fpta_secondary_withdups_unordered_nullable_obverse:
    return "secondary-withdups-unordered.nullable-obverse";
  case fpta_secondary_withdups_unordered_nullable_reverse:
    return "secondary-withdups-unordered.nullable-reverse";
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
  case fpta_node_fncol:
    return "FN_COLUMN()";
  case fpta_node_fnrow:
    return "FN_ROW()";
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
    return "unsorted.dont-fetch";
  case fpta_ascending_dont_fetch:
    return "ascending.dont-fetch";
  case fpta_descending_dont_fetch:
    return "descending.dont-fetch";
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

__cold string to_string(const fpta_table_schema *def) {
  if (def == nullptr)
    return "nullptr";

  string result =
      fptu::format("%p={v%" PRIu64 ", $%" PRIx32 "_%" PRIx64 ", @%" PRIx64
                   ", %" PRIuSIZE "=[",
                   def, def->version_csn(), def->signature(), def->checksum(),
                   def->table_shove(), def->column_count());

  for (size_t i = 0; i < def->column_count(); ++i) {
    const fpta_shove_t shove = def->column_shove(i);
    const fpta_index_type index = fpta_shove2index(shove);
    const fptu_type type = fpta_shove2type(shove);
    result += fptu::format(&", @%" PRIx64 "."[(i == 0) ? 2 : 0], shove) +
              to_string(index) + "." + to_string(type);
  }

  return result + "]}";
}

__cold string to_string(const fpta_name *id) {
  if (id == nullptr)
    return "nullptr";

  const bool is_table =
      fpta_shove2index(id->shove) == (fpta_index_type)fpta_flag_table;

  if (is_table) {
    string partial = fptu::format("table.%p{@%" PRIx64 ", v%" PRIu64 ", ", id,
                                  id->shove, id->version);
    const fpta_table_schema *table_def = id->table_schema;
    if (table_def == nullptr)
      return partial + ", no-schema}";

    const fpta_index_type index = fpta_shove2index(table_def->table_pk());
    const fptu_type type = fpta_shove2type(table_def->table_pk());
    return partial + to_string(index) + "." + to_string(type) +
           fptu::format(", dbi-hint#%u, ", table_def->handle_cache(0)) +
           to_string(table_def) + "}";
  }

  string partial = fptu::format("column.%p{@%" PRIx64 ", v%" PRIu64, id,
                                id->shove, id->version);

  const fpta_name *table_id = id->column.table;
  if (table_id == nullptr)
    return partial + ", no-table, no-schema}";

  const fpta_table_schema *table_def = table_id->table_schema;
  if (table_def == nullptr)
    return partial + fptu::format(", @%" PRIx64 ".%p, no-schema}",
                                  table_id->shove, table_id);

  const fpta_index_type index = fpta_name_colindex(id);
  const fptu_type type = fpta_name_coltype(id);
  return partial + fptu::format(", col#%i, @%" PRIx64 ".%p, ", id->column.num,
                                table_id->shove, table_id) +
         to_string(index) + "." + to_string(type) +
         fptu::format(", dbi-hint#%u}",
                      table_def->handle_cache(id->column.num));
}

__cold string to_string(const fpta_column_set *) { return FIXME; }

__cold string to_string(const fpta_filter *filter) {
  if (filter == nullptr)
    return "TRUE";

  switch (filter->type) {
  default:
    return fptu::format("invalid(filter-type)%i", (int)filter->type);
  case fpta_node_not:
    return "NOT (" + to_string(filter->node_not) + "(";
  case fpta_node_or:
    return "(" + to_string(filter->node_or.a) + " OR " +
           to_string(filter->node_or.b) + ")";
  case fpta_node_and:
    return "(" + to_string(filter->node_and.a) + " AND " +
           to_string(filter->node_and.b) + ")";
  case fpta_node_fncol:
    return fptu::format("FN_COLUMN.%p(", filter->node_fncol.predicate) +
           to_string(filter->node_fncol.column_id) +
           fptu::format(", arg.%p)", filter->node_fncol.arg);
  case fpta_node_fnrow:
    return fptu::format("FN_ROW.%p(", filter->node_fnrow.predicate) +
           fptu::format(", context.%p, arg.%p)", filter->node_fnrow.context,
                        filter->node_fncol.arg);
  case fpta_node_lt:
  case fpta_node_gt:
  case fpta_node_le:
  case fpta_node_ge:
  case fpta_node_eq:
  case fpta_node_ne:
    return to_string(filter->node_cmp.left_id) + " " +
           to_string((fptu_lge)filter->type) + " " +
           to_string(filter->node_cmp.right_value);
  }
}

__cold string to_string(const fpta_db *db) {
  return fptu::format("%p." FIXME, db);
}

__cold string to_string(const fpta_txn *txt) {
  return fptu::format("%p." FIXME, txt);
}

__cold string to_string(const MDBX_val &value) {
  return fptu::format("%" PRIuPTR "_%p (", value.iov_len, value.iov_base) +
         fptu::hexadecimal(value.iov_base, value.iov_len) + ")";
}

__cold string to_string(const fpta_key &key) { return to_string(key.mdbx); }

__cold string to_string(const fpta_cursor *cursor) {
  string result = fptu::format("cursor.%p={", cursor);
  if (cursor) {
    result += fptu::format("\n\tmdbx %p,\n\toptions ", cursor->mdbx_cursor) +
              to_string(cursor->options);

    if (cursor->is_filled())
      result += ",\n\tcurrent " + to_string(cursor->current);
    else if (cursor->is_before_first())
      result += ",\n\tstate before-first (FPTA_NODATA)";
    else if (cursor->is_after_last())
      result += ",\n\tstate after-last (FPTA_NODATA)";
    else
      result += ",\n\tstate non-positioned (FPTA_ECURSOR)";

    const fpta_shove_t shove = cursor->index_shove();
    const fpta_index_type index = fpta_shove2index(shove);
    const fptu_type type = fpta_shove2type(shove);

    result += ",\n\t" + to_string(cursor->table_id) +
              fptu::format(",\n\tindex {@%" PRIx64 ".", shove) +
              to_string(index) + "." + to_string(type) +
              fptu::format(", col#%u, dbi#%u_%u},\n\trange-from-key ",
                           cursor->column_number, cursor->tbl_handle,
                           cursor->idx_handle) +
              to_string(cursor->range_from_key) + ",\n\trange-to-key " +
              to_string(cursor->range_to_key) + ",\n\tfilter " +
              to_string(cursor->filter) + ",\n\ttxn " + to_string(cursor->txn) +
              ",\n\tdb " + to_string(cursor->db) + "\n";
  }
  return result + "}";
}
}

int_fast32_t mrand64(void) {
  static uint_fast64_t state;
  state = state * UINT64_C(6364136223846793005) + UINT64_C(1442695040888963407);
  return (int_fast32_t)(state >> 32);
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

static __inline unsigned add_with_carry(uint64_t *sum, uint64_t addend) {
  *sum += addend;
  return *sum < addend;
}

uint64_t fpta_umul_64x64_128(uint64_t a, uint64_t b, uint64_t *h) {
#ifdef umul_64x64_128
  return umul_64x64_128(a, b, h);
#else
  /* performs 64x64 to 128 bit multiplication */
  uint64_t ll = umul_32x32_64((uint32_t)a, (uint32_t)b);
  uint64_t lh = umul_32x32_64(a >> 32, (uint32_t)b);
  uint64_t hl = umul_32x32_64((uint32_t)a, b >> 32);
  *h = umul_32x32_64(a >> 32, b >> 32) + (lh >> 32) + (hl >> 32) +
       add_with_carry(&ll, lh << 32) + add_with_carry(&ll, hl << 32);
  return ll;
#endif
}

uint64_t fpta_umul_32x32_64(uint32_t a, uint32_t b) {
  return umul_32x32_64(a, b);
}

uint64_t fpta_umul_64x64_high(uint64_t a, uint64_t b) {
  return umul_64x64_high(a, b);
}

//----------------------------------------------------------------------------

#ifdef __SANITIZE_ADDRESS__
extern "C" FPTA_API __attribute__((weak)) const char *__asan_default_options() {
  return "symbolize=1:allow_addr2line=1:"
#ifdef _DEBUG
         "debug=1:"
#endif /* _DEBUG */
         "report_globals=1:"
         "replace_str=1:replace_intrin=1:"
         "malloc_context_size=9:"
         "detect_leaks=1:"
         "check_printf=1:"
         "detect_deadlocks=1:"
#ifndef LTO_ENABLED
         "check_initialization_order=1:"
#endif
         "detect_stack_use_after_return=1:"
         "intercept_tls_get_addr=1:"
         "decorate_proc_maps=1:"
         "abort_on_error=1";
}
#endif /* __SANITIZE_ADDRESS__ */

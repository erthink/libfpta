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

static __hot fptu_lge fpta_cmp_null(const fptu_field *left) {
  auto payload = fptu_field_payload(left);

  switch (fptu_get_type(left->ct)) {
  case fptu_null:
    return fptu_eq;
  case fptu_opaque:
    if (payload->other.varlen.opaque_bytes == 0)
      return fptu_eq;
  default:
    return fptu_ic;
  }
}

static __hot fptu_lge fpta_cmp_sint(const fptu_field *left, int64_t right) {
  auto payload = fptu_field_payload(left);

  switch (fptu_get_type(left->ct)) {
  case fptu_uint16:
    return fptu_cmp2lge<int64_t>(left->get_payload_uint16(), right);

  case fptu_uint32:
    return fptu_cmp2lge<int64_t>(payload->u32, right);

  case fptu_int32:
    return fptu_cmp2lge<int64_t>(payload->i32, right);

  case fptu_uint64:
    if (right < 0)
      return fptu_gt;
    return fptu_cmp2lge(payload->u64, (uint64_t)right);

  case fptu_int64:
    return fptu_cmp2lge(payload->i64, right);

  case fptu_fp32:
    return fptu_cmp2lge<double>(payload->fp32, (double)right);

  case fptu_fp64:
    return fptu_cmp2lge<double>(payload->fp64, (double)right);

  default:
    return fptu_ic;
  }
}

static __hot fptu_lge fpta_cmp_uint(const fptu_field *left, uint64_t right) {
  auto payload = fptu_field_payload(left);

  switch (fptu_get_type(left->ct)) {
  case fptu_uint16:
    return fptu_cmp2lge<uint64_t>(left->get_payload_uint16(), right);

  case fptu_int32:
    if (payload->i32 < 0)
      return fptu_lt;
  case fptu_uint32:
    return fptu_cmp2lge<uint64_t>(payload->u32, right);

  case fptu_int64:
    if (payload->i64 < 0)
      return fptu_lt;
  case fptu_uint64:
    return fptu_cmp2lge(payload->u64, right);

  case fptu_fp32:
    return fptu_cmp2lge<double>(payload->fp32, (double)right);

  case fptu_fp64:
    return fptu_cmp2lge<double>(payload->fp64, (double)right);

  default:
    return fptu_ic;
  }
}

static __hot fptu_lge fpta_cmp_fp(const fptu_field *left, double right) {
  auto payload = fptu_field_payload(left);

  switch (fptu_get_type(left->ct)) {
  case fptu_uint16:
    return fptu_cmp2lge<double>(left->get_payload_uint16(), right);

  case fptu_int32:
    return fptu_cmp2lge<double>(payload->i32, right);

  case fptu_uint32:
    return fptu_cmp2lge<double>(payload->u32, right);

  case fptu_int64:
    return fptu_cmp2lge<double>((double)payload->i64, right);

  case fptu_uint64:
    return fptu_cmp2lge<double>((double)payload->u64, right);

  case fptu_fp32:
    return fptu_cmp2lge<double>(payload->fp32, right);

  case fptu_fp64:
    return fptu_cmp2lge(payload->fp64, right);

  default:
    return fptu_ic;
  }
}

static __hot fptu_lge fpta_cmp_datetime(const fptu_field *left,
                                        const fptu_time right) {

  if (unlikely(fptu_get_type(left->ct) != fptu_datetime))
    return fptu_ic;

  auto payload = fptu_field_payload(left);
  return fptu_cmp2lge(payload->u64, right.fixedpoint);
}

static __hot fptu_lge fpta_cmp_string(const fptu_field *left, const char *right,
                                      size_t length) {
  auto payload = fptu_field_payload(left);

  switch (fptu_get_type(left->ct)) {
  default:
    return fptu_ic;

  case fptu_cstr:
    return fptu_cmp_str_binary(payload->cstr, right, length);

  case fptu_opaque:
    return fptu_cmp_binary(payload->other.data,
                           payload->other.varlen.opaque_bytes, right, length);
  }
}

static __hot fptu_lge fpta_cmp_binary(const fptu_field *left,
                                      const void *right_data,
                                      size_t right_len) {
  auto payload = fptu_field_payload(left);
  const void *left_data = payload->fixbin;
  size_t left_len;

  switch (fptu_get_type(left->ct)) {
  case fptu_null:
    return (right_len == 0) ? fptu_eq : fptu_ic;

  case fptu_uint16:
  case fptu_uint32:
  case fptu_int32:
  case fptu_fp32:
  case fptu_uint64:
  case fptu_int64:
  case fptu_fp64:
  case fptu_datetime:
    return fptu_ic;

  case fptu_96:
    left_len = 12;
    break;
  case fptu_128:
    left_len = 16;
    break;
  case fptu_160:
    left_len = 20;
    break;
  case fptu_256:
    left_len = 32;
    break;

  case fptu_cstr:
    return fptu_cmp_str_binary(payload->cstr, right_data, right_len);

  case fptu_opaque:
    left_len = payload->other.varlen.opaque_bytes;
    left_data = payload->other.data;
    break;

  case fptu_nested:
  default: /* fptu_farray */
    left_len = units2bytes(payload->other.varlen.brutto);
    left_data = payload->other.data;
    break;
  }

  return fptu_cmp_binary(left_data, left_len, right_data, right_len);
}

//----------------------------------------------------------------------------

static __hot fptu_lge fpta_filter_cmp(const fptu_field *pf,
                                      const fpta_value &right) {
  if ((unlikely(pf == nullptr)))
    return (right.type == fpta_null) ? fptu_eq : fptu_ic;

  switch (right.type) {
  case fpta_null:
    return fpta_cmp_null(pf);

  case fpta_signed_int:
    return fpta_cmp_sint(pf, right.sint);

  case fpta_unsigned_int:
    return fpta_cmp_uint(pf, right.uint);

  case fpta_float_point:
    return fpta_cmp_fp(pf, right.fp);

  case fpta_datetime:
    return fpta_cmp_datetime(pf, right.datetime);

  case fpta_string:
    return fpta_cmp_string(pf, right.str, right.binary_length);

  case fpta_binary:
  case fpta_shoved:
    return fpta_cmp_binary(pf, right.binary_data, right.binary_length);

  default:
    assert(false);
    return fptu_ic;
  }
}

fptu_lge __fpta_filter_cmp(const fptu_field *pf, const fpta_value *right) {
  return fpta_filter_cmp(pf, *right);
}

__hot bool fpta_filter_match(fpta_filter *fn, fptu_ro tuple) {

tail_recursion:

  if (unlikely(fn == nullptr))
    // empty filter
    return true;

  switch (fn->type) {
  case fpta_node_not:
    return !fpta_filter_match(fn->node_not, tuple);

  case fpta_node_or:
    if (fpta_filter_match(fn->node_or.a, tuple))
      return true;
    fn = fn->node_or.b;
    goto tail_recursion;

  case fpta_node_and:
    if (!fpta_filter_match(fn->node_and.a, tuple))
      return false;
    fn = fn->node_and.b;
    goto tail_recursion;

  case fpta_node_fn:
    return fn->node_fn.predicate(
        fptu_lookup_ro(tuple, (unsigned)fn->node_fn.column_id->column.num,
                       fpta_id2type(fn->node_fn.column_id)),
        fn->node_fn.column_id, fn->node_fn.context, fn->node_fn.arg);

  default:
    int cmp_bits = fpta_filter_cmp(
        fptu_lookup_ro(tuple, (unsigned)fn->node_cmp.left_id->column.num,
                       fpta_id2type(fn->node_cmp.left_id)),
        fn->node_cmp.right_value);

    return (cmp_bits & fn->type) != 0;
  }
}

//----------------------------------------------------------------------------

bool fpta_filter_validate(const fpta_filter *filter) {
  (void)filter;
  // TODO
  return true;
}

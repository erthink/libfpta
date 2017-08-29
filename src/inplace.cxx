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

template <typename TYPE>
static TYPE confine_value(const fpta_value &value, const TYPE begin,
                          const TYPE end) {
  return (TYPE)confine_value(value, fptu::cast_wide(begin),
                             fptu::cast_wide(end));
}

template <>
uint64_t confine_value<uint64_t>(const fpta_value &value, const uint64_t begin,
                                 const uint64_t end) {
  assert(begin < end);
  switch (value.type) {
  default:
    assert(false);
    return begin;
  case fpta_signed_int:
    if (value.sint < 0)
      return begin;
  // no break here
  case fpta_unsigned_int:
    if (value.uint < begin)
      return begin;
    if (value.uint > end)
      return end;
    return value.uint;
  case fpta_float_point:
    if (value.fp < 0 || value.fp < (double_t)begin)
      return begin;
    if (value.fp > (double_t)end)
      return end;
    const uint64_t integer = uint64_t(value.fp);
    if (integer < begin)
      return begin;
    if (integer > end)
      return end;
    return integer;
  }
}

template <>
int64_t confine_value<int64_t>(const fpta_value &value, const int64_t begin,
                               const int64_t end) {
  assert(begin < end);
  switch (value.type) {
  default:
    assert(false);
    return 0;
  case fpta_unsigned_int:
    if (value.uint > (uint64_t)INT64_MAX)
      return end;
  // no break here
  case fpta_signed_int:
    if (value.sint < begin)
      return begin;
    if (value.sint > end)
      return end;
    return value.sint;
  case fpta_float_point:
    if (value.fp < (double_t)begin)
      return begin;
    if (value.fp > (double_t)end)
      return end;
    const int64_t integer = int64_t(value.fp);
    if (integer < begin)
      return begin;
    if (integer > end)
      return end;
    return integer;
  }
}

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4056) /* overflow in floating-point                  \
                                 * constant arithmetic */
#endif

template <>
double_t confine_value<double_t>(const fpta_value &value, const double_t begin,
                                 const double_t end) {
  assert(begin < end);
  (void)begin;
  (void)end;
  static_assert(std::numeric_limits<float>::has_infinity,
                "expects the float type has infinity with saturation");
  assert(std::isinf(float(FLT_MAX + FLT_MAX)) && 0 < float(FLT_MAX + FLT_MAX) &&
         std::isinf(float(0 - FLT_MAX - FLT_MAX)) &&
         0 > float(0 - FLT_MAX - FLT_MAX));
  static_assert(std::numeric_limits<double>::has_infinity,
                "expects the double type has infinity with saturation");
  assert(std::isinf(double(DBL_MAX + DBL_MAX)) &&
         0 < double(DBL_MAX + DBL_MAX) &&
         std::isinf(double(0 - DBL_MAX - DBL_MAX)) &&
         0 > double(0 - DBL_MAX - DBL_MAX));
  switch (value.type) {
  default:
    assert(false);
    return 0;
  case fpta_unsigned_int:
    return (double_t)value.uint;
  case fpta_signed_int:
    return (double_t)value.sint;
  case fpta_float_point:
    return value.fp;
  }
}

#ifdef _MSC_VER
#pragma warning(pop)
#endif

//----------------------------------------------------------------------------

template <fptu_type type> struct saturated {
  typedef numeric_traits<type> traits;
  typedef typename traits::native native;
  typedef typename traits::fast fast;
  typedef typename traits::native_limits native_limits;

  static fast bottom(const fpta_index_type index) {
    if (!fpta_index_is_nullable(index) || !native_limits::is_integer)
      return native_limits::lowest();
    const fast lower = native_limits::min();
    return (lower != traits::denil(index)) ? lower : lower + 1;
  }

  static fast top(const fpta_index_type index) {
    const fast upper = native_limits::max();
    if (!fpta_index_is_nullable(index) || !native_limits::is_integer)
      return upper;
    return (upper != traits::denil(index)) ? upper : upper - 1;
  }

  static int confine(const fpta_index_type index, fpta_value &value) {
    assert(value.is_number());
    value = traits::make_value(
        confine_value<fast>(value, bottom(index), top(index)));
    return FPTA_SUCCESS;
  }

  static bool min(const fpta_index_type index, const fptu_field *field,
                  const fpta_value &value, fast &result) {
    assert(value.is_number());
    const fast ones = confine_value(value, bottom(index), top(index));
    if (unlikely(!field)) {
      result = ones;
      return true;
    }

    const fast present = fptu::get_number<type, fast>(field);
    if (ones < present) {
      result = ones;
      return true;
    }
    return false;
  }

  static bool max(const fpta_index_type index, const fptu_field *field,
                  const fpta_value &value, fast &result) {
    assert(value.is_number());
    const fast ones = confine_value(value, bottom(index), top(index));
    if (unlikely(!field)) {
      result = ones;
      return true;
    }

    const fast present = fptu::get_number<type, fast>(field);
    if (ones > present) {
      result = ones;
      return true;
    }
    return false;
  }

  static bool add(const fpta_index_type index, const fptu_field *field,
                  const fpta_value &value, fast &result) {
    assert(value.is_number() && !value.is_negative());

    const fast lower = bottom(index);
    const fast upper = top(index);
    const fast addend = confine_value(value, (fast)0, upper);
    if (unlikely(!field)) {
      if (addend == 0 && lower > 0)
        return false /* не допускаем появления не-нулевого значения при добавлении нуля к пустоте */;
      result = confine_value(value, lower, upper);
      return true;
    }

    const fast present = fptu::get_number<type, fast>(field);
    if (addend == 0)
      return false;

    if (traits::has_native_saturation) {
      result = present + addend;
      return result != present;
    } else {
      if (present >= upper)
        return false;
      result = (upper - present > addend) ? present + addend : upper;
      return true;
    }
  }

  static bool sub(const fpta_index_type index, const fptu_field *field,
                  const fpta_value &value, fast &result) {
    assert(value.is_number() && !value.is_negative());

    const fast lower = bottom(index);
    const fast upper = top(index);
    const fast subtrahend = confine_value(value, (fast)0, upper);
    if (unlikely(!field)) {
      if (subtrahend == 0 && lower > 0)
        return false /* не допускаем появления не-нулевого значения при вычитании нуля из пустоты */;
      result = (traits::has_native_saturation || subtrahend + lower < 0)
                   ? 0 - subtrahend
                   : lower;
      return true;
    }

    const fast present = fptu::get_number<type, fast>(field);
    if (subtrahend == 0)
      return false;

    if (traits::has_native_saturation) {
      result = present - subtrahend;
      return result != present;
    } else {
      if (present <= lower)
        return false;
      result = (lower + subtrahend < present) ? present - subtrahend : lower;
      return true;
    }
  }

  static int inplace(const fpta_inplace op, const fpta_index_type index,
                     const fptu_field *field, fpta_value value, fast &result) {
    assert(value.is_number());

    switch (op) {
    default:
      return FPTA_EFLAG;
    case fpta_saturated_add:
      if (value.is_negative()) {
        value = -value;
        goto subtraction;
      }
    addition:
      if (!add(index, field, value, result))
        return FPTA_NODATA;
      break;
    case fpta_saturated_sub:
      if (value.is_negative()) {
        value = -value;
        goto addition;
      }
    subtraction:
      if (!sub(index, field, value, result))
        return FPTA_NODATA;
      break;
    case fpta_min:
      if (!min(index, field, value, result))
        return FPTA_NODATA;
      break;
    case fpta_max:
      if (!max(index, field, value, result))
        return FPTA_NODATA;
      break;
    case fpta_saturated_mul:
    case fpta_saturated_div:
    case fpta_bes:
      return FPTA_ENOIMP /* TODO */;
    }
    return FPTA_SUCCESS;
  }

  static int inplace(const fpta_inplace op, const fpta_index_type index,
                     fptu_field *field, fpta_value value, fptu_rw *row,
                     const unsigned colnum) {
    fast result;
    int rc = inplace(op, index, field, value, result);
    if (unlikely(rc != FPTA_SUCCESS))
      return rc;

    if (unlikely(field == nullptr))
      return fptu::upsert_number<type>(row, colnum, result);

    fptu::set_number<type>(field, result);
    return rc;
  }
};

//----------------------------------------------------------------------------

FPTA_API int fpta_confine_number(fpta_value *value, fpta_name *column_id) {
  if (unlikely(!value))
    return FPTA_EINVAL;
  int rc = fpta_id_validate(column_id, fpta_column_with_schema);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  const fptu_type coltype = fpta_shove2type(column_id->shove);
  const fpta_index_type index = fpta_name_colindex(column_id);
  if (unlikely((fptu_any_number & (INT32_C(1) << coltype)) == 0))
    return FPTA_ETYPE;

  switch (value->type) {
  case fpta_null:
    if (fpta_index_is_nullable(index))
      return FPTA_SUCCESS;
  // no break here
  default:
    return FPTA_EVALUE;
  case fpta_float_point:
    if (unlikely(std::isnan(value->fp))) {
      if ((fptu_any_fp & (INT32_C(1) << coltype)) == 0 ||
          FPTA_PROHIBIT_UPSERT_NAN)
        return FPTA_EVALUE;
      value->fp = std::nan("");
      return FPTA_SUCCESS;
    }
    if (coltype == fptu_fp64)
      return FPTA_SUCCESS;
  case fpta_signed_int:
  case fpta_unsigned_int:
    break;
  }

  switch (coltype) {
  default:
    assert(false);
    return FPTA_EOOPS;
  case fptu_uint16:
    return saturated<fptu_uint16>::confine(index, *value);
  case fptu_uint32:
    return saturated<fptu_uint32>::confine(index, *value);
  case fptu_uint64:
    return saturated<fptu_uint64>::confine(index, *value);
  case fptu_int32:
    return saturated<fptu_int32>::confine(index, *value);
  case fptu_int64:
    return saturated<fptu_int64>::confine(index, *value);
  case fptu_fp32:
    return saturated<fptu_fp32>::confine(index, *value);
  case fptu_fp64:
    return saturated<fptu_fp64>::confine(index, *value);
  }
}

FPTA_API int fpta_column_inplace(fptu_rw *row, const fpta_name *column_id,
                                 const fpta_inplace op, const fpta_value value,
                                 ...) {
  if (unlikely(!row))
    return FPTA_EINVAL;
  int rc = fpta_id_validate(column_id, fpta_column_with_schema);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;
  if (unlikely(op < fpta_saturated_add || op > fpta_bes))
    return FPTA_EFLAG;

  const unsigned colnum = column_id->column.num;
  assert(colnum <= fpta_max_cols);

  const fptu_type coltype = fpta_shove2type(column_id->shove);
  if (unlikely((fptu_any_number & (INT32_C(1) << coltype)) == 0))
    return FPTA_ETYPE;

  switch (value.type) {
  default:
    return FPTA_ETYPE;
  case fpta_float_point:
    if (likely(!std::isnan(value.fp)))
      break;
    if (FPTA_PROHIBIT_UPSERT_NAN)
      return FPTA_EVALUE;
  // no break here
  case fpta_null:
    return FPTA_NODATA /* silently ignore null-arg as no-op */;
  case fpta_signed_int:
  case fpta_unsigned_int:
    break;
  }

  const fpta_index_type index = fpta_name_colindex(column_id);
  fptu_field *field = fptu_lookup(row, colnum, coltype);
  switch (coltype) {
  default:
    assert(false);
    return FPTA_EOOPS;
  case fptu_uint16:
    return saturated<fptu_uint16>::inplace(op, index, field, value, row,
                                           colnum);
  case fptu_uint32:
    return saturated<fptu_uint32>::inplace(op, index, field, value, row,
                                           colnum);
  case fptu_uint64:
    return saturated<fptu_uint64>::inplace(op, index, field, value, row,
                                           colnum);
  case fptu_int32:
    return saturated<fptu_int32>::inplace(op, index, field, value, row, colnum);
  case fptu_int64:
    return saturated<fptu_int64>::inplace(op, index, field, value, row, colnum);
  case fptu_fp32:
    return saturated<fptu_fp32>::inplace(op, index, field, value, row, colnum);
  case fptu_fp64:
    return saturated<fptu_fp64>::inplace(op, index, field, value, row, colnum);
  }
}

//----------------------------------------------------------------------------

FPTA_API int fpta_cursor_inplace(fpta_cursor *cursor, fpta_name *column_id,
                                 const fpta_inplace op, const fpta_value value,
                                 ...) {
  if (unlikely(op < fpta_saturated_add || op > fpta_bes))
    return FPTA_EFLAG;

  int rc = fpta_cursor_validate(cursor, fpta_write);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  rc = fpta_name_refresh_couple(cursor->txn, cursor->table_id, column_id);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (unlikely(cursor->column_number == column_id->column.num))
    return FPTA_EINVAL;

  const fptu_type coltype = fpta_shove2type(column_id->shove);
  if (unlikely((fptu_any_number & (INT32_C(1) << coltype)) == 0))
    return FPTA_ETYPE;

  switch (value.type) {
  default:
    return FPTA_ETYPE;
  case fpta_float_point:
    if (likely(!std::isnan(value.fp)))
      break;
    if (FPTA_PROHIBIT_UPSERT_NAN)
      return FPTA_EVALUE;
  // no break here
  case fpta_null:
    return FPTA_NODATA /* silently ignore null-arg as no-op */;
  case fpta_signed_int:
  case fpta_unsigned_int:
    break;
  }

  fptu_ro source_row;
  rc = fpta_cursor_get(cursor, &source_row);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  union {
    numeric_traits<fptu_uint16>::fast uint16;
    numeric_traits<fptu_uint32>::fast uint32;
    numeric_traits<fptu_uint64>::fast uint64;
    numeric_traits<fptu_int32>::fast int32;
    numeric_traits<fptu_int64>::fast int64;
    numeric_traits<fptu_fp32>::fast fp32;
    numeric_traits<fptu_fp64>::fast fp64;
  } result;

  const unsigned colnum = column_id->column.num;
  const fpta_index_type index = fpta_name_colindex(column_id);
  const fptu_field *field = fptu_lookup_ro(source_row, colnum, coltype);

  switch (coltype) {
  default:
    assert(false);
    return FPTA_EOOPS;
  case fptu_uint16:
    rc =
        saturated<fptu_uint16>::inplace(op, index, field, value, result.uint16);
    break;
  case fptu_uint32:
    rc =
        saturated<fptu_uint32>::inplace(op, index, field, value, result.uint32);
    break;
  case fptu_uint64:
    rc =
        saturated<fptu_uint64>::inplace(op, index, field, value, result.uint64);
    break;
  case fptu_int32:
    rc = saturated<fptu_int32>::inplace(op, index, field, value, result.int32);
    break;
  case fptu_int64:
    rc = saturated<fptu_int64>::inplace(op, index, field, value, result.int64);
    break;
  case fptu_fp32:
    rc = saturated<fptu_fp32>::inplace(op, index, field, value, result.fp32);
    break;
  case fptu_fp64:
    rc = saturated<fptu_fp64>::inplace(op, index, field, value, result.fp64);
    break;
  }
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  const size_t buffer_size =
      fptu_get_buffer_size(source_row, field ? 0u : 1u, field ? 0u : 8u);
  void *const buffer = alloca(buffer_size);
  fptu_rw *changeable_row =
      fptu_fetch(source_row, buffer, buffer_size, field ? 0u : 1u);
  if (unlikely(changeable_row == nullptr))
    return FPTA_EOOPS;

  switch (coltype) {
  default:
    assert(false);
    return FPTA_EOOPS;
  case fptu_uint16:
    rc =
        fptu::upsert_number<fptu_uint16>(changeable_row, colnum, result.uint16);
    break;
  case fptu_uint32:
    rc =
        fptu::upsert_number<fptu_uint16>(changeable_row, colnum, result.uint32);
    break;
  case fptu_uint64:
    rc =
        fptu::upsert_number<fptu_uint16>(changeable_row, colnum, result.uint64);
    break;
  case fptu_int32:
    rc = fptu::upsert_number<fptu_uint16>(changeable_row, colnum, result.int32);
    break;
  case fptu_int64:
    rc = fptu::upsert_number<fptu_uint16>(changeable_row, colnum, result.int64);
    break;
  case fptu_fp32:
    rc = fptu::upsert_number<fptu_uint16>(changeable_row, colnum, result.fp32);
    break;
  case fptu_fp64:
    rc = fptu::upsert_number<fptu_uint16>(changeable_row, colnum, result.fp64);
    break;
  }
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  const fptu_ro modified_row = fptu_take(changeable_row);
  if (fpta_is_indexed(index) && fpta_index_is_unique(index)) {
    rc = fpta_cursor_validate_update(cursor, modified_row);
    if (unlikely(rc != FPTA_SUCCESS))
      return rc;
  }

  return fpta_cursor_update(cursor, modified_row);
}

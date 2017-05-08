/*
 * Copyright 2016-2017 libfptu authors: please see AUTHORS file.
 *
 * This file is part of libfptu, aka "Fast Positive Tuples".
 *
 * libfptu is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * libfptu is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with libfptu.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "fast_positive/tuples_internal.h"

fptu_type fptu_field_type(const fptu_field *pf) {
  if (unlikely(pf == nullptr))
    return fptu_null;

  return fptu_get_type(pf->ct);
}

int fptu_field_column(const fptu_field *pf) {
  if (unlikely(pf == nullptr || ct_is_dead(pf->ct)))
    return -1;

  return (int)fptu_get_col(pf->ct);
}

//----------------------------------------------------------------------------

uint16_t fptu_field_uint16(const fptu_field *pf) {
  if (unlikely(fptu_field_type(pf) != fptu_uint16))
    return FPTU_DENIL_UINT16;

  return pf->offset;
}

int32_t fptu_field_int32(const fptu_field *pf) {
  if (unlikely(fptu_field_type(pf) != fptu_int32))
    return FPTU_DENIL_INT32;

  return fptu_field_payload(pf)->i32;
}

uint32_t fptu_field_uint32(const fptu_field *pf) {
  if (unlikely(fptu_field_type(pf) != fptu_uint32))
    return FPTU_DENIL_UINT32;

  return fptu_field_payload(pf)->u32;
}

int64_t fptu_field_int64(const fptu_field *pf) {
  if (unlikely(fptu_field_type(pf) != fptu_int64))
    return FPTU_DENIL_INT64;

  return fptu_field_payload(pf)->i64;
}

uint64_t fptu_field_uint64(const fptu_field *pf) {
  if (unlikely(fptu_field_type(pf) != fptu_uint64))
    return FPTU_DENIL_UINT64;

  return fptu_field_payload(pf)->u64;
}

double fptu_field_fp64(const fptu_field *pf) {
  if (unlikely(fptu_field_type(pf) != fptu_fp64))
    return FPTU_DENIL_FP64;

  return fptu_field_payload(pf)->fp64;
}

float fptu_field_fp32(const fptu_field *pf) {
  if (unlikely(fptu_field_type(pf) != fptu_fp32))
    return FPTU_DENIL_FP32;

  return fptu_field_payload(pf)->fp32;
}

fptu_time fptu_field_datetime(const fptu_field *pf) {
  if (unlikely(fptu_field_type(pf) != fptu_datetime))
    return FPTU_DENIL_TIME;

  fptu_time result = {fptu_field_payload(pf)->u64};
  return result;
}

const char *fptu_field_cstr(const fptu_field *pf) {
  if (unlikely(fptu_field_type(pf) != fptu_cstr))
    return FPTU_DENIL_CSTR;

  return fptu_field_payload(pf)->cstr;
}

const uint8_t *fptu_field_96(const fptu_field *pf) {
  if (unlikely(fptu_field_type(pf) != fptu_96))
    return FPTU_DENIL_FIXBIN;

  return fptu_field_payload(pf)->fixbin;
}

const uint8_t *fptu_field_128(const fptu_field *pf) {
  if (unlikely(fptu_field_type(pf) != fptu_128))
    return FPTU_DENIL_FIXBIN;

  return fptu_field_payload(pf)->fixbin;
}

const uint8_t *fptu_field_160(const fptu_field *pf) {
  if (unlikely(fptu_field_type(pf) != fptu_160))
    return FPTU_DENIL_FIXBIN;

  return fptu_field_payload(pf)->fixbin;
}

const uint8_t *fptu_field_256(const fptu_field *pf) {
  if (unlikely(fptu_field_type(pf) != fptu_256))
    return FPTU_DENIL_FIXBIN;
  return fptu_field_payload(pf)->fixbin;
}

struct iovec fptu_field_opaque(const fptu_field *pf) {
  iovec io;
  if (unlikely(fptu_field_type(pf) != fptu_opaque)) {
    io.iov_base = FPTU_DENIL_FIXBIN;
    io.iov_len = 0;
  } else {
    const fptu_payload *payload = fptu_field_payload(pf);
    io.iov_len = payload->other.varlen.opaque_bytes;
    io.iov_base = (void *)payload->other.data;
  }
  return io;
}

struct iovec fptu_field_as_iovec(const fptu_field *pf) {
  struct iovec opaque;
  const fptu_payload *payload;
  unsigned type = (unsigned)fptu_field_type(pf);

  switch (type) {
  default:
    if (likely(type < fptu_farray)) {
      assert(type < fptu_cstr);
      opaque.iov_len = fptu_internal_map_t2b[type];
      opaque.iov_base = (void *)fptu_field_payload(pf);
      break;
    }
    // TODO: array support
    payload = fptu_field_payload(pf);
    opaque.iov_base = (void *)payload->other.data;
    opaque.iov_len = units2bytes(payload->other.varlen.brutto);
    break;
  case fptu_null:
    opaque.iov_base = nullptr;
    opaque.iov_len = 0;
    break;
  case fptu_uint16:
    opaque.iov_base = (void *)&pf->offset;
    opaque.iov_len = 2;
    break;
  case fptu_opaque:
    payload = fptu_field_payload(pf);
    opaque.iov_len = payload->other.varlen.opaque_bytes;
    opaque.iov_base = (void *)payload->other.data;
    break;
  case fptu_cstr:
    payload = fptu_field_payload(pf);
    opaque.iov_len = strlen(payload->cstr) + 1;
    opaque.iov_base = (void *)payload->cstr;
    break;
  case fptu_nested:
    payload = fptu_field_payload(pf);
    opaque.iov_len = units2bytes(payload->other.varlen.brutto + (size_t)1);
    opaque.iov_base = (void *)payload;
    break;
  }

  return opaque;
}

fptu_ro fptu_field_nested(const fptu_field *pf) {
  fptu_ro tuple;

  if (unlikely(fptu_field_type(pf) != fptu_nested)) {
    tuple.total_bytes = 0;
    tuple.units = nullptr;
    return tuple;
  }

  const fptu_payload *payload = fptu_field_payload(pf);
  tuple.total_bytes = units2bytes(payload->other.varlen.brutto + (size_t)1);
  tuple.units = (const fptu_unit *)payload;
  return tuple;
}

//----------------------------------------------------------------------------

uint16_t fptu_get_uint16(fptu_ro ro, unsigned column, int *error) {
  const fptu_field *pf = fptu_lookup_ro(ro, column, fptu_uint16);
  if (error)
    *error = pf ? FPTU_SUCCESS : FPTU_ENOFIELD;
  return fptu_field_uint16(pf);
}

int32_t fptu_get_int32(fptu_ro ro, unsigned column, int *error) {
  const fptu_field *pf = fptu_lookup_ro(ro, column, fptu_int32);
  if (error)
    *error = pf ? FPTU_SUCCESS : FPTU_ENOFIELD;
  return fptu_field_int32(pf);
}

uint32_t fptu_get_uint32(fptu_ro ro, unsigned column, int *error) {
  const fptu_field *pf = fptu_lookup_ro(ro, column, fptu_uint32);
  if (error)
    *error = pf ? FPTU_SUCCESS : FPTU_ENOFIELD;
  return fptu_field_uint32(pf);
}

int64_t fptu_get_int64(fptu_ro ro, unsigned column, int *error) {
  const fptu_field *pf = fptu_lookup_ro(ro, column, fptu_int64);
  if (error)
    *error = pf ? FPTU_SUCCESS : FPTU_ENOFIELD;
  return fptu_field_int64(pf);
}

uint64_t fptu_get_uint64(fptu_ro ro, unsigned column, int *error) {
  const fptu_field *pf = fptu_lookup_ro(ro, column, fptu_uint64);
  if (error)
    *error = pf ? FPTU_SUCCESS : FPTU_ENOFIELD;
  return fptu_field_uint64(pf);
}

double fptu_get_fp64(fptu_ro ro, unsigned column, int *error) {
  const fptu_field *pf = fptu_lookup_ro(ro, column, fptu_fp64);
  if (error)
    *error = pf ? FPTU_SUCCESS : FPTU_ENOFIELD;
  return fptu_field_fp64(pf);
}

float fptu_get_fp32(fptu_ro ro, unsigned column, int *error) {
  const fptu_field *pf = fptu_lookup_ro(ro, column, fptu_fp32);
  if (error)
    *error = pf ? FPTU_SUCCESS : FPTU_ENOFIELD;
  return fptu_field_fp32(pf);
}

//----------------------------------------------------------------------------

int64_t fptu_get_sint(fptu_ro ro, unsigned column, int *error) {
  const fptu_field *pf = fptu_lookup_ro(ro, column, fptu_any_int);
  if (error)
    *error = pf ? FPTU_SUCCESS : FPTU_ENOFIELD;

  if (likely(pf)) {
    switch (fptu_get_type(pf->ct)) {
    case fptu_int32:
      return fptu_field_payload(pf)->i32;
    case fptu_int64:
      return fptu_field_payload(pf)->i64;
    default:
      assert(false && "unexpected field type");
      __unreachable();
    }
  }
  return FPTU_DENIL_INT64;
}

uint64_t fptu_get_uint(fptu_ro ro, unsigned column, int *error) {
  const fptu_field *pf = fptu_lookup_ro(ro, column, fptu_any_uint);
  if (error)
    *error = pf ? FPTU_SUCCESS : FPTU_ENOFIELD;

  if (likely(pf)) {
    switch (fptu_get_type(pf->ct)) {
    case fptu_uint16:
      return pf->get_payload_uint16();
    case fptu_uint32:
      return fptu_field_payload(pf)->u32;
    case fptu_uint64:
      return fptu_field_payload(pf)->u64;
    default:
      assert(false && "unexpected field type");
      __unreachable();
    }
  }
  return FPTU_DENIL_UINT64;
}

double fptu_get_fp(fptu_ro ro, unsigned column, int *error) {
  const fptu_field *pf = fptu_lookup_ro(ro, column, fptu_any_fp);
  if (error)
    *error = pf ? FPTU_SUCCESS : FPTU_ENOFIELD;

  if (likely(pf)) {
    switch (fptu_get_type(pf->ct)) {
    case fptu_fp32:
      return fptu_field_payload(pf)->fp32;
    case fptu_fp64:
      return fptu_field_payload(pf)->fp64;
    default:
      assert(false && "unexpected field type");
      __unreachable();
    }
  }
  return FPTU_DENIL_FP64;
}

fptu_time fptu_get_datetime(fptu_ro ro, unsigned column, int *error) {
  const fptu_field *pf = fptu_lookup_ro(ro, column, fptu_datetime);
  if (error)
    *error = pf ? FPTU_SUCCESS : FPTU_ENOFIELD;

  return fptu_field_datetime(pf);
}

//----------------------------------------------------------------------------

const uint8_t *fptu_get_96(fptu_ro ro, unsigned column, int *error) {
  const fptu_field *pf = fptu_lookup_ro(ro, column, fptu_96);
  if (error)
    *error = pf ? FPTU_SUCCESS : FPTU_ENOFIELD;
  return fptu_field_96(pf);
}

const uint8_t *fptu_get_128(fptu_ro ro, unsigned column, int *error) {
  const fptu_field *pf = fptu_lookup_ro(ro, column, fptu_128);
  if (error)
    *error = pf ? FPTU_SUCCESS : FPTU_ENOFIELD;
  return fptu_field_128(pf);
}

const uint8_t *fptu_get_160(fptu_ro ro, unsigned column, int *error) {
  const fptu_field *pf = fptu_lookup_ro(ro, column, fptu_160);
  if (error)
    *error = pf ? FPTU_SUCCESS : FPTU_ENOFIELD;
  return fptu_field_160(pf);
}

const uint8_t *fptu_get_256(fptu_ro ro, unsigned column, int *error) {
  const fptu_field *pf = fptu_lookup_ro(ro, column, fptu_256);
  if (error)
    *error = pf ? FPTU_SUCCESS : FPTU_ENOFIELD;
  return fptu_field_256(pf);
}

const char *fptu_get_cstr(fptu_ro ro, unsigned column, int *error) {
  const fptu_field *pf = fptu_lookup_ro(ro, column, fptu_cstr);
  if (error)
    *error = pf ? FPTU_SUCCESS : FPTU_ENOFIELD;
  return fptu_field_cstr(pf);
}

struct iovec fptu_get_opaque(fptu_ro ro, unsigned column, int *error) {
  const fptu_field *pf = fptu_lookup_ro(ro, column, fptu_opaque);
  if (error)
    *error = pf ? FPTU_SUCCESS : FPTU_ENOFIELD;
  return fptu_field_opaque(pf);
}

fptu_ro fptu_get_nested(fptu_ro ro, unsigned column, int *error) {
  const fptu_field *pf = fptu_lookup_ro(ro, column, fptu_nested);
  if (error)
    *error = pf ? FPTU_SUCCESS : FPTU_ENOFIELD;
  return fptu_field_nested(pf);
}

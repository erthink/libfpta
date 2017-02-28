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
#include <cinttypes> // for PRId64, PRIu64
#include <cmath>     // for exp2()
#include <cstdarg>   // for va_list
#include <cstdio>    // for _vscprintf()
#include <cstdlib>   // for snprintf()
#include <ctime>     // for gmtime()

#if defined(_WIN32) || defined(_WIN64)
__extern_C __declspec(dllimport) __noreturn
    void __cdecl _assert(char const *message, char const *filename,
                         unsigned line);

void __assert_fail(const char *assertion, const char *filename, unsigned line,
                   const char *function) {
  (void)function;
  _assert(assertion, filename, line);
  abort();
}
#endif /* windows mustdie */

namespace fptu {

__cold std::string format(const char *fmt, ...) {
  va_list ap, ones;
  va_start(ap, fmt);
  va_copy(ones, ap);
#ifdef _MSC_VER
  int needed = _vscprintf(fmt, ap);
#else
  int needed = vsnprintf(nullptr, 0, fmt, ap);
#endif
  assert(needed >= 0);
  va_end(ap);
  std::string result;
  result.reserve((size_t)needed + 1);
  result.resize((size_t)needed, '\0');
#ifdef _MSC_VER
  int actual = vsnprintf_s((char *)result.data(), result.capacity(),
                           _TRUNCATE, fmt, ones);
#else
  int actual = vsnprintf((char *)result.data(), result.capacity(), fmt, ones);
#endif
  assert(actual == needed);
  (void)actual;
  va_end(ones);
  return result;
}

__cold std::string hexadecimal(const void *data, size_t bytes) {
  std::string result;
  if (bytes > 0) {
    result.reserve(bytes * 2);
    const uint8_t *ptr = (const uint8_t *)data;
    const uint8_t *const end = ptr + bytes;
    do {
      char high = *ptr >> 4;
      char low = *ptr & 15;
      result.push_back((high < 10) ? high + '0' : high - 10 + 'a');
      result.push_back((low < 10) ? low + '0' : low - 10 + 'a');
    } while (++ptr < end);
  }
  return result;
}

} /* namespace fptu */

//----------------------------------------------------------------------------

__cold const char *fptu_type_name(const fptu_type type) {
  switch ((int /* hush 'not in enumerated' */)type) {
  default: {
    static __thread char buf[16];
#ifdef _MSC_VER
    _snprintf_s(buf, sizeof(buf), _TRUNCATE,
#else
    snprintf(buf, sizeof(buf),
#endif
                "invalid(fptu_type)%i", (int)type);
    return buf;
  }
  case fptu_null:
    return "null";
  case fptu_uint16:
    return "uint16";
  case fptu_int32:
    return "int32";
  case fptu_uint32:
    return "uint32";
  case fptu_fp32:
    return "fp32";
  case fptu_int64:
    return "int64";
  case fptu_uint64:
    return "uint64";
  case fptu_fp64:
    return "fp64";
  case fptu_datetime:
    return "datetime";
  case fptu_96:
    return "b96";
  case fptu_128:
    return "b128";
  case fptu_160:
    return "b160";
  case fptu_256:
    return "b256";
  case fptu_cstr:
    return "cstr";
  case fptu_opaque:
    return "opaque";
  case fptu_nested:
    return "nested";

  case fptu_null | fptu_farray:
    return "invalid (null[])";
  case fptu_uint16 | fptu_farray:
    return "uint16[]";
  case fptu_int32 | fptu_farray:
    return "int32[]";
  case fptu_uint32 | fptu_farray:
    return "uint32[]";
  case fptu_fp32 | fptu_farray:
    return "fp32[]";
  case fptu_int64 | fptu_farray:
    return "int64[]";
  case fptu_uint64 | fptu_farray:
    return "uint64[]";
  case fptu_fp64 | fptu_farray:
    return "fp64[]";
  case fptu_datetime | fptu_farray:
    return "datetime[]";
  case fptu_96 | fptu_farray:
    return "b96[]";
  case fptu_128 | fptu_farray:
    return "b128[]";
  case fptu_160 | fptu_farray:
    return "b160[]";
  case fptu_256 | fptu_farray:
    return "b256[]";
  case fptu_cstr | fptu_farray:
    return "cstr[]";
  case fptu_opaque | fptu_farray:
    return "opaque[]";
  case fptu_nested | fptu_farray:
    return "nested[]";
  }
}

namespace std {

__cold string to_string(const fptu_error error) {
  switch (error) {
  case FPTU_SUCCESS:
    return "FPTU_SUCCESS";
  case FPTU_ENOFIELD:
    return "FPTU_ENOFIELD";
  case FPTU_EINVAL:
    return "FPTU_EINVAL";
  case FPTU_ENOSPACE:
    return "FPTU_ENOSPACE";
  default:
    return fptu::format("invalid(fptu_error)%i", (int)error);
  }
}

template <typename native>
static inline std::string
array2str_native(unsigned ct, const fptu_payload *payload, const char *name,
                 const char *comma_fmt) {
  std::string result =
      fptu::format("{%u.%s[%u(%zu)]=", fptu_get_col(ct), name,
                   payload->other.varlen.array_length,
                   units2bytes(payload->other.varlen.brutto));

  const native *array = (const native *)&payload->other.data[1];
  for (unsigned i = 0; i < payload->other.varlen.array_length; ++i)
    result += fptu::format(&comma_fmt[i == 0], array[i]);

  return result + "}";
}

static std::string array2str_fixbin(unsigned ct, const fptu_payload *payload,
                                    const char *name, unsigned itemsize) {
  std::string result =
      fptu::format("{%u.%s[%u(%zu)]=", fptu_get_col(ct), name,
                   payload->other.varlen.array_length,
                   units2bytes(payload->other.varlen.brutto));

  const uint8_t *array = (const uint8_t *)&payload->other.data[1];
  for (unsigned i = 0; i < payload->other.varlen.array_length; ++i) {
    if (i)
      result += ",";
    result += fptu::hexadecimal(array, itemsize);
    array += itemsize;
  }

  return result + "}";
}

__cold string to_string(const fptu_field &field) {
  const auto type = fptu_get_type(field.ct);
  auto payload = fptu_field_payload(&field);
  switch ((int /* hush 'not in enumerated' */)type) {
  default:
  case fptu_null:
    return fptu::format("{%u.%s}", fptu_get_col(field.ct),
                        fptu_type_name(type));
  case fptu_uint16:
    return fptu::format("{%u.%s=%u}", fptu_get_col(field.ct),
                        fptu_type_name(type),
                        (unsigned)field.get_payload_uint16());
  case fptu_int32:
    return fptu::format("{%u.%s=%" PRId32 "}", fptu_get_col(field.ct),
                        fptu_type_name(type), payload->i32);
  case fptu_uint32:
    return fptu::format("{%u.%s=%" PRIu32 "}", fptu_get_col(field.ct),
                        fptu_type_name(type), payload->u32);
  case fptu_fp32:
    return fptu::format("{%u.%s=%g}", fptu_get_col(field.ct),
                        fptu_type_name(type), payload->fp32);
  case fptu_int64:
    return fptu::format("{%u.%s=%" PRId64 "}", fptu_get_col(field.ct),
                        fptu_type_name(type), payload->i64);
  case fptu_uint64:
    return fptu::format("{%u.%s=%" PRIu64 "}", fptu_get_col(field.ct),
                        fptu_type_name(type), payload->u64);
  case fptu_fp64:
    return fptu::format("{%u.%s=%.12g}", fptu_get_col(field.ct),
                        fptu_type_name(type), payload->fp64);

  case fptu_datetime:
    return fptu::format("{%u.%s=", fptu_get_col(field.ct),
                        fptu_type_name(type)) +
           to_string(payload->dt) + '}';

  case fptu_96:
    return fptu::format("{%u.%s=", fptu_get_col(field.ct),
                        fptu_type_name(type)) +
           fptu::hexadecimal(payload->fixbin, 96 / 8) + '}';

  case fptu_128:
    return fptu::format("{%u.%s=", fptu_get_col(field.ct),
                        fptu_type_name(type)) +
           fptu::hexadecimal(payload->fixbin, 128 / 8) + '}';

  case fptu_160:
    return fptu::format("{%u.%s=", fptu_get_col(field.ct),
                        fptu_type_name(type)) +
           fptu::hexadecimal(payload->fixbin, 160 / 8) + '}';

  case fptu_256:
    return fptu::format("{%u.%s=", fptu_get_col(field.ct),
                        fptu_type_name(type)) +
           fptu::hexadecimal(payload->fixbin, 256 / 8) + '}';

  case fptu_cstr:
    return fptu::format("{%u.%s=%s}", fptu_get_col(field.ct),
                        fptu_type_name(type), payload->cstr);

  case fptu_opaque:
    return fptu::format("{%u.%s=", fptu_get_col(field.ct),
                        fptu_type_name(type)) +
           fptu::hexadecimal(payload->other.data,
                             payload->other.varlen.opaque_bytes) +
           '}';
    break;

  case fptu_nested:
    return fptu::format("{%u.%s=", fptu_get_col(field.ct),
                        fptu_type_name(type)) +
           std::to_string(fptu_field_nested(&field)) + "}";

  case fptu_null | fptu_farray:
    return fptu::format("{%u.invalid-null[%u(%zu)]}", fptu_get_col(field.ct),
                        payload->other.varlen.array_length,
                        units2bytes(payload->other.varlen.brutto));

  case fptu_uint16 | fptu_farray:
    return array2str_native<int16_t>(field.ct, payload, "uint16", ",%u");
  case fptu_int32 | fptu_farray:
    return array2str_native<int32_t>(field.ct, payload, "int32", ",%" PRId32);
  case fptu_uint32 | fptu_farray:
    return array2str_native<uint32_t>(field.ct, payload, "uint32",
                                      ",%" PRIu32);
  case fptu_fp32 | fptu_farray:
    return array2str_native<float>(field.ct, payload, "fp32", ",%g");
  case fptu_int64 | fptu_farray:
    return array2str_native<int64_t>(field.ct, payload, "int64", ",%" PRId64);
  case fptu_uint64 | fptu_farray:
    return array2str_native<uint64_t>(field.ct, payload, "uint64",
                                      ",%" PRIu64);
  case fptu_fp64 | fptu_farray:
    return array2str_native<double>(field.ct, payload, "fp64", ",%.12g");

  case fptu_datetime | fptu_farray: {
    std::string result =
        fptu::format("{%u.%s[%u(%zu)]=", fptu_get_col(field.ct), "datetime",
                     payload->other.varlen.array_length,
                     units2bytes(payload->other.varlen.brutto));

    const fptu_time *array = (const fptu_time *)&payload->other.data[1];
    for (unsigned i = 0; i < payload->other.varlen.array_length; ++i) {
      if (i)
        result += ",";
      result += to_string(array[i]);
    }
    return result + "}";
  }

  case fptu_96 | fptu_farray:
    return array2str_fixbin(field.ct, payload, "b96", 96 / 8);
  case fptu_128 | fptu_farray:
    return array2str_fixbin(field.ct, payload, "b128", 128 / 8);
  case fptu_160 | fptu_farray:
    return array2str_fixbin(field.ct, payload, "b160", 160 / 8);
  case fptu_256 | fptu_farray:
    return array2str_fixbin(field.ct, payload, "b256", 256 / 8);

  case fptu_cstr | fptu_farray: {
    std::string result =
        fptu::format("{%u.%s[%u(%zu)]=", fptu_get_col(field.ct), "cstr",
                     payload->other.varlen.array_length,
                     units2bytes(payload->other.varlen.brutto));

    const char *array = (const char *)&payload->other.data[1];
    for (unsigned i = 0; i < payload->other.varlen.array_length; ++i) {
      result += fptu::format(&",%s"[i == 0], array);
      array += strlen(array) + 1;
    }
    return result + "}";
  }

  case fptu_opaque | fptu_farray: {
    std::string result =
        fptu::format("{%u.%s[%u(%zu)]=", fptu_get_col(field.ct), "opaque",
                     payload->other.varlen.array_length,
                     units2bytes(payload->other.varlen.brutto));

    const fptu_unit *array = (const fptu_unit *)&payload->other.data[1];
    for (unsigned i = 0; i < payload->other.varlen.array_length; ++i) {
      if (i)
        result += ",";
      result += fptu::hexadecimal(array + 1, array->varlen.opaque_bytes);
      array += array->varlen.brutto + 1;
    }
    return result + "}";
  }

  case fptu_nested | fptu_farray: {
    std::string result =
        fptu::format("{%u.%s[%u(%zu)]=", fptu_get_col(field.ct), "nested",
                     payload->other.varlen.array_length,
                     units2bytes(payload->other.varlen.brutto));

    const fptu_unit *array = (const fptu_unit *)&payload->other.data[1];
    for (unsigned i = 0; i < payload->other.varlen.array_length; ++i) {
      fptu_ro nested;
      nested.total_bytes = units2bytes(array->varlen.brutto + (size_t)1);
      nested.units = array;

      if (i)
        result += ",";
      result += to_string(nested);
      array += array->varlen.brutto + 1;
    }
    return result + "}";
  }
  }
}

__cold string to_string(const fptu_type type) {
  return string(fptu_type_name(type));
}

__cold string to_string(const fptu_ro &ro) {
  const fptu_field *const begin = fptu::begin(ro);
  const fptu_field *const end = fptu::end(ro);
  string result = fptu::format("(%zi bytes, %ti fields, %p)={",
                               ro.total_bytes, end - begin, ro.units);
  for (auto i = begin; i != end; ++i) {
    if (i != begin)
      result.append(", ");
    result.append(to_string(*i));
  }
  result.push_back('}');
  return result;
}

__cold string to_string(const fptu_rw &rw) {
  const void *addr = std::addressof(rw);
  const fptu_field *const begin = fptu::begin(rw);
  const fptu_field *const end = fptu::end(rw);
  string result =
      fptu::format("(%p, %ti fields, %zu bytes, %zu junk, %zu/%zu space, "
                   "H%u_P%u_T%u_E%u)={",
                   addr, end - begin, units2bytes(rw.tail - rw.head),
                   fptu_junkspace(&rw), fptu_space4items(&rw),
                   fptu_space4data(&rw), rw.head, rw.pivot, rw.tail, rw.end);

  for (auto i = begin; i != end; ++i) {
    if (i != begin)
      result.append(", ");
    result.append(to_string(*i));
  }
  result.push_back('}');
  return result;
}

__cold string to_string(const fptu_lge lge) {
  switch (lge) {
  default:
    return fptu::format("invalid(fptu_lge)%i", (int)lge);
  case fptu_ic:
    return "><";
  case fptu_eq:
    return "==";
  case fptu_lt:
    return "<";
  case fptu_gt:
    return ">";
  case fptu_ne:
    return "!=";
  case fptu_le:
    return "<=";
  case fptu_ge:
    return ">=";
  }
}

__cold string to_string(const fptu_time &time) {
  const double scale = exp2(-32);
  char fractional[16];
#ifdef _MSC_VER
  _snprintf_s(fractional, sizeof(fractional), _TRUNCATE,
#else
  snprintf(fractional, sizeof(fractional),
#endif
              "%.9f", scale * (uint32_t)time.fixedpoint);
  assert(fractional[0] == '0' || fractional[0] == '1');

  time_t utc_sec = (time_t)(time.fixedpoint >> 32);
  if (fractional[0] == '1')
    /* учитываем перенос при округлении fractional */
    utc_sec += 1;

  struct tm utc_tm;
#ifdef _MSC_VER
  gmtime_s(&utc_tm, &utc_sec);
#else
  gmtime_r(&utc_sec, &utc_tm);
#endif

  char datetime[32];
#ifdef _MSC_VER
  _snprintf_s(datetime, sizeof(datetime), _TRUNCATE,
#else
  snprintf(datetime, sizeof(datetime),
#endif
              "%04d-%02d-%02d_%02d:%02d:%02d", utc_tm.tm_year + 1900,
              utc_tm.tm_mon + 1, utc_tm.tm_mday, utc_tm.tm_hour,
              utc_tm.tm_min, utc_tm.tm_sec);
  return string(datetime) + (fractional + /* skip leading */ 1);
}

/* #define FIXME "FIXME: " __FILE__ ", " FPT_STRINGIFY(__LINE__) */

} /* namespace std */

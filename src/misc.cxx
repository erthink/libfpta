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
#include <cmath>     // for exp2()
#include <cstdarg>   // for va_list
#include <cstdio>    // for _vscprintf()

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
#ifdef MSVC
  int needed = _vscprintf(fmt, ap);
#else
  int needed = vsnprintf(nullptr, 0, fmt, ap);
#endif
  assert(needed >= 0);
  va_end(ap);
  std::string result;
  result.reserve((size_t)needed + 1);
  result.resize(needed, '\0');
#ifdef MSVC
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

#define FIXME "FIXME: " __FILE__ ", " FPT_STRINGIFY(__LINE__)

namespace std {

__cold string to_string(fptu_error) { return FIXME; }

__cold string to_string(const fptu_varlen &) { return FIXME; }

__cold string to_string(const fptu_unit &) { return FIXME; }

__cold string to_string(const fptu_field &) { return FIXME; }

__cold string to_string(fptu_type type) {
  switch ((int /* hush 'not in enumerated' */)type) {
  default:
    return "invalid(fptu_type)" + to_string((int)type);

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

__cold string to_string(const fptu_rw &) { return FIXME; }
__cold string to_string(const fptu_ro &) { return FIXME; }

__cold string to_string(fptu_lge lge) {
  switch (lge) {
  default:
    return "invalid(fptu_lge)" + to_string((int)lge);
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
  snprintf(fractional, sizeof(fractional), "%.9f",
           scale * (uint32_t)time.fixedpoint);
  assert(fractional[0] == '0' || fractional[0] == '1');

  time_t utc_sec = time.fixedpoint >> 32;
  if (fractional[0] == '1')
    /* учитываем перенос при округлении fractional */
    utc_sec += 1;

  struct tm utc_tm;
  gmtime_r(&utc_sec, &utc_tm);

  char datetime[32];
  snprintf(datetime, sizeof(datetime), "%04d-%02d-%02d_%02d:%02d:%02d",
           utc_tm.tm_year + 1900, utc_tm.tm_mon + 1, utc_tm.tm_mday,
           utc_tm.tm_hour, utc_tm.tm_min, utc_tm.tm_sec);
  return string(datetime) + (fractional + /* skip leading */ 1);
}
} /* namespace std */

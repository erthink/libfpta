/*
 * Copyright 2016-2018 libfptu authors: please see AUTHORS file.
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

#define NSEC_PER_SEC 1000000000u
uint_fast32_t fptu_time::ns2fractional(uint_fast32_t ns) {
  assert(ns < NSEC_PER_SEC);
  /* LY: здесь и далее используется "длинное деление", которое
   * для ясности кода оставлено как есть (без ручной оптимизации). Так как
   * GCC, Clang и даже MSVC сами давно умеют конвертировать деление на
   * константу в быструю reciprocal-форму. */
  return ((uint64_t)ns << 32) / NSEC_PER_SEC;
}

uint_fast32_t fptu_time::fractional2ns(uint_fast32_t fractional) {
  return (fractional * (uint64_t)NSEC_PER_SEC) >> 32;
}

#define USEC_PER_SEC 1000000u
uint_fast32_t fptu_time::us2fractional(uint_fast32_t us) {
  assert(us < USEC_PER_SEC);
  return ((uint64_t)us << 32) / USEC_PER_SEC;
}

uint_fast32_t fptu_time::fractional2us(uint_fast32_t fractional) {
  return (fractional * (uint64_t)USEC_PER_SEC) >> 32;
}

#define MSEC_PER_SEC 1000u
uint_fast32_t fptu_time::ms2fractional(uint_fast32_t ms) {
  assert(ms < MSEC_PER_SEC);
  return ((uint64_t)ms << 32) / MSEC_PER_SEC;
}

uint_fast32_t fptu_time::fractional2ms(uint_fast32_t fractional) {
  return (fractional * (uint64_t)MSEC_PER_SEC) >> 32;
}

//----------------------------------------------------------------------------

#if defined(_WIN32) || defined(_WIN64) || defined(_WINDOWS)
#pragma warning(push, 1)
#include <windows.h>
#pragma warning(pop)
#pragma warning(disable : 4191) /* unsafe conversion from 'FARPROC' to ... */

#define N100SEC_PER_SEC (NSEC_PER_SEC / 100u)
static __inline fptu_time from_filetime(FILETIME *pFileTime) {
  const uint64_t ns100 =
      ((uint64_t)pFileTime->dwHighDateTime << 32) + pFileTime->dwLowDateTime -
      UINT64_C(/* UTC offset from 1601-01-01 */ 116444736000000000);

  fptu_time result;
  result.utc = (uint32_t)(ns100 / N100SEC_PER_SEC);
  result.fractional =
      (uint32_t)(((ns100 % N100SEC_PER_SEC) << 32) / N100SEC_PER_SEC);
  return result;
}

#elif !defined(HAVE_TIMESPEC_TV_NSEC)
#error FIXME: HAVE_TIMESPEC_TV_NSEC?
#endif /* ! WINDOWS */

fptu_time fptu_now_fine(void) {
#if defined(_WIN32) || defined(_WIN64) || defined(_WINDOWS)
  static void(WINAPI * query_time)(LPFILETIME);
  if (!query_time) {
    query_time = (void(WINAPI *)(LPFILETIME))GetProcAddress(
        GetModuleHandle(TEXT("kernel32.dll")),
        "GetSystemTimePreciseAsFileTime");
    if (!query_time)
      query_time = GetSystemTimeAsFileTime;
  }

  FILETIME filetime;
  query_time(&filetime);
  return from_filetime(&filetime);
#else  /* WINDOWS */
  struct timespec now;
  int rc = clock_gettime(CLOCK_REALTIME, &now);
  if (unlikely(rc != 0))
    __assert_fail("clock_gettime() failed", "fptu/time.cxx", __LINE__,
                  __func__);
  return fptu_time::from_timespec(now);
#endif /* ! WINDOWS */
}

#if defined(_WIN32) || defined(_WIN64) || defined(_WINDOWS)
static uint32_t coarse_resolution_ns;
#elif defined(CLOCK_REALTIME_COARSE)
static clockid_t coarse_clockid;
static uint_fast32_t coarse_resolution_ns;

static void __attribute__((constructor)) fptu_clock_init(void) {
  struct timespec resolution;
  /* LY: Получаем точность coarse-таймеров, одновременно проверяя
   * поддерживается ли CLOCK_REALTIME_COARSE ядром.
   * Если нет, то используем вместо него CLOCK_REALTIME.  */
  if (clock_getres(CLOCK_REALTIME_COARSE, &resolution) == 0 &&
      resolution.tv_sec == 0) {
    coarse_clockid = CLOCK_REALTIME_COARSE;
    coarse_resolution_ns = resolution.tv_nsec;
  } else {
    coarse_clockid = CLOCK_REALTIME;
    coarse_resolution_ns = 0xffffFFFF;
  }
}
#else /* CLOCK_REALTIME_COARSE */
/* LY: Если CLOCK_REALTIME_COARSE не определен, то в наличии
 * дремучая версия glibc. В этом случае не пытаемся использовать
 * CLOCK_REALTIME_COARSE, а просто ставим заглушку. */
#define coarse_clockid CLOCK_REALTIME
#define coarse_resolution_ns 0xffffFFFF
#endif /* ! CLOCK_REALTIME_COARSE */

fptu_time fptu_now_coarse(void) {
#if defined(_WIN32) || defined(_WIN64) || defined(_WINDOWS)
  FILETIME filetime;
  GetSystemTimeAsFileTime(&filetime);
  return from_filetime(&filetime);
#elif defined(CLOCK_REALTIME_COARSE)
  struct timespec now;
  int rc = clock_gettime(coarse_clockid, &now);
  if (unlikely(rc != 0))
    __assert_fail("clock_gettime() failed", "fptu/time.cxx", __LINE__,
                  __func__);

  return fptu_time::from_timespec(now);
#else  /* CLOCK_REALTIME_COARSE */
  return fptu_now_fine();
#endif /* ! CLOCK_REALTIME_COARSE */
}

fptu_time fptu_now(int grain_ns) {
  uint_fast32_t mask = 0xffffFFFF;
  uint_fast32_t grain = (uint32_t)grain_ns;
  if (grain_ns < 0) {
    if (likely(grain_ns > -32)) {
      mask <<= -grain_ns;
      grain = 1u << -grain_ns;
    } else {
      mask = 0;
      grain = ~mask;
    }
  }

#if defined(_WIN32) || defined(_WIN64) || defined(_WINDOWS)
  if (!coarse_resolution_ns) {
    NTSTATUS(NTAPI * query_resolution)(PULONG, PULONG, PULONG);
    query_resolution =
        (NTSTATUS(NTAPI *)(PULONG, PULONG, PULONG))GetProcAddress(
            GetModuleHandle(TEXT("ntdll.dll")), "NtQueryTimerResolution");

    ULONG min_100ns, max_100ns, actual_100ns;
    if (!query_resolution ||
        query_resolution(&min_100ns, &max_100ns, &actual_100ns) < 0)
      min_100ns = max_100ns = actual_100ns = 156250;
    coarse_resolution_ns =
        100u * ((min_100ns > max_100ns) ? min_100ns : max_100ns);
  }
#endif /* WINDOWS */

  fptu_time result =
      (grain >= coarse_resolution_ns) ? fptu_now_coarse() : fptu_now_fine();
  result.fractional &= mask;
  return result;
}

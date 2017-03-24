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

#define NSEC_PER_SEC 1000000000u
uint32_t fptu_time::ns2fractional(uint32_t ns) {
  assert(ns < NSEC_PER_SEC);
  /* LY: здесь и далее используется "длинное деление", которое
   * для ясности кода оставлено как есть (без ручной оптимизации). Так как
   * GCC, Clang и даже MSVC сами давно умеют конвертировать деление на
   * константу в быструю reciprocal-форму. */
  return ((uint64_t)ns << 32) / NSEC_PER_SEC;
}

uint32_t fptu_time::fractional2ns(uint32_t fractional) {
  return (fractional * (uint64_t)NSEC_PER_SEC) >> 32;
}

#define USEC_PER_SEC 1000000u
uint32_t fptu_time::us2fractional(uint32_t us) {
  assert(us < USEC_PER_SEC);
  return ((uint64_t)us << 32) / USEC_PER_SEC;
}

uint32_t fptu_time::fractional2us(uint32_t fractional) {
  return (fractional * (uint64_t)USEC_PER_SEC) >> 32;
}

#define MSEC_PER_SEC 1000u
uint32_t fptu_time::ms2fractional(uint32_t ms) {
  assert(ms < MSEC_PER_SEC);
  return ((uint64_t)ms << 32) / MSEC_PER_SEC;
}

uint32_t fptu_time::fractional2ms(uint32_t fractional) {
  return (fractional * (uint64_t)MSEC_PER_SEC) >> 32;
}

//----------------------------------------------------------------------------

#ifndef CLOCK_REALTIME
#define CLOCK_REALTIME 0

#ifdef _MSC_VER
#pragma warning(push, 1)
#endif

#if defined(_WIN32) || defined(_WIN64)
#include <windows.h>

#ifndef HAVE_TIMESPEC_TV_NSEC
struct timespec {
  time_t tv_sec;
  long tv_nsec;
};

static __inline fptu_time from_timespec(const struct timespec &ts) {
  fptu_time result;
  result.fixedpoint = ((uint64_t)ts.tv_sec << 32) |
                      fptu_time::ns2fractional((uint32_t)ts.tv_nsec);
  return result;
}
#endif /* HAVE_TIMESPEC_TV_NSEC */

static int clock_gettime(int clk_id, struct timespec *tp) {
  (void)clk_id;
  FILETIME filetime;
  GetSystemTimeAsFileTime(&filetime);
  uint64_t ns =
      (uint64_t)filetime.dwHighDateTime << 32 | filetime.dwLowDateTime;
  tp->tv_sec = (time_t)(ns / 1000000000ul);
  tp->tv_nsec = (long)(ns % 1000000000ul);
  return 0;
}

#else

static int clock_gettime(int clk_id, struct timespec *tp) {
  (void)clk_id;
  (void)tp;
#error FIXME /* ? */
  return ENOSYS;
}
#endif

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif /* CLOCK_REALTIME */

static void clock_failure(void) {
  /* LY: немного паранойи */
  __assert_fail("clock_gettime() failed", "fptu/time.cxx", 42, "fptu_now()");
}

fptu_time fptu_now_fine(void) {
  struct timespec now;
  int rc = clock_gettime(CLOCK_REALTIME, &now);
  if (unlikely(rc != 0))
    clock_failure();
#ifdef HAVE_TIMESPEC_TV_NSEC
  return fptu_time::from_timespec(now);
#else
  return from_timespec(now);
#endif
}

#ifdef CLOCK_REALTIME_COARSE
static clockid_t fptu_coarse_clockid;
static uint32_t fptu_coarse_resulution_ns;

static void __attribute__((constructor)) fptu_clock_init(void) {
  struct timespec resolution;
  /* LY: Получаем точность coarse-таймеров, одновременно проверяя
   * поддерживается ли CLOCK_REALTIME_COARSE ядром.
   * Если нет, то используем вместо него CLOCK_REALTIME.  */
  if (clock_getres(CLOCK_REALTIME_COARSE, &resolution) == 0 &&
      resolution.tv_sec == 0) {
    fptu_coarse_clockid = CLOCK_REALTIME_COARSE;
    fptu_coarse_resulution_ns = resolution.tv_nsec;
  } else {
    fptu_coarse_clockid = CLOCK_REALTIME;
    fptu_coarse_resulution_ns = 0xffffFFFF;
  }
}
#else /* CLOCK_REALTIME_COARSE */
/* LY: Если CLOCK_REALTIME_COARSE не определен, то в наличии
 * дремучая версия glibc. В этом случае не пытаемся использовать
 * CLOCK_REALTIME_COARSE, а просто ставим заглушку. */
#define fptu_coarse_clockid CLOCK_REALTIME
#define fptu_coarse_resulution_ns 0xffffFFFF
#endif /* ! CLOCK_REALTIME_COARSE */

fptu_time fptu_now_coarse(void) {
#ifdef CLOCK_REALTIME_COARSE
  struct timespec now;
  int rc = clock_gettime(fptu_coarse_clockid, &now);
  if (unlikely(rc != 0))
    clock_failure();

  return fptu_time::from_timespec(now);
#else  /* CLOCK_REALTIME_COARSE */
  return fptu_now_fine();
#endif /* ! CLOCK_REALTIME_COARSE */
}

fptu_time fptu_now(int grain_ns) {
  uint32_t mask = 0xffffFFFF;
  uint32_t grain = (uint32_t)grain_ns;
  if (grain_ns < 0) {
    if (likely(grain_ns > -32)) {
      mask <<= -grain_ns;
      grain = 1u << -grain_ns;
    } else {
      mask = 0;
      grain = ~mask;
    }
  }

  struct timespec now;
  int rc =
      clock_gettime((grain >= fptu_coarse_resulution_ns) ? fptu_coarse_clockid
                                                         : CLOCK_REALTIME,
                    &now);
  if (unlikely(rc != 0))
    clock_failure();

  fptu_time result;
  result.fractional =
      mask ? fptu_time::ns2fractional((uint32_t)now.tv_nsec) & mask : 0;
  result.utc = (uint32_t)now.tv_sec;
  return result;
}

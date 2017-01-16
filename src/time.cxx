/*
 * Copyright 2016 libfptu AUTHORS: please see AUTHORS file.
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

static void clock_failure(void) {
  __assert_fail("clock_gettime() failed", "fptu/time.cxx", 42, "fptu_now()");
}

fptu_time fptu_now_fine(void) {
  struct timespec now;
  int rc = clock_gettime(CLOCK_REALTIME, &now);
  if (unlikely(rc != 0))
    clock_failure();
  return fptu_time::from_timespec(now);
}

#ifdef CLOCK_REALTIME_COARSE
static clockid_t fptu_coarse_clockid;
static uint32_t fptu_coarse_resulution_ns;

static void __attribute__((constructor)) fptu_clock_init(void) {
  struct timespec resolution;
  if (clock_getres(CLOCK_REALTIME_COARSE, &resolution) == 0 &&
      resolution.tv_sec == 0) {
    fptu_coarse_clockid = CLOCK_REALTIME_COARSE;
    fptu_coarse_resulution_ns = resolution.tv_nsec;
  } else {
    fptu_coarse_clockid = CLOCK_REALTIME;
    fptu_coarse_resulution_ns = 0xffffFFFF;
  }
}
#else
#define fptu_coarse_clockid CLOCK_REALTIME
#define fptu_coarse_resulution_ns 0xffffFFFF
#endif /* CLOCK_REALTIME_COARSE */

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
  uint32_t grain = grain_ns;

  if (grain_ns < 0) {
    if (likely(grain_ns > -32)) {
      mask <<= -grain_ns;
      grain = 1u << -grain;
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
  result.fractional = mask ? fptu_time::ns2fractional(now.tv_nsec) & mask : 0;
  result.utc = now.tv_sec;
  return result;
}

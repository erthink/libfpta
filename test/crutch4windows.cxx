/*
 * Copyright 2017-2018 libfpta authors: please see AUTHORS file.
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

#include "fpta_test.h"

#if defined(_WIN32) || defined(_WIN64)

int unlink_crutch(const char *pathname) {
  int retry = 0;
  for (;;) {
    int rc =
#ifdef _MSC_VER
        _unlink(pathname);
#else
        unlink(pathname);
#endif

    if (rc == 0 || errno != EACCES || ++retry > 42)
      return rc;

    /* FIXME: Windows kernel is ugly and mad...
     *
     * Workaround for UnlockFile() bugs, e.g. Windows could return EACCES
     * when such file was unlocked just recently. */
    Sleep(42);
  }
}

fptu_time fptu_now_fine_crutch(void) {
  static fptu_time last;
  fptu_time now;

  do
    now = fptu_now_fine();
  while (last.fixedpoint == now.fixedpoint);

  last.fixedpoint = now.fixedpoint;
  return now;
}

#endif /* windows */

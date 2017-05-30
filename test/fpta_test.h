/*
 * Copyright 2017 libfpta authors: please see AUTHORS file.
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

#pragma once

#include "fast_positive/tables_internal.h"

#ifdef _MSC_VER
#pragma warning(disable : 4625) /* constructor was implicitly defined          \
                                   as deleted */
#pragma warning(disable : 4626) /* assignment operator was implicitly defined  \
                                   as deleted */
#pragma warning(disable : 5026) /* move constructor was implicitly defined     \
                                   as deleted */
#pragma warning(disable : 5027) /* move assignment operator was implicitly     \
                                   defined as deleted */
#pragma warning(disable : 4820) /* 'xyz': 'N' bytes padding added after        \
                                   data member */

#pragma warning(push, 1)
#pragma warning(disable : 4530) /* C++ exception handler used, but             \
                                    unwind semantics are not enabled. Specify  \
                                    /EHsc */
#pragma warning(disable : 4577) /* 'noexcept' used with no exception           \
                                    handling mode specified; termination on    \
                                    exception is not guaranteed. Specify /EHsc \
                                    */
#endif                          /* _MSC_VER (warnings) */

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <limits>
#include <map>
#include <memory>
#include <tuple>
#include <type_traits>
#include <typeinfo>
#include <unordered_map>
#include <utility>
#include <vector>

#ifdef _MSC_VER
#pragma warning(pop)
#endif

//----------------------------------------------------------------------------

#if defined(_WIN32) || defined(_WIN64)

fptu_time fptu_now_fine_crutch(void);
#define NOW_FINE() fptu_now_fine_crutch()

int unlink_crutch(const char *pathname);
#define REMOVE_FILE(pathname) unlink_crutch(pathname)
#define TEST_DB_DIR ""

#else

#define NOW_FINE() fptu_now_fine()
#define REMOVE_FILE(pathname) unlink(pathname)
#ifdef __linux__
#define TEST_DB_DIR "/dev/shm/"
#else
#define TEST_DB_DIR "/tmp/"
#endif

#endif

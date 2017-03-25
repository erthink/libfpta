/*
 * Copyright 2017 libfptu authors: please see AUTHORS file.
 *
 * This file is part of libfptu, aka "Fast Positive Tables".
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

#pragma once

#include "fast_positive/tuples_internal.h"

#ifdef _MSC_VER
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

#ifdef _MSC_VER
#pragma warning(pop)
#endif

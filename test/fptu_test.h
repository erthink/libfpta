/*
 * Copyright 2017 libfptu authors: please see AUTHORS file.
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

#pragma once

#if defined(_MSC_VER) && !defined(_CRT_SECURE_NO_WARNINGS)
#define _CRT_SECURE_NO_WARNINGS
#endif

#include "fast_positive/tuples_internal.h"

#ifdef _MSC_VER
#pragma warning(push, 1)
#pragma warning(disable : 4530) /* C++ exception handler used, but             \
                                   unwind semantics are not enabled.           \
                                   Specify /EHsc */
#pragma warning(disable : 4577) /* 'noexcept' used with no exception           \
                                   handling mode specified; termination on     \
                                   exception is not guaranteed.                \
                                   Specify /EHsc */
#pragma warning(disable : 4738) /* storing 32-bit float result in memory,      \
                                   possible loss of performance */
#if _MSC_VER < 1900
/* LY: workaround for dead code:
       microsoft visual studio 12.0\vc\include\xtree(1826) */
#pragma warning(disable : 4702) /* unreachable code */
#endif
#endif /* _MSC_VER (warnings) */

#include <gtest/gtest.h>

#ifdef _MSC_VER
#pragma warning(pop)
#endif

/* LY: reduce test runtime (significantly on Elbrus) */
#if defined(__LCC__) && defined(NDEBUG) && defined(__OPTIMIZE__) &&            \
    !defined(ENABLE_GPROF)
#undef SCOPED_TRACE
#define SCOPED_TRACE(message) __noop()
#endif /* __LCC__ */

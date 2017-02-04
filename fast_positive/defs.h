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

#pragma once
/* *INDENT-OFF* */
/* clang-format off */

#if defined(__KERNEL__) || !defined(__cplusplus) || __cplusplus < 201103L
#	include <stddef.h>
#	include <stdint.h>
#	include <assert.h>
#else
#	include <cstddef>
#	include <cstdint>
#	include <cassert>
#endif

#ifndef __GNUC_PREREQ
#if defined(__GNUC__) && defined(__GNUC_MINOR__)
#define __GNUC_PREREQ(maj, min)                                                \
  ((__GNUC__ << 16) + __GNUC_MINOR__ >= ((maj) << 16) + (min))
#else
#define __GNUC_PREREQ(maj, min) 0
#endif
#endif

#if defined(__GNUC__) && !__GNUC_PREREQ(4,2)
	/* Actualy libfptu was not tested with compilers older than GCC from RHEL6.
	 * But you could remove this #error and try to continue at your own risk.
	 * In such case please don't rise up an issues related ONLY to old compilers. */
#	error "libfptu required at least GCC 4.2 compatible C/C++ compiler."
#endif

#ifndef __CLANG_PREREQ
#	ifdef __clang__
#		define __CLANG_PREREQ(maj,min) \
			((__clang_major__ << 16) + __clang_minor__ >= ((maj) << 16) + (min))
#	else
#		define __CLANG_PREREQ(maj,min) (0)
#	endif
#endif /* __CLANG_PREREQ */

#ifndef __has_attribute
#	define __has_attribute(x) (0)
#endif

#ifndef __GLIBC_PREREQ
#	if defined(__GLIBC__) && defined(__GLIBC_MINOR__)
#		define __GLIBC_PREREQ(maj, min) \
			((__GLIBC__ << 16) + __GLIBC_MINOR__ >= ((maj) << 16) + (min))
#	else
#		define __GLIBC_PREREQ(maj, min) 0
#	endif
#endif /* __GLIBC_PREREQ */

#if defined(__GLIBC__) && !__GLIBC_PREREQ(2,12)
	/* Actualy libfptu requires just C99 (e.g glibc >= 2.1), but was
	 * not tested with glibc older than 2.12 (from RHEL6). So you could
	 * remove this #error and try to continue at your own risk.
	 * In such case please don't rise up an issues related ONLY to old glibc. */
#	error "libfptu required at least glibc version 2.12 or later."
#endif

//----------------------------------------------------------------------------

#ifndef __extern_C
#	ifdef __cplusplus
#		define __extern_C extern "C"
#	else
#		define __extern_C
#	endif
#endif /* __extern_C */

#ifndef __cplusplus
#	ifndef bool
#		define bool _Bool
#	endif
#	ifndef true
#		define true (1)
#	endif
#	ifndef false
#		define false (0)
#	endif
#endif

#if !defined(nullptr) && !defined(__cplusplus) || (__cplusplus < 201103L && !defined(_MSC_VER))
#	define nullptr NULL
#endif

#if !defined(__BYTE_ORDER__) || !defined(__ORDER_LITTLE_ENDIAN__) ||           \
    !defined(__ORDER_BIG_ENDIAN__)
#define __ORDER_LITTLE_ENDIAN__ 1234
#define __ORDER_BIG_ENDIAN__ 4321
#if defined(__LITTLE_ENDIAN__) || defined(__ARMEL__) ||                        \
    defined(__THUMBEL__) || defined(__AARCH64EL__) || defined(__MIPSEL__) ||   \
    defined(_MIPSEL) || defined(__MIPSEL) || defined(__i386) ||                \
    defined(__x86_64__) || defined(_M_IX86) || defined(_M_X64) ||              \
    defined(i386) || defined(_X86_) || defined(__i386__) ||                    \
    defined(_X86_64_) || defined(_M_ARM)
#define __BYTE_ORDER__ __ORDER_LITTLE_ENDIAN__
#elif defined(__BIG_ENDIAN__) || defined(__ARMEB__) || defined(__THUMBEB__) || \
    defined(__AARCH64EB__) || defined(__MIPSEB__) || defined(_MIPSEB) ||       \
    defined(__MIPSEB)
#define __BYTE_ORDER__ __ORDER_BIG_ENDIAN__
#else
#error __BYTE_ORDER__ should be defined.
#endif
#endif

//----------------------------------------------------------------------------

#if !defined(__thread) && (defined(_MSC_VER) || defined(__DMC__))
#	define __thread __declspec(thread)
#endif

#ifndef __alwaysinline
#	if defined(__GNUC__) || __has_attribute(always_inline)
#		define __alwaysinline __inline __attribute__((always_inline))
#	elif defined(_MSC_VER)
#		define __alwaysinline __forceinline
#	else
#		define __alwaysinline
#	endif
#endif /* __alwaysinline */

#ifndef __noinline
#	if defined(__GNUC__) || __has_attribute(noinline)
#		define __noinline __attribute__((noinline))
#	elif defined(_MSC_VER)
#		define __noinline __declspec(noinline)
#	endif
#endif /* __noinline */

#ifndef __must_check_result
#	if defined(__GNUC__) || __has_attribute(warn_unused_result)
#		define __must_check_result __attribute__((warn_unused_result))
#	else
#		define __must_check_result
#	endif
#endif /* __must_check_result */

#ifndef __deprecated
#	if defined(__GNUC__) || __has_attribute(deprecated)
#		define __deprecated __attribute__((deprecated))
#	elif defined(_MSC_VER)
#		define __deprecated __declspec(deprecated)
#	else
#		define __deprecated
#	endif
#endif /* __deprecated */

#ifndef __packed
#	if defined(__GNUC__) || __has_attribute(packed)
#		define __packed __attribute__((packed))
#	else
#		define  __packed
#	endif
#endif

#ifndef __hidden
#	if defined(__GNUC__) || __has_attribute(visibility)
#		define __hidden __attribute__((visibility("hidden")))
#	else
#		define __hidden
#	endif
#endif

#ifndef __public
#	if defined(__GNUC__) || __has_attribute(visibility)
#		define __public __attribute__((visibility("default")))
#	else
#		define __public
#	endif
#endif

#ifndef __noreturn
#	if defined(__GNUC__) || __has_attribute(noreturn)
#		define __noreturn __attribute__((noreturn))
#	elif defined(__MSC_VER)
#		define __noreturn __declspec(noreturn)
#	else
#		define __noreturn
#	endif
#endif

#ifndef __nothrow
#	if defined(__GNUC__) || __has_attribute(nothrow)
#		define __nothrow __attribute__((nothrow))
#	elif defined(__MSC_VER)
#		define __nothrow __declspec(nothrow)
#	else
#		define __nothrow
#	endif
#endif

#ifndef __pure_function
	/* Many functions have no effects except the return value and their
	 * return value depends only on the parameters and/or global variables.
	 * Such a function can be subject to common subexpression elimination
	 * and loop optimization just as an arithmetic operator would be.
	 * These functions should be declared with the attribute pure. */
#	if defined(__GNUC__) || __has_attribute(pure)
#		define __pure_function __attribute__((pure))
#	else
#		define __pure_function
#	endif
#endif

#ifndef __const_function
	/* Many functions do not examine any values except their arguments,
	 * and have no effects except the return value. Basically this is just
	 * slightly more strict class than the PURE attribute, since function
	 * is not allowed to read global memory.
	 *
	 * Note that a function that has pointer arguments and examines the
	 * data pointed to must not be declared const. Likewise, a function
	 * that calls a non-const function usually must not be const.
	 * It does not make sense for a const function to return void. */
#	if defined(__GNUC__) || __has_attribute(const)
#		define __const_function __attribute__((const))
#	else
#		define __const_function
#	endif
#endif /* __const_function */

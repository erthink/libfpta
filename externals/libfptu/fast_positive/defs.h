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

#pragma once
/* *INDENT-OFF* */
/* clang-format off */

#ifdef _MSC_VER
#pragma warning(push, 1)
#pragma warning(disable : 4548) /* expression before comma has no effect;      \
                                   expected expression with side - effect */
#pragma warning(disable : 4530) /* C++ exception handler used, but unwind      \
                                   semantics are not enabled. Specify /EHsc */
#pragma warning(disable : 4577) /* 'noexcept' used with no exception handling  \
                                   mode specified; termination on exception    \
                                   is not guaranteed. Specify /EHsc */
#endif

#if defined(__KERNEL__) || !defined(__cplusplus) || __cplusplus < 201103L
#	include <stddef.h>
#	include <stdint.h>
#	include <assert.h>
#else
#	include <cstddef>
#	include <cstdint>
#	include <cassert>
#endif

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#ifndef __GNUC_PREREQ
#	if defined(__GNUC__) && defined(__GNUC_MINOR__)
#		define __GNUC_PREREQ(maj, min) \
			((__GNUC__ << 16) + __GNUC_MINOR__ >= ((maj) << 16) + (min))
#	else
#		define __GNUC_PREREQ(maj, min) (0)
#	endif
#endif /* __GNUC_PREREQ */

#ifndef __CLANG_PREREQ
#	ifdef __clang__
#		define __CLANG_PREREQ(maj,min) \
			((__clang_major__ << 16) + __clang_minor__ >= ((maj) << 16) + (min))
#	else
#		define __CLANG_PREREQ(maj,min) (0)
#	endif
#endif /* __CLANG_PREREQ */

#ifndef __GLIBC_PREREQ
#	if defined(__GLIBC__) && defined(__GLIBC_MINOR__)
#		define __GLIBC_PREREQ(maj, min) \
			((__GLIBC__ << 16) + __GLIBC_MINOR__ >= ((maj) << 16) + (min))
#	else
#		define __GLIBC_PREREQ(maj, min) (0)
#	endif
#endif /* __GLIBC_PREREQ */

#ifndef __has_attribute
#	define __has_attribute(x) (0)
#endif

#ifndef __has_feature
#	define __has_feature(x) (0)
#endif

#ifndef __has_extension
#	define __has_extension(x) (0)
#endif

#ifndef __has_builtin
#	define __has_builtin(x) (0)
#endif

#ifndef __has_warning
#	define __has_warning(x) (0)
#endif

#ifndef __has_include
#	define __has_include(x) (0)
#endif

#if __has_feature(thread_sanitizer)
#	define __SANITIZE_THREAD__ 1
#endif

#if __has_feature(address_sanitizer)
#	define __SANITIZE_ADDRESS__ 1
#endif

#if !defined(__STDC_VERSION__) || __STDC_VERSION__ < 199901
#	if (defined(__GNUC__) && __GNUC__ >= 2) || defined(__clang__) || defined(_MSC_VER)
#		define __func__ __FUNCTION__
#	else
#		define __func__ "__func__"
#	endif
#endif /* __func__ */

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

//----------------------------------------------------------------------------

#if !defined(__thread) && (defined(_MSC_VER) || defined(__DMC__))
#	define __thread __declspec(thread)
#endif /* __thread */

#ifndef __alwaysinline
#	if defined(__GNUC__) || __has_attribute(always_inline)
#		define __alwaysinline __inline __attribute__((always_inline))
#	elif defined(_MSC_VER)
#		define __alwaysinline __forceinline
#	else
#		define __alwaysinline
#	endif
#endif /* __alwaysinline */

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

#ifdef _MSC_VER
#	define FPT_PACKED_STRUCT(name) \
		__pragma(pack(push, 1)) struct name __pragma(pack(pop))
#elif defined(__GNUC__) || __has_attribute(packed)
#	define FPT_PACKED_STRUCT(name) struct __attribute__((packed)) name
#else
#	error Unsupported C/C++ compiler
#endif /* FPT_PACKED_STRUCT */

#ifndef __noreturn
#	if defined(__GNUC__) || __has_attribute(noreturn)
#		define __noreturn __attribute__((noreturn))
#	elif defined(_MSC_VER)
#		define __noreturn __declspec(noreturn)
#	else
#		define __noreturn
#	endif
#endif /* __noreturn */

#ifndef __nothrow
#	if defined(__GNUC__) || __has_attribute(nothrow)
#		define __nothrow __attribute__((nothrow))
#	elif defined(_MSC_VER) && defined(__cplusplus)
#		define __nothrow __declspec(nothrow)
#	else
#		define __nothrow
#	endif
#endif /* __nothrow */

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
#endif /* __pure_function */

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

#ifndef __dll_hidden
#	if defined(__GNUC__) || __has_attribute(visibility)
#		define __hidden __attribute__((visibility("hidden")))
#	else
#		define __hidden
#	endif
#endif /* __dll_hidden */

#ifndef __dll_export
#	if defined(_WIN32) || defined(__CYGWIN__)
#		if defined(__GNUC__) || __has_attribute(dllexport)
#			define __dll_export __attribute__((dllexport))
#		elif defined(_MSC_VER)
#			define __dll_export __declspec(dllexport)
#		else
#			define __dll_export
#		endif
#	elif defined(__GNUC__) || __has_attribute(visibility)
#		define __dll_export __attribute__((visibility("default")))
#	else
#		define __dll_export
#	endif
#endif /* __dll_export */

#ifndef __dll_import
#	if defined(_WIN32) || defined(__CYGWIN__)
#		if defined(__GNUC__) || __has_attribute(dllimport)
#			define __dll_import __attribute__((dllimport))
#		elif defined(_MSC_VER)
#			define __dll_import __declspec(dllimport)
#		else
#			define __dll_import
#		endif
#	else
#		define __dll_import
#	endif
#endif /* __dll_import */

//----------------------------------------------------------------------------

/* *INDENT-ON* */
/* clang-format on */

#if defined(i386) || defined(__386) || defined(__i386) || defined(__i386__) || \
    defined(i486) || defined(__i486) || defined(__i486__) ||                   \
    defined(i586) | defined(__i586) || defined(__i586__) || defined(i686) ||   \
    defined(__i686) || defined(__i686__) || defined(_M_IX86) ||                \
    defined(_X86_) || defined(__THW_INTEL__) || defined(__I86__) ||            \
    defined(__INTEL__) || defined(__x86_64) || defined(__x86_64__) ||          \
    defined(__amd64__) || defined(__amd64) || defined(_M_X64) ||               \
    defined(_M_AMD64) || defined(__IA32__) || defined(__INTEL__)
#ifndef __ia32__
/* LY: define neutral __ia32__ for x86 and x86-64 archs */
#define __ia32__ 1
#endif /* __ia32__ */
#if !defined(__amd64__) && (defined(__x86_64) || defined(__x86_64__) ||        \
                            defined(__amd64) || defined(_M_X64))
/* LY: define trusty __amd64__ for all AMD64/x86-64 arch */
#define __amd64__ 1
#endif /* __amd64__ */
#endif /* all x86 */

#if !defined(__BYTE_ORDER__) || !defined(__ORDER_LITTLE_ENDIAN__) ||           \
    !defined(__ORDER_BIG_ENDIAN__)

/* *INDENT-OFF* */
/* clang-format off */

#if defined(__GLIBC__) || defined(__GNU_LIBRARY__) || defined(__ANDROID__) ||  \
    __has_include(<endian.h>)
#include <endian.h>
#elif defined(__APPLE__) || defined(__MACH__) || defined(__OpenBSD__) ||       \
    __has_include(<machine/endian.h>)
#include <machine/endian.h>
#elif __has_include(<sys/isa_defs.h>)
#include <sys/isa_defs.h>
#elif __has_include(<sys/types.h>) && __has_include(<sys/endian.h>)
#include <sys/endian.h>
#include <sys/types.h>
#elif defined(__bsdi__) || defined(__DragonFly__) || defined(__FreeBSD__) ||   \
    defined(__NETBSD__) || defined(__NetBSD__) ||                              \
    __has_include(<sys/param.h>)
#include <sys/param.h>
#endif /* OS */

/* *INDENT-ON* */
/* clang-format on */

#if defined(__BYTE_ORDER) && defined(__LITTLE_ENDIAN) && defined(__BIG_ENDIAN)
#define __ORDER_LITTLE_ENDIAN__ __LITTLE_ENDIAN
#define __ORDER_BIG_ENDIAN__ __BIG_ENDIAN
#define __BYTE_ORDER__ __BYTE_ORDER
#elif defined(_BYTE_ORDER) && defined(_LITTLE_ENDIAN) && defined(_BIG_ENDIAN)
#define __ORDER_LITTLE_ENDIAN__ _LITTLE_ENDIAN
#define __ORDER_BIG_ENDIAN__ _BIG_ENDIAN
#define __BYTE_ORDER__ _BYTE_ORDER
#else
#define __ORDER_LITTLE_ENDIAN__ 1234
#define __ORDER_BIG_ENDIAN__ 4321

#if defined(__LITTLE_ENDIAN__) ||                                              \
    (defined(_LITTLE_ENDIAN) && !defined(_BIG_ENDIAN)) ||                      \
    defined(__ARMEL__) || defined(__THUMBEL__) || defined(__AARCH64EL__) ||    \
    defined(__MIPSEL__) || defined(_MIPSEL) || defined(__MIPSEL) ||            \
    defined(_M_ARM) || defined(_M_ARM64) || defined(__e2k__) ||                \
    defined(__elbrus_4c__) || defined(__elbrus_8c__) || defined(__bfin__) ||   \
    defined(__BFIN__) || defined(__ia64__) || defined(_IA64) ||                \
    defined(__IA64__) || defined(__ia64) || defined(_M_IA64) ||                \
    defined(__itanium__) || defined(__ia32__) || defined(__CYGWIN__) ||        \
    defined(_WIN64) || defined(_WIN32) || defined(__TOS_WIN__) ||              \
    defined(__WINDOWS__)
#define __BYTE_ORDER__ __ORDER_LITTLE_ENDIAN__

#elif defined(__BIG_ENDIAN__) ||                                               \
    (defined(_BIG_ENDIAN) && !defined(_LITTLE_ENDIAN)) ||                      \
    defined(__ARMEB__) || defined(__THUMBEB__) || defined(__AARCH64EB__) ||    \
    defined(__MIPSEB__) || defined(_MIPSEB) || defined(__MIPSEB) ||            \
    defined(__m68k__) || defined(M68000) || defined(__hppa__) ||               \
    defined(__hppa) || defined(__HPPA__) || defined(__sparc__) ||              \
    defined(__sparc) || defined(__370__) || defined(__THW_370__) ||            \
    defined(__s390__) || defined(__s390x__) || defined(__SYSC_ZARCH__)
#define __BYTE_ORDER__ __ORDER_BIG_ENDIAN__

#else
#error __BYTE_ORDER__ should be defined.
#endif /* Arch */

#endif
#endif /* __BYTE_ORDER__ || __ORDER_LITTLE_ENDIAN__ || __ORDER_BIG_ENDIAN__ */

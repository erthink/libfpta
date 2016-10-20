/*
 * Copyright 2016 libfptu AUTHORS: please see AUTHORS file.
 *
 * This file is part of libfptu, aka "Positive Tuples".
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

#ifndef __STDC_LIMIT_MACROS
#   define __STDC_LIMIT_MACROS 1
#endif

/*! \todo Support for other compilers. */
#if !defined(__GNUC__) || __GNUC__ < 4
#    error Sorry, this source code wanna GCC >= 4.x only :(
#endif

/*! \todo Enhance for Linux kernel. */
#if defined(__KERNEL__) || !defined(__cplusplus) || __cplusplus < 201103L
#    include <stddef.h>
#    include <stdint.h>
#else
#    include <cstddef>
#    include <cstdint>
#endif

#if !defined(__cplusplus) || __cplusplus < 201103L
#    define nullptr NULL
#    define final
#endif

#ifndef __cplusplus
#    define mutable
#    ifndef bool
#        define bool _Bool
#    endif
#    ifndef true
#        define true (1)
#    endif
#    ifndef false
#        define false (0)
#    endif
#endif

#ifndef __extern_C
#    ifdef __cplusplus
#        define __extern_C extern "C"
#    else
#        define __extern_C
#    endif
#endif

#ifndef __noinline
#    define __noinline __attribute__((__noinline__))
#endif

#ifndef __must_check_result
#    define __must_check_result __attribute__((warn_unused_result))
#endif

#ifndef __deprecated
#    define __deprecated __attribute__((deprecated))
#endif

#ifndef __forceinline
#    define __forceinline __inline __attribute__((always_inline))
#endif

#ifndef __packed
#    define __packed __attribute__((packed))
#endif

#ifndef __maybe_unused
#    define __maybe_unused __attribute__((unused))
#endif

#ifndef __hidden
#    define __hidden __attribute__((visibility("hidden")))
#endif

#ifndef __public
#    define __public __attribute__((visibility("default")))
#endif

#ifndef __noreturn
#    define __noreturn __attribute__((noreturn))
#endif

#ifndef __nothrow
#    define __nothrow __attribute__((nothrow))
#endif

#ifndef __pure_function
/*
 * Many functions have no effects except the return value and their return value depends only
 * on the parameters and/or global variables. Such a function can be subject to common
 * subexpression elimination and loop optimization just as an arithmetic operator would be.
 * These functions should be declared with the attribute pure.
 */
#    define __pure_function __attribute__((pure))
#endif

#ifndef __const_function
/*
 * Many functions do not examine any values except their arguments,
 * and have no effects except the return value. Basically this is just slightly
 * more strict class than the PURE attribute, since function is not allowed to read global memory.
 *
 * Note that a function that has pointer arguments and examines the data pointed to must not be declared const.
 * Likewise, a function that calls a non-const function usually must not be const.
 * It does not make sense for a const function to return void..
 */
#    define __const_function __attribute__((const))
#endif

#ifndef __aligned
#   define __aligned(N) __attribute__((aligned(N)))
#endif

/*! \todo Be sure to NDEBUG is enough. */
#if NDEBUG
#    ifndef __hot
#        define __hot __attribute__((hot, optimize("O3")))
#    endif
#    ifndef __cold
#        define __cold __attribute__((cold, optimize("Os")))
#    endif
#    ifndef __flatten
#        define __flatten __attribute__((flatten))
#    endif
#else
#    ifndef __hot
#        define __hot
#    endif
#    ifndef __cold
#        define __cold
#    endif
#    ifndef __flatten
#        define __flatten
#    endif
#endif

#ifdef __cplusplus
#   define FPT_NONCOPYABLE(typename) \
        typename(const typename&) = delete; \
        typename& operator=(typename const&) = delete
#endif

#ifndef __noop
#    define __noop() do {} while (0)
#endif

#ifndef __unreachable
#    if __GNUC_MINOR__ >= 5
#        define __unreachable() __builtin_unreachable()
#    else
#        define __unreachable() __noop()
#    endif
#endif

#ifndef __prefetch
#    define __prefetch(ptr) __builtin_prefetch(ptr)
#endif

#ifndef __expect_equal
#    define __expect_equal(a, b) __builtin_expect((a), (b))
#endif

#define FPT_TETRAD(a, b, c, d) ((a) << 24 | (b) << 16 | (c) << 8 | (d))

#ifdef __cplusplus
    template <typename T, size_t N>
    char (&__FPT_ArraySizeHelper(T (&array)[N]))[N];
#   define FPT_ARRAY_LENGTH(array) (sizeof(::__FPT_ArraySizeHelper(array)))
#else
#   define FPT_ARRAY_LENGTH(array) (sizeof(array) / sizeof(array[0]))
#endif

#define FPT_ARRAY_END(array) (&array[FPT_ARRAY_LENGTH(array)])

#define FPT_STR(x) #x
#define FPT_STRINGIFY(x) FPT_STR(x)

#ifndef offsetof
#   define offsetof(type, member)  __builtin_offsetof(type, member)
#endif

#ifndef container_of
#   define container_of(ptr, type, member) \
    ({ \
        const __typeof(((type*)nullptr)->member) * __ptr = (ptr); \
        (type*)((char*)__ptr - offsetof(type, member)); \
    })
#endif

#define FPT_IS_POWER2(value) (((value) & ((value) - 1)) == 0 && (value) > 0)
#define __FPT_FLOOR_MASK(type, value, mask) ((value) & ~(type)(mask))
#define __FPT_CEIL_MASK(type, value, mask) __FPT_FLOOR_MASK(type, (value) + (mask), mask)
#define FPT_ALIGN_FLOOR(value, align) __FPT_FLOOR_MASK(__typeof(value), value, (align) - 1LL)
#define FPT_ALIGN_CEIL(value, align) __FPT_CEIL_MASK(__typeof(value), value, (align) - 1LL)
#define FPT_IS_ALIGNED(ptr, align) ((((align) - 1LL) & ((__typeof(align))((uintptr_t)(ptr)))) == 0)

#ifndef likely
#ifdef __cplusplus
    /* LY: workaround for "pretty" boost */
    static __forceinline bool likely(bool cond) { return __builtin_expect(cond, 1); }
#else
#    define likely(cond) __builtin_expect(!!(cond), 1)
#endif
#endif /* likely */

#ifndef unlikely
#ifdef __cplusplus
    /* LY: workaround for "pretty" boost */
    static __forceinline bool unlikely(bool cond) { return __builtin_expect(cond, 0); }
#else
#    define unlikely(cond) __builtin_expect(!!(cond), 0)
#endif
#endif /* unlikely */

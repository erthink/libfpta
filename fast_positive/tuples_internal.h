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

#pragma once

/* *INDENT-OFF* */
/* clang-format off */
#ifndef _ISOC99_SOURCE
#	define _ISOC99_SOURCE 1
#endif

#ifndef _ISOC11_SOURCE
#	define _ISOC11_SOURCE 1
#endif

#ifndef _POSIX_C_SOURCE
#	define _POSIX_C_SOURCE 200809L
#endif

#ifndef _GNU_SOURCE
#	define _GNU_SOURCE 1
#endif

#ifndef __STDC_LIMIT_MACROS
#	define __STDC_LIMIT_MACROS 1
#endif

#ifndef _THREAD_SAFE
#	define _THREAD_SAFE 1
#endif

#ifndef _REENTRANT
#	define _REENTRANT 1
#endif

#include "fast_positive/tuples.h"
#include <limits.h>
#include <string.h>

//----------------------------------------------------------------------------

#ifndef __hot
#	if defined(NDEBUG)
#		if defined(__clang__)
			/* cland case, just put frequently used functions in separate section */
#			define __hot __attribute__((section("text.hot_fptu")))
#		elif defined(__GNUC__)
#			define __hot __attribute__((hot, optimize("O3")))
#		else
#			define __hot
#		endif
#	else
#		define __hot
#	endif
#endif /* __hot */

#ifndef __cold
#	if defined(NDEBUG)
#		if defined(__clang__)
			/* cland case, just put infrequently used functions in separate section */
#			define __cold __attribute__((section("text.unlikely_fptu")))
#		elif defined(__GNUC__)
#			define __cold __attribute__((cold, optimize("Os")))
#		else
#			define __cold
#		endif
#	else
#		define __cold
#	endif
#endif /* __cold */

#ifndef __flatten
#	if defined(NDEBUG) && (defined(__GNUC__) || __has_attribute(flatten))
#		define __flatten __attribute__((flatten))
#	else
#		define __flatten
#	endif
#endif /* __flatten */

#ifdef __cplusplus
#	define FPT_NONCOPYABLE(typename) \
		typename(const typename&) = delete; \
		typename& operator=(typename const&) = delete
#endif

#ifndef __noop
#	define __noop() do {} while(0)
#endif

#ifndef __unreachable
#	if __GNUC_PREREQ(4,5)
#		define __unreachable() __builtin_unreachable()
#	else
#		define __unreachable() __noop()
#	endif
#endif

#ifndef __prefetch
#	if defined(__GNUC__) || defined(__clang__)
#		define __prefetch(ptr) __builtin_prefetch(ptr)
#	else
#		define __prefetch(ptr) do {(void)(ptr)} while(0)
#	endif
#endif /* __prefetch */

#ifndef __expect_equal
#	if defined(__GNUC__) || defined(__clang__)
#		define __expect_equal(exp, c) __builtin_expect((exp), (c))
#	else
#		define __expect_equal(exp, c) (exp)
#	endif
#endif /* __expect_equal */

#ifndef likely
#	define likely(cond) __expect_equal(!!(cond), 1)
#endif

#ifndef unlikely
#	define unlikely(cond) __expect_equal(!!(cond), 0)
#endif

#ifndef __aligned
#	if defined(__GNUC__) || defined(__clang__)
#		define __aligned(N) __attribute__((aligned(N)))
#	elif defined(__MSC_VER)
#		define __aligned(N) __declspec(align(N))
#	else
#		define __aligned(N)
#	endif
#endif /* __align */

#ifndef CACHELINE_SIZE
#	if defined(__ia64__) || defined(__ia64) || defined(_M_IA64)
#		define CACHELINE_SIZE 128
#	else
#		define CACHELINE_SIZE 64
#	endif
#endif

#ifndef __cache_aligned
#	define __cache_aligned __aligned(CACHELINE_SIZE)
#endif

//----------------------------------------------------------------------------

#ifdef __cplusplus
	template <typename T, size_t N>
	char (&__FPT_ArraySizeHelper(T (&array)[N]))[N];
#	define FPT_ARRAY_LENGTH(array) (sizeof(::__FPT_ArraySizeHelper(array)))
#else
#	define FPT_ARRAY_LENGTH(array) (sizeof(array) / sizeof(array[0]))
#endif

#define FPT_ARRAY_END(array) (&array[FPT_ARRAY_LENGTH(array)])

#define FPT_STR(x) #x
#define FPT_STRINGIFY(x) FPT_STR(x)

#ifndef offsetof
#	define offsetof(type, member)  __builtin_offsetof(type, member)
#endif

#ifndef container_of
#define container_of(ptr, type, member)                                      \
    ({                                                                       \
        const __typeof(((type *)nullptr)->member) *__ptr = (ptr);            \
        (type *)((char *)__ptr - offsetof(type, member));                    \
    })
#endif /* container_of */

#define FPT_IS_POWER2(value) (((value) & ((value)-1LL)) == 0 && (value) > 0)
#define __FPT_FLOOR_MASK(type, value, mask) ((value) & ~(type)(mask))
#define __FPT_CEIL_MASK(type, value, mask)                                   \
    __FPT_FLOOR_MASK(type, (value) + (mask), mask)
#define FPT_ALIGN_FLOOR(value, align)                                        \
    __FPT_FLOOR_MASK(__typeof(value), value, (align)-1LL)
#define FPT_ALIGN_CEIL(value, align)                                         \
    __FPT_CEIL_MASK(__typeof(value), value, (align)-1LL)
#define FPT_IS_ALIGNED(ptr, align)                                           \
    ((((align)-1LL) & ((__typeof(align))((uintptr_t)(ptr)))) == 0)

/* *INDENT-ON* */
/* clang-format on */
//----------------------------------------------------------------------------

static __inline unsigned fptu_get_col(uint16_t packed)
{
    return packed >> fptu_co_shift;
}

static __inline fptu_type fptu_get_type(unsigned packed)
{
    return (fptu_type)(packed & fptu_ty_mask);
}

static __inline unsigned fptu_pack_coltype(unsigned column, unsigned type)
{
    assert(type <= fptu_ty_mask);
    assert(column <= fptu_max_cols);
    return type + (column << fptu_co_shift);
}

static __inline bool fptu_ct_match(const fptu_field *pf, unsigned column,
                                   int type_or_filter)
{
    if (fptu_get_col(pf->ct) != column)
        return false;
    if (type_or_filter & fptu_filter)
        return (type_or_filter & (1 << fptu_get_type(pf->ct))) ? true : false;
    return type_or_filter == fptu_get_type(pf->ct);
}

typedef union fptu_payload {
    uint32_t u32;
    int32_t i32;
    uint64_t u64;
    int64_t i64;
    float fp32;
    double fp64;
    char cstr[4];
    uint8_t fixbin[8];
    struct {
        fptu_varlen varlen;
        uint32_t data[1];
    } other;
} fptu_payload;

static __inline size_t bytes2units(size_t bytes)
{
    return (bytes + fptu_unit_size - 1) >> fptu_unit_shift;
}

static __inline size_t units2bytes(size_t units)
{
    return units << fptu_unit_shift;
}

static __inline fptu_payload *fptu_field_payload(fptu_field *pf)
{
    return (fptu_payload *)&pf->body[pf->offset];
}

#ifdef __cplusplus
static __inline const fptu_payload *fptu_field_payload(const fptu_field *pf)
{
    return (const fptu_payload *)&pf->body[pf->offset];
}
#endif /* __cplusplus */

extern const uint8_t fptu_internal_map_t2b[];
extern const uint8_t fptu_internal_map_t2u[];

static __inline bool ct_is_fixedsize(unsigned ct)
{
    return fptu_get_type(ct) < fptu_cstr;
}

static __inline bool ct_is_dead(unsigned ct)
{
    return ct >= (fptu_co_dead << fptu_co_shift);
}

static __inline size_t ct_elem_size(unsigned ct)
{
    unsigned type = fptu_get_type(ct);
    if (likely(ct_is_fixedsize(type)))
        return fptu_internal_map_t2b[type];

    /* fptu_opaque, fptu_cstr or fptu_farray.
     * at least 4 bytes for length or '\0'. */
    return fptu_unit_size;
}

static __inline bool ct_match_fixedsize(unsigned ct, unsigned units)
{
    return ct_is_fixedsize(ct) &&
           units == fptu_internal_map_t2u[fptu_get_type(ct)];
}

size_t fptu_field_units(const fptu_field *pf);

static __inline const void *fptu_ro_detent(fptu_ro ro)
{
    return (char *)ro.sys.iov_base + ro.sys.iov_len;
}

static __inline const void *fptu_detent(const fptu_rw *rw)
{
    return &rw->units[rw->end];
}

fptu_field *fptu_lookup_ct(fptu_rw *pt, unsigned ct);

template <typename type>
static __inline fptu_lge fptu_cmp2lge(type left, type right)
{
    if (left == right)
        return fptu_eq;
    return (left < right) ? fptu_lt : fptu_gt;
}

template <typename type> static __inline fptu_lge fptu_diff2lge(type diff)
{
    return fptu_cmp2lge<type>(diff, 0);
}

fptu_lge fptu_cmp2lge(fptu_lge left, fptu_lge right) = delete;
fptu_lge fptu_diff2lge(fptu_lge diff) = delete;

static __inline fptu_lge fptu_cmp_binary_str(const void *left_data,
                                             size_t left_len,
                                             const char *right_cstr)
{
    size_t right_len = right_cstr ? strlen(right_cstr) : 0;
    return fptu_cmp_binary(left_data, left_len, right_cstr, right_len);
}

static __inline fptu_lge fptu_cmp_str_binary(const char *left_cstr,
                                             const void *right_data,
                                             size_t right_len)
{
    size_t left_len = left_cstr ? strlen(left_cstr) : 0;
    return fptu_cmp_binary(left_cstr, left_len, right_data, right_len);
}

template <typename type>
static __inline int fptu_cmp2int(type left, type right)
{
    return (right > left) ? -1 : left > right;
}

bool fptu_is_ordered(const fptu_field *begin, const fptu_field *end);
uint16_t *fptu_tags(uint16_t *const first, const fptu_field *const begin,
                    const fptu_field *const end);

template <typename iterator>
static __inline fptu_lge
fptu_depleted2lge(const iterator &left_pos, const iterator &left_end,
                  const iterator &right_pos, const iterator &right_end)
{
    bool left_depleted = (left_pos >= left_end);
    bool right_depleted = (right_pos >= right_end);

    if (left_depleted == right_depleted)
        return fptu_eq;
    return left_depleted ? fptu_lt : fptu_gt;
}

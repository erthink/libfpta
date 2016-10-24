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

#if !defined(__cplusplus) || __cplusplus < 201103L
#	define nullptr NULL
#	define final
#endif

//----------------------------------------------------------------------

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

//----------------------------------------------------------------------

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
#	define container_of(ptr, type, member) \
	({ \
		const __typeof(((type*)nullptr)->member) * __ptr = (ptr); \
		(type*)((char*)__ptr - offsetof(type, member)); \
	})
#endif /* container_of */

#define FPT_IS_POWER2(value) \
	(((value) & ((value) - 1LL)) == 0 && (value) > 0)
#define __FPT_FLOOR_MASK(type, value, mask) \
	((value) & ~(type)(mask))
#define __FPT_CEIL_MASK(type, value, mask) \
	__FPT_FLOOR_MASK(type, (value) + (mask), mask)
#define FPT_ALIGN_FLOOR(value, align) \
	__FPT_FLOOR_MASK(__typeof(value), value, (align) - 1LL)
#define FPT_ALIGN_CEIL(value, align) \
	__FPT_CEIL_MASK(__typeof(value), value, (align) - 1LL)
#define FPT_IS_ALIGNED(ptr, align) \
	((((align) - 1LL) & ((__typeof(align))((uintptr_t)(ptr)))) == 0)

//----------------------------------------------------------------------

static __inline
unsigned fpt_get_col(uint16_t packed) {
	return packed >> fpt_co_shift;
}

static __inline
fpt_type fpt_get_type(unsigned packed) {
	return (fpt_type) (packed & fpt_ty_mask);
}

static __inline
unsigned fpt_pack_coltype(unsigned column, unsigned type) {
	assert(type <= fpt_ty_mask);
	assert(column <= fpt_max_cols);
	return type + (column << fpt_co_shift);
}

static __inline
bool fpt_ct_match(const fpt_field* pf, unsigned column, int type_or_filter) {
	if (fpt_get_col(pf->ct) != column)
		return false;
	if (type_or_filter & fpt_filter)
		return (type_or_filter & (1 << fpt_get_type(pf->ct))) ? true : false;
	return type_or_filter == fpt_get_type(pf->ct);
}

typedef union fpt_payload {
	uint32_t u32;
	int32_t  i32;
	uint64_t u64;
	int64_t  i64;
	float    fp32;
	double   fp64;
	char     cstr[4];
	uint8_t  fixed_opaque[8];
	struct {
		fpt_varlen varlen;
		uint32_t data[1];
	} other;
} fpt_payload;


static __inline
size_t bytes2units(size_t bytes) {
	return (bytes + fpt_unit_size - 1) >> fpt_unit_shift;
}

static __inline
size_t units2bytes(size_t units) {
	return units << fpt_unit_shift;
}

static __inline
fpt_payload* fpt_field_payload(fpt_field* pf) {
	return (fpt_payload*) &pf->body[pf->offset];
}

#ifdef __cplusplus
static __inline
const fpt_payload* fpt_field_payload(const fpt_field* pf) {
	return (const fpt_payload*) &pf->body[pf->offset];
}
#endif /* __cplusplus */

extern const uint8_t fpt_internal_map_t2b[];
extern const uint8_t fpt_internal_map_t2u[];

static __inline
bool ct_is_fixedsize(unsigned ct) {
	return fpt_get_type(ct) < fpt_string;
}

static __inline
bool ct_is_dead(unsigned ct) {
	return ct >= (fpt_co_dead << fpt_co_shift);
}

static __inline
size_t ct_elem_size(unsigned ct) {
	unsigned type = fpt_get_type(ct);
	if (likely(ct_is_fixedsize(type)))
		return fpt_internal_map_t2b[type];

	/* fpt_opaque, fpt_string or fpt_farray.
	 * at least 4 bytes for length or '\0'. */
	return fpt_unit_size;
}

static __inline
bool ct_match_fixedsize(unsigned ct, unsigned units) {
	return ct_is_fixedsize(ct)
		&& units == fpt_internal_map_t2u[fpt_get_type(ct)];
}

size_t fpt_field_units(const fpt_field* pf);

static __inline
const void* fpt_ro_detent(fpt_ro ro) {
	return (char *) ro.sys.iov_base + ro.sys.iov_len;
}

static __inline
const void* fpt_detent(const fpt_rw* rw) {
	return &rw->units[rw->end];
}

fpt_field* fpt_lookup_ct(fpt_rw* pt, unsigned ct);

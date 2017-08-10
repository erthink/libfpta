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
#include "fast_positive/config.h"

#include <assert.h>
#if defined(_MSC_VER) && defined(_ASSERTE)
#undef assert
#define assert _ASSERTE
#endif /* _MSC_VER */

/*----------------------------------------------------------------------------*/
/* Compiler's includes for builtins/intrinsics */

#if __GNUC_PREREQ(4, 4) || defined(__clang__)

#if defined(__i386__) || defined(__x86_64__)
#include <cpuid.h>
#include <x86intrin.h>
#endif
#define bswap64(v) __builtin_bswap64(v)
#define bswap32(v) __builtin_bswap32(v)
#if __GNUC_PREREQ(4, 8) || __has_builtin(__builtin_bswap16)
#define bswap16(v) __builtin_bswap16(v)
#endif

#elif defined(_MSC_VER)

#if _MSC_FULL_VER < 190024218
#error At least "Microsoft C/C++ Compiler" version 19.00.24218 (Visual Studio 2015 Update 5) is required.
#endif

#pragma warning(push, 1)

#include <intrin.h>
#define bswap64(v) _byteswap_uint64(v)
#define bswap32(v) _byteswap_ulong(v)
#define bswap16(v) _byteswap_ushort(v)
#define rot64(v, s) _rotr64(v, s)
#define rot32(v, s) _rotr(v, s)

#if defined(_M_ARM64) || defined(_M_X64) || defined(_M_IA64)
#pragma intrinsic(_umul128)
#define umul_64x64_128(a, b, ph) _umul128(a, b, ph)
#pragma intrinsic(__umulh)
#define umul_64x64_high(a, b) __umulh(a, b)
#endif

#if defined(_M_IX86)
#pragma intrinsic(__emulu)
#define umul_32x32_64(a, b) __emulu(a, b)
#elif defined(_M_ARM)
#define umul_32x32_64(a, b) _arm_umull(a, b)
#endif

#endif /* Compiler */

#ifndef rot64
static __inline uint64_t rot64(uint64_t v, unsigned s) {
  return (v >> s) | (v << (64 - s));
}
#endif /* rot64 */

#ifndef rot32
static __inline uint32_t rot32(uint32_t v, unsigned s) {
  return (v >> s) | (v << (32 - s));
}
#endif /* rot32 */

#ifndef umul_32x32_64
static __inline uint64_t umul_32x32_64(uint32_t a, uint32_t b) {
  return a * (uint64_t)b;
}
#endif /* umul_32x32_64 */

#ifndef umul_64x64_128
static __inline uint64_t umul_64x64_128(uint64_t a, uint64_t b, uint64_t *h) {
#if defined(__SIZEOF_INT128__) ||                                              \
    (defined(_INTEGRAL_MAX_BITS) && _INTEGRAL_MAX_BITS >= 128)
  __uint128_t r = (__uint128_t)a * (__uint128_t)b;
  /* modern GCC could nicely optimize this */
  *h = r >> 64;
  return r;
#elif defined(umul_64x64_high)
  *h = umul_64x64_high(a, b);
  return a * b;
#else
  return fpta_umul_64x64_128(a, b, h);
#endif
}
#endif /* umul_64x64_128() */

#ifndef umul_64x64_high
static __inline uint64_t umul_64x64_high(uint64_t a, uint64_t b) {
  uint64_t h;
  umul_64x64_128(a, b, &h);
  return h;
}
#endif /* umul_64x64_high */

#if !defined(UNALIGNED_OK)
#if defined(__i386) || defined(__x86_64__) || defined(_M_IX86) ||              \
    defined(_M_X64) || defined(i386) || defined(_X86_) || defined(__i386__) || \
    defined(_X86_64_)
#define UNALIGNED_OK 1
#else
#define UNALIGNED_OK 0
#endif
#endif /* UNALIGNED_OK */

/*----------------------------------------------------------------------------*/
/* Byteorder */

#if defined(HAVE_ENDIAN_H)
#include <endian.h>
#elif defined(HAVE_SYS_PARAM_H)
#include <sys/param.h> /* for endianness */
#elif defined(HAVE_NETINET_IN_H) && defined(HAVE_RESOLV_H)
#include <netinet/in.h>
#include <resolv.h> /* defines BYTE_ORDER on HPUX and Solaris */
#endif

#ifdef HAVE_BYTESWAP_H
#include <byteswap.h>
#endif

#if !defined(__BYTE_ORDER__) || !defined(__ORDER_LITTLE_ENDIAN__) ||           \
    !defined(__ORDER_BIG_ENDIAN__)

#if defined(__BYTE_ORDER) && defined(__LITTLE_ENDIAN) && defined(__BIG_ENDIAN)
#define __ORDER_LITTLE_ENDIAN__ __LITTLE_ENDIAN
#define __ORDER_BIG_ENDIAN__ __BIG_ENDIAN
#define __BYTE_ORDER__ __BYTE_ORDER
#else
#define __ORDER_LITTLE_ENDIAN__ 1234
#define __ORDER_BIG_ENDIAN__ 4321
#if defined(__LITTLE_ENDIAN__) || defined(_LITTLE_ENDIAN) ||                   \
    defined(__ARMEL__) || defined(__THUMBEL__) || defined(__AARCH64EL__) ||    \
    defined(__MIPSEL__) || defined(_MIPSEL) || defined(__MIPSEL) ||            \
    defined(__i386) || defined(__x86_64__) || defined(_M_IX86) ||              \
    defined(_M_X64) || defined(i386) || defined(_X86_) || defined(__i386__) || \
    defined(_X86_64_) || defined(_M_ARM) || defined(_M_ARM64) ||               \
    defined(__e2k__)
#define __BYTE_ORDER__ __ORDER_LITTLE_ENDIAN__
#elif defined(__BIG_ENDIAN__) || defined(_BIG_ENDIAN) || defined(__ARMEB__) || \
    defined(__THUMBEB__) || defined(__AARCH64EB__) || defined(__MIPSEB__) ||   \
    defined(_MIPSEB) || defined(__MIPSEB) || defined(_M_IA64)
#define __BYTE_ORDER__ __ORDER_BIG_ENDIAN__
#else
#error __BYTE_ORDER__ should be defined.
#endif
#endif

#endif /* __BYTE_ORDER__ || __ORDER_LITTLE_ENDIAN__ || __ORDER_BIG_ENDIAN__ */

#ifndef bswap64
#if defined(bswap_64)
#define bswap64 bswap_64
#elif defined(__bswap_64)
#define bswap64 __bswap_64
#else
static __inline uint64_t bswap64(uint64_t v) {
  return v << 56 | v >> 56 | ((v << 40) & UINT64_C(0x00ff000000000000)) |
         ((v << 24) & UINT64_C(0x0000ff0000000000)) |
         ((v << 8) & UINT64_C(0x000000ff00000000)) |
         ((v >> 8) & UINT64_C(0x00000000ff000000)) |
         ((v >> 24) & UINT64_C(0x0000000000ff0000)) |
         ((v >> 40) & UINT64_C(0x000000000000ff00));
}
#endif
#endif /* bswap64 */

#ifndef bswap32
#if defined(bswap_32)
#define bswap32 bswap_32
#elif defined(__bswap_32)
#define bswap32 __bswap_32
#else
static __inline uint32_t bswap32(uint32_t v) {
  return v << 24 | v >> 24 | ((v << 8) & UINT32_C(0x00ff0000)) |
         ((v >> 8) & UINT32_C(0x0000ff00));
}
#endif
#endif /* bswap32 */

#ifndef bswap16
#if defined(bswap_16)
#define bswap16 bswap_16
#elif defined(__bswap_16)
#define bswap16 __bswap_16
#else
static __inline uint16_t bswap16(uint16_t v) { return v << 8 | v >> 8; }
#endif
#endif /* bswap16 */

#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__

#if !defined(htobe16) || !defined(htole16) || !defined(be16toh) ||             \
    !defined(le16toh)
#define htobe16(x) bswap16(x)
#define htole16(x) (x)
#define be16toh(x) bswap16(x)
#define le16toh(x) (x)
#endif

#if !defined(htobe32) || !defined(htole32) || !defined(be32toh) ||             \
    !defined(le32toh)
#define htobe32(x) bswap32(x)
#define htole32(x) (x)
#define be32toh(x) bswap32(x)
#define le32toh(x) (x)
#endif

#if !defined(htobe64) || !defined(htole64) || !defined(be64toh) ||             \
    !defined(le64toh)
#define htobe64(x) bswap64(x)
#define htole64(x) (x)
#define be64toh(x) bswap64(x)
#define le64toh(x) (x)
#endif

#elif __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__

#if !defined(htobe16) || !defined(htole16) || !defined(be16toh) ||             \
    !defined(le16toh)
#define htobe16(x) (x)
#define htole16(x) bswap16(x)
#define be16toh(x) (x)
#define le16toh(x) bswap16(x)
#endif

#if !defined(htobe32) || !defined(htole32) || !defined(be32toh) ||             \
    !defined(le32toh)
#define htobe32(x) (x)
#define htole32(x) bswap32(x)
#define be32toh(x) (x)
#define le32toh(x) bswap32(x)
#endif

#if !defined(htobe64) || !defined(htole64) || !defined(be64toh) ||             \
    !defined(le64toh)
#define htobe64(x) (x)
#define htole64(x) bswap64(x)
#define be64toh(x) (x)
#define le64toh(x) bswap64(x)
#endif

#else
#error Unsupported byte order.
#endif /* htole / htobe / htole / letoh */

template <typename integer> inline integer htole(integer value);
template <> inline uint16_t htole<uint16_t>(uint16_t value) {
  return htole16(value);
}
template <> inline int16_t htole<int16_t>(int16_t value) {
  return htole16(value);
}
template <> inline uint32_t htole<uint32_t>(uint32_t value) {
  return htole32(value);
}
template <> inline int32_t htole<int32_t>(int32_t value) {
  return htole32(value);
}
template <> inline uint64_t htole<uint64_t>(uint64_t value) {
  return htole64(value);
}
template <> inline int64_t htole<int64_t>(int64_t value) {
  return htole64(value);
}

template <typename integer> inline integer htobe(integer value);
template <> inline uint16_t htobe<uint16_t>(uint16_t value) {
  return htobe16(value);
}
template <> inline int16_t htobe<int16_t>(int16_t value) {
  return htobe16(value);
}
template <> inline uint32_t htobe<uint32_t>(uint32_t value) {
  return htobe32(value);
}
template <> inline int32_t htobe<int32_t>(int32_t value) {
  return htobe32(value);
}
template <> inline uint64_t htobe<uint64_t>(uint64_t value) {
  return htobe64(value);
}
template <> inline int64_t htobe<int64_t>(int64_t value) {
  return htobe64(value);
}

template <typename integer> inline integer letoh(integer value);
template <> inline uint16_t letoh<uint16_t>(uint16_t value) {
  return le16toh(value);
}
template <> inline int16_t letoh<int16_t>(int16_t value) {
  return le16toh(value);
}
template <> inline uint32_t letoh<uint32_t>(uint32_t value) {
  return le32toh(value);
}
template <> inline int32_t letoh<int32_t>(int32_t value) {
  return le32toh(value);
}
template <> inline uint64_t letoh<uint64_t>(uint64_t value) {
  return le64toh(value);
}
template <> inline int64_t letoh<int64_t>(int64_t value) {
  return le64toh(value);
}

template <typename integer> inline integer betoh(integer value);
template <> inline uint16_t betoh<uint16_t>(uint16_t value) {
  return be16toh(value);
}
template <> inline int16_t betoh<int16_t>(int16_t value) {
  return be16toh(value);
}
template <> inline uint32_t betoh<uint32_t>(uint32_t value) {
  return be32toh(value);
}
template <> inline int32_t betoh<int32_t>(int32_t value) {
  return be32toh(value);
}
template <> inline uint64_t betoh<uint64_t>(uint64_t value) {
  return be64toh(value);
}
template <> inline int64_t betoh<int64_t>(int64_t value) {
  return be64toh(value);
}

/*----------------------------------------------------------------------------*/
/* Threads */

#ifdef CMAKE_HAVE_PTHREAD_H
#include <pthread.h>

typedef struct fpta_rwl { pthread_rwlock_t prwl; } fpta_rwl_t;

static int __inline fpta_rwl_init(fpta_rwl_t *rwl) {
  return pthread_rwlock_init(&rwl->prwl, NULL);
}

static int __inline fpta_rwl_sharedlock(fpta_rwl_t *rwl) {
  return pthread_rwlock_rdlock(&rwl->prwl);
}

static int __inline fpta_rwl_exclusivelock(fpta_rwl_t *rwl) {
  return pthread_rwlock_wrlock(&rwl->prwl);
}

static int __inline fpta_rwl_unlock(fpta_rwl_t *rwl) {
  return pthread_rwlock_unlock(&rwl->prwl);
}

static int __inline fpta_rwl_destroy(fpta_rwl_t *rwl) {
  return pthread_rwlock_destroy(&rwl->prwl);
}

typedef struct fpta_mutex { pthread_mutex_t ptmx; } fpta_mutex_t;

static int __inline fpta_mutex_init(fpta_mutex_t *mutex) {
  return pthread_mutex_init(&mutex->ptmx, NULL);
}

static int __inline fpta_mutex_lock(fpta_mutex_t *mutex) {
  return pthread_mutex_lock(&mutex->ptmx);
}

static int __inline fpta_mutex_trylock(fpta_mutex_t *mutex) {
  return pthread_mutex_trylock(&mutex->ptmx);
}

static int __inline fpta_mutex_unlock(fpta_mutex_t *mutex) {
  return pthread_mutex_unlock(&mutex->ptmx);
}

static int __inline fpta_mutex_destroy(fpta_mutex_t *mutex) {
  return pthread_mutex_destroy(&mutex->ptmx);
}

#else

#ifdef _MSC_VER
#pragma warning(disable : 4514) /* 'xyz': unreferenced inline function         \
                                   has been removed */
#pragma warning(disable : 4127) /* conditional expression is constant          \
                                   */

#pragma warning(push, 1)
#pragma warning(disable : 4530) /* C++ exception handler used, but             \
                                   unwind semantics are not enabled. Specify   \
                                   /EHsc */
#pragma warning(disable : 4577) /* 'noexcept' used with no exception           \
                                   handling mode specified; termination on     \
                                   exception is not guaranteed. Specify /EHsc  \
                                   */
#endif                          /* _MSC_VER (warnings) */

#include <windows.h>

#ifdef _MSC_VER
#pragma warning(pop)
#endif

enum fpta_rwl_state : ptrdiff_t {
  SRWL_FREE = (ptrdiff_t)0xF08EE00Fl,
  SRWL_RDLC = (ptrdiff_t)0xF0008D1Cl,
  SRWL_POISON = (ptrdiff_t)0xDEADBEEFl,
};

typedef struct fpta_rwl {
  SRWLOCK srwl;
  ptrdiff_t state;
} fpta_rwl_t;

static __inline void __srwl_set_state(fpta_rwl_t *rwl, ptrdiff_t state) {
  assert((ptrdiff_t)GetCurrentThreadId() != SRWL_FREE);
  assert((ptrdiff_t)GetCurrentThreadId() != SRWL_RDLC);
  assert((ptrdiff_t)GetCurrentThreadId() != SRWL_POISON);
  rwl->state = state;
}

static __inline ptrdiff_t __srwl_get_state(fpta_rwl_t *rwl) {
  assert((ptrdiff_t)GetCurrentThreadId() != SRWL_FREE);
  assert((ptrdiff_t)GetCurrentThreadId() != SRWL_RDLC);
  assert((ptrdiff_t)GetCurrentThreadId() != SRWL_POISON);
  return rwl->state;
}

static int __inline fpta_rwl_init(fpta_rwl_t *rwl) {
  if (!rwl)
    return ERROR_INVALID_PARAMETER;
  InitializeSRWLock(&rwl->srwl);
  __srwl_set_state(rwl, SRWL_FREE);
  return ERROR_SUCCESS;
}

static int __inline fpta_rwl_sharedlock(fpta_rwl_t *rwl) {
  if (!rwl || __srwl_get_state(rwl) == SRWL_POISON)
    return ERROR_INVALID_PARAMETER;

  AcquireSRWLockShared(&rwl->srwl);
  __srwl_set_state(rwl, SRWL_RDLC);
  return ERROR_SUCCESS;
}

static int __inline fpta_rwl_exclusivelock(fpta_rwl_t *rwl) {
  if (!rwl || __srwl_get_state(rwl) == SRWL_POISON)
    return ERROR_INVALID_PARAMETER;
  AcquireSRWLockExclusive(&rwl->srwl);
  __srwl_set_state(rwl, (ptrdiff_t)GetCurrentThreadId());
  return ERROR_SUCCESS;
}

static int __inline fpta_rwl_unlock(fpta_rwl_t *rwl) {
  if (!rwl)
    return ERROR_INVALID_PARAMETER;

  ptrdiff_t state = __srwl_get_state(rwl);
  switch (state) {
  default:
    if (state == (ptrdiff_t)GetCurrentThreadId()) {
      __srwl_set_state(rwl, SRWL_FREE);
      ReleaseSRWLockExclusive(&rwl->srwl);
      return ERROR_SUCCESS;
    }
  case SRWL_FREE:
#ifndef EPERM
    return EPERM;
#endif
  case SRWL_POISON:
    return ERROR_INVALID_PARAMETER;
  case SRWL_RDLC:
    ReleaseSRWLockShared(&rwl->srwl);
    return ERROR_SUCCESS;
  }
}

static int __inline fpta_rwl_destroy(fpta_rwl_t *rwl) {
  if (!rwl || __srwl_get_state(rwl) == SRWL_POISON)
    return ERROR_INVALID_PARAMETER;
  AcquireSRWLockExclusive(&rwl->srwl);
  __srwl_set_state(rwl, SRWL_POISON);
  return ERROR_SUCCESS;
}

typedef struct fpta_mutex { CRITICAL_SECTION cs; } fpta_mutex_t;

static int __inline fpta_mutex_init(fpta_mutex_t *mutex) {
  if (!mutex)
    return ERROR_INVALID_PARAMETER;
  InitializeCriticalSection(&mutex->cs);
  return ERROR_SUCCESS;
}

static int __inline fpta_mutex_lock(fpta_mutex_t *mutex) {
  if (!mutex)
    return ERROR_INVALID_PARAMETER;
  EnterCriticalSection(&mutex->cs);
  return ERROR_SUCCESS;
}

#ifdef EBUSY
static int __inline fpta_mutex_trylock(fpta_mutex_t *mutex) {
  if (!mutex)
    return ERROR_INVALID_PARAMETER;
  return TryEnterCriticalSection(&mutex->cs) ? ERROR_SUCCESS : EBUSY;
}
#endif /* EBUSY */

static int __inline fpta_mutex_unlock(fpta_mutex_t *mutex) {
  if (!mutex)
    return ERROR_INVALID_PARAMETER;
  LeaveCriticalSection(&mutex->cs);
  return ERROR_SUCCESS;
}

static int __inline fpta_mutex_destroy(fpta_mutex_t *mutex) {
  if (!mutex)
    return ERROR_INVALID_PARAMETER;
  DeleteCriticalSection(&mutex->cs);
  return ERROR_SUCCESS;
}

#endif /* CMAKE_HAVE_PTHREAD_H */

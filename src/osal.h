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
  __srwl_set_state(rwl, GetCurrentThreadId());
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

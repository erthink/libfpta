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

#include "fast_positive/tuples_internal.h"

size_t fptu_space(size_t items, size_t data_bytes) {
  if (items > fptu_max_fields)
    items = fptu_max_fields;
  if (data_bytes > fptu_max_tuple_bytes)
    data_bytes = fptu_max_tuple_bytes;

  return sizeof(fptu_rw) + items * fptu_unit_size +
         FPT_ALIGN_CEIL(data_bytes, fptu_unit_size);
}

fptu_rw *fptu_init(void *space, size_t buffer_bytes, size_t items_limit) {
  if (unlikely(space == nullptr || items_limit > fptu_max_fields))
    return nullptr;

  if (unlikely(buffer_bytes < sizeof(fptu_rw) + fptu_unit_size * items_limit))
    return nullptr;

  if (unlikely(buffer_bytes > fptu_buffer_limit))
    return nullptr;

  fptu_rw *pt = (fptu_rw *)space;
  // make a empty tuple
  pt->end = (unsigned)(buffer_bytes - sizeof(fptu_rw)) / fptu_unit_size + 1;
  pt->head = pt->tail = pt->pivot = (unsigned)items_limit + 1;
  pt->junk = 0;
  return pt;
}

int fptu_clear(fptu_rw *pt) {
  if (unlikely(pt == nullptr))
    return FPTU_EINVAL;
  if (unlikely(pt->pivot < 1 || pt->pivot > fptu_max_fields + 1 ||
               pt->pivot >= pt->end ||
               pt->end > bytes2units(fptu_buffer_limit)))
    return FPTU_EINVAL;

  pt->head = pt->tail = pt->pivot;
  pt->junk = 0;
  return FPTU_OK;
}

size_t fptu_space4items(const fptu_rw *pt) {
  return (pt->head > 0) ? pt->head - 1 : 0;
}

size_t fptu_space4data(const fptu_rw *pt) {
  return units2bytes(pt->end - pt->tail);
}

size_t fptu_junkspace(const fptu_rw *pt) { return units2bytes(pt->junk); }

//----------------------------------------------------------------------------

fptu_rw *fptu_fetch(fptu_ro ro, void *space, size_t buffer_bytes,
                    unsigned more_items) {
  if (ro.total_bytes == 0)
    return fptu_init(space, buffer_bytes, more_items);

  if (unlikely(ro.units == nullptr))
    return nullptr;
  if (unlikely(ro.total_bytes < fptu_unit_size))
    return nullptr;
  if (unlikely(ro.total_bytes > fptu_max_tuple_bytes))
    return nullptr;
  if (unlikely(ro.total_bytes !=
               units2bytes(1 + (size_t)ro.units[0].varlen.brutto)))
    return nullptr;

  size_t items = (size_t)ro.units[0].varlen.tuple_items & fptu_lt_mask;
  if (unlikely(items > fptu_max_fields))
    return nullptr;
  if (unlikely(space == nullptr || more_items > fptu_max_fields))
    return nullptr;
  if (unlikely(buffer_bytes > fptu_buffer_limit))
    return nullptr;

  const char *end = (const char *)ro.units + ro.total_bytes;
  const char *begin = (const char *)&ro.units[1];
  const char *pivot = (const char *)begin + units2bytes(items);
  if (unlikely(pivot > end))
    return nullptr;

  size_t reserve_items = items + more_items;
  if (reserve_items > fptu_max_fields)
    reserve_items = fptu_max_fields;

  ptrdiff_t payload_bytes = end - pivot;
  if (unlikely(buffer_bytes <
               sizeof(fptu_rw) + units2bytes(reserve_items) + payload_bytes))
    return nullptr;

  fptu_rw *pt = (fptu_rw *)space;
  pt->end = (unsigned)(buffer_bytes - sizeof(fptu_rw)) / fptu_unit_size + 1;
  pt->pivot = (unsigned)reserve_items + 1;
  pt->head = pt->pivot - (unsigned)items;
  pt->tail = pt->pivot + (unsigned)(payload_bytes >> fptu_unit_shift);
  pt->junk = 0;

  memcpy(&pt->units[pt->head], begin, ro.total_bytes - fptu_unit_size);
  return pt;
}

static size_t more_buffer_size(const fptu_ro &ro, unsigned more_items,
                               unsigned more_payload) {
  size_t items = (size_t)ro.units[0].varlen.tuple_items & fptu_lt_mask;
  size_t payload_bytes = ro.total_bytes - units2bytes(items + 1);
  return fptu_space(items + more_items, payload_bytes + more_payload);
}

size_t fptu_check_and_get_buffer_size(fptu_ro ro, unsigned more_items,
                                      unsigned more_payload,
                                      const char **error) {
  if (unlikely(error == nullptr))
    return ~(size_t)0;

  *error = fptu_check_ro(ro);
  if (unlikely(*error != nullptr))
    return 0;

  if (unlikely(more_items > fptu_max_fields)) {
    *error = "more_items > fptu_max_fields";
    return 0;
  }
  if (unlikely(more_payload > fptu_max_tuple_bytes)) {
    *error = "more_payload > fptu_max_tuple_bytes";
    return 0;
  }

  return more_buffer_size(ro, more_items, more_payload);
}

size_t fptu_get_buffer_size(fptu_ro ro, unsigned more_items,
                            unsigned more_payload) {
  if (more_items > fptu_max_fields)
    more_items = fptu_max_fields;
  if (more_payload > fptu_max_tuple_bytes)
    more_payload = fptu_max_tuple_bytes;
  return more_buffer_size(ro, more_items, more_payload);
}

//----------------------------------------------------------------------------

// TODO: split out
fptu_rw *fptu_alloc(size_t items_limit, size_t data_bytes) {
  if (unlikely(items_limit > fptu_max_fields ||
               data_bytes > fptu_max_tuple_bytes))
    return nullptr;

  size_t size = fptu_space(items_limit, data_bytes);
  void *buffer = malloc(size);
  if (unlikely(!buffer))
    return nullptr;

  fptu_rw *pt = fptu_init(buffer, size, items_limit);
  assert(pt != nullptr);

  return pt;
}

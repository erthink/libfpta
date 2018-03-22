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

__hot const fptu_field *fptu_first(const fptu_field *begin,
                                   const fptu_field *end, unsigned column,
                                   int type_or_filter) {
  if (type_or_filter & fptu_filter) {
    for (const fptu_field *pf = begin; pf < end; ++pf) {
      if (fptu_ct_match(pf, column, type_or_filter))
        return pf;
    }
  } else {
    uint_fast16_t ct = fptu_pack_coltype(column, type_or_filter);
    for (const fptu_field *pf = begin; pf < end; ++pf) {
      if (pf->ct == ct)
        return pf;
    }
  }
  return end;
}

__hot const fptu_field *fptu_next(const fptu_field *from, const fptu_field *end,
                                  unsigned column, int type_or_filter) {
  return fptu_first(from + 1, end, column, type_or_filter);
}

//----------------------------------------------------------------------------

__hot const fptu_field *fptu_first_ex(const fptu_field *begin,
                                      const fptu_field *end,
                                      fptu_field_filter filter, void *context,
                                      void *param) {
  for (const fptu_field *pf = begin; pf < end; ++pf) {
    if (ct_is_dead(pf->ct))
      continue;
    if (filter(pf, context, param))
      return pf;
  }
  return end;
}

__hot const fptu_field *fptu_next_ex(const fptu_field *from,
                                     const fptu_field *end,
                                     fptu_field_filter filter, void *context,
                                     void *param) {
  return fptu_first_ex(from + 1, end, filter, context, param);
}

//----------------------------------------------------------------------------

__hot const fptu_field *fptu_begin_ro(fptu_ro ro) {
  if (unlikely(ro.total_bytes < fptu_unit_size))
    return nullptr;
  if (unlikely(ro.total_bytes !=
               fptu_unit_size + units2bytes(ro.units[0].varlen.brutto)))
    return nullptr;

  return &ro.units[1].field;
}

__hot const fptu_field *fptu_end_ro(fptu_ro ro) {
  if (unlikely(ro.total_bytes < fptu_unit_size))
    return nullptr;
  if (unlikely(ro.total_bytes !=
               fptu_unit_size + units2bytes(ro.units[0].varlen.brutto)))
    return nullptr;

  size_t items = (size_t)ro.units[0].varlen.tuple_items & fptu_lt_mask;
  return &ro.units[1 + items].field;
}

//----------------------------------------------------------------------------

__hot const fptu_field *fptu_begin_rw(const fptu_rw *pt) {
  return &pt->units[pt->head].field;
}

__hot const fptu_field *fptu_end_rw(const fptu_rw *pt) {
  return &pt->units[pt->pivot].field;
}

//----------------------------------------------------------------------------

size_t fptu_field_count(const fptu_rw *pt, unsigned column,
                        int type_or_filter) {
  const fptu_field *end = fptu_end_rw(pt);
  const fptu_field *begin = fptu_begin_rw(pt);
  const fptu_field *pf = fptu_first(begin, end, column, type_or_filter);

  size_t count;
  for (count = 0; pf != end; pf = fptu_next(pf, end, column, type_or_filter))
    count++;

  return count;
}

size_t fptu_field_count_ro(fptu_ro ro, unsigned column, int type_or_filter) {
  const fptu_field *end = fptu_end_ro(ro);
  const fptu_field *begin = fptu_begin_ro(ro);
  const fptu_field *pf = fptu_first(begin, end, column, type_or_filter);

  size_t count;
  for (count = 0; pf != end; pf = fptu_next(pf, end, column, type_or_filter))
    count++;

  return count;
}

size_t fptu_field_count_ex(const fptu_rw *pt, fptu_field_filter filter,
                           void *context, void *param) {
  const fptu_field *end = fptu_end_rw(pt);
  const fptu_field *begin = fptu_begin_rw(pt);
  const fptu_field *pf = fptu_first_ex(begin, end, filter, context, param);

  size_t count;
  for (count = 0; pf != end; pf = fptu_next_ex(pf, end, filter, context, param))
    count++;

  return count;
}

size_t fptu_field_count_ro_ex(fptu_ro ro, fptu_field_filter filter,
                              void *context, void *param) {
  const fptu_field *end = fptu_end_ro(ro);
  const fptu_field *begin = fptu_begin_ro(ro);
  const fptu_field *pf = fptu_first_ex(begin, end, filter, context, param);

  size_t count;
  for (count = 0; pf != end; pf = fptu_next_ex(pf, end, filter, context, param))
    count++;

  return count;
}

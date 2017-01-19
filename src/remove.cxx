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

#include "fast_positive/tuples_internal.h"

static __inline bool fptu_is_tailed(fptu_rw *pt, fptu_field *pf,
                                    size_t units) {
  assert(pf == &pt->units[pt->head].field);

  return units == 0 ||
         &pf->body[pf->offset + units] == &pt->units[pt->tail].data;
}

void fptu_erase_field(fptu_rw *pt, fptu_field *pf) {
  if (unlikely(ct_is_dead(pf->ct)))
    return;

  // mark field as `dead`
  pf->ct |= fptu_co_dead << fptu_co_shift;
  size_t units = fptu_field_units(pf);

  // head & tail optimization
  if (pf != &pt->units[pt->head].field || !fptu_is_tailed(pt, pf, units)) {
    // account junk
    pt->junk += units + 1;
    return;
  }

  // cutoff head and tail
  pt->head += 1;
  pt->tail -= units;

  // continue cutting junk
  while (pt->head < pt->pivot) {
    pf = &pt->units[pt->head].field;
    if (!ct_is_dead(pf->ct))
      break;

    units = fptu_field_units(pf);
    if (!fptu_is_tailed(pt, pf, units))
      break;

    assert(pt->junk >= units + 1);
    pt->junk -= units + 1;
    pt->head += 1;
    pt->tail -= units;
  }
}

int fptu_erase(fptu_rw *pt, unsigned column, int type_or_filter) {
  if (unlikely(column > fptu_max_cols))
    return -FPTU_EINVAL;

  if (type_or_filter & fptu_filter) {
    int count = 0;
    fptu_field *begin = &pt->units[pt->head].field;
    fptu_field *pivot = &pt->units[pt->pivot].field;
    for (fptu_field *pf = begin; pf < pivot; ++pf) {
      if (fptu_ct_match(pf, column, type_or_filter)) {
        fptu_erase_field(pt, pf);
        count++;
      }
    }
    return count;
  }

  fptu_field *pf =
      fptu_lookup_ct(pt, fptu_pack_coltype(column, type_or_filter));
  if (pf == nullptr)
    return 0;

  fptu_erase_field(pt, pf);
  return 1;
}

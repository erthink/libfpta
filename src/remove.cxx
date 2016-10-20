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

#include "fast_positive/tuples.h"
#include "internal.h"

void fpt_erase_field(fpt_rw* pt, fpt_field *pf) {
    if (unlikely(ct_is_dead(pf->ct)))
        return;

    // mark field as `dead`
    pf->ct |= fpt_co_dead << fpt_co_shift;

    size_t units = fpt_field_units(pf);

    // head & tail optimization
    if (pf == &pt->units[pt->head].field) {
        if (&pf->body[pf->offset + units] == &pt->units[pt->tail].data) {
            // cutoff head and tail
            pt->head += 1;
            pt->tail -= units;

            // continue cutting junk
            while (pt->head < pt->pivot) {
                pf = &pt->units[pt->head].field;
                if (! ct_is_dead(pf->ct))
                    break;

                units = fpt_field_units(pf);
                if (&pf->body[pf->offset + units] != &pt->units[pt->tail].data)
                    break;

                assert(pt->junk >= units + 1);
                pt->junk -= units + 1;
                pt->head += 1;
                pt->tail -= units;
            }
        }
    } else {
        // account junk
        pt->junk += units + 1;
    }
}

int fpt_erase(fpt_rw* pt, unsigned column, int type_or_filter) {
    if (unlikely(column > fpt_max_cols))
        return fpt_einval;

    if (type_or_filter & fpt_filter) {
        int count = 0;
        fpt_field *begin = &pt->units[pt->head].field;
        fpt_field *pivot = &pt->units[pt->pivot].field;
        for (fpt_field* pf = begin; pf < pivot; ++pf) {
            if (fpt_ct_match(pf, column, type_or_filter)) {
                fpt_erase_field(pt, pf);
                count++;
            }
        }
        return count;
    }

    fpt_field *pf = fpt_lookup_ct(pt, fpt_pack_coltype(column, type_or_filter));
    if (pf == nullptr)
        return 0;

    fpt_erase_field(pt, pf);
    return 1;
}

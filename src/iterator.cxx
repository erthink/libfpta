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

const fpt_field* fpt_first(const fpt_field* begin, const fpt_field* end,
                           unsigned column, int type_or_filter) {
    if (type_or_filter & fpt_filter) {
        for (const fpt_field* pf = begin; pf < end; ++pf) {
            if (fpt_ct_match(pf, column, type_or_filter))
                return pf;
        }
    } else {
        unsigned ct = fpt_pack_coltype(column, type_or_filter);
        for (const fpt_field* pf = begin; pf < end; ++pf) {
            if (pf->ct == ct)
                return pf;
        }
    }
    return end;
}

const fpt_field* fpt_next(const fpt_field* from, const fpt_field* end,
                          unsigned column, int type_or_filter) {
    return fpt_first(from + 1, end, column, type_or_filter);
}

//----------------------------------------------------------------------

const fpt_field* fpt_first_ex(const fpt_field* begin, const fpt_field* end,
                              fpt_field_filter filter, void* context, void *param) {
    for (const fpt_field* pf = begin; pf < end; ++pf) {
        if (filter(pf, context, param))
            return pf;
    }
    return end;
}

const fpt_field* fpt_next_ex(const fpt_field* from, const fpt_field* end,
                             fpt_field_filter filter, void* context, void *param) {
    return fpt_first_ex(from + 1, end, filter, context, param);
}

//----------------------------------------------------------------------

const fpt_field* fpt_begin_ro(fpt_ro ro) {
    if (unlikely(ro.total_bytes < fpt_unit_size))
        return nullptr;
    if (unlikely(ro.total_bytes != fpt_unit_size
            + fpt_unit_size * (size_t) ro.units[0].varlen.brutto))
        return nullptr;

    return &ro.units[1].field;
}

const fpt_field* fpt_end_ro(fpt_ro ro) {
    if (unlikely(ro.total_bytes < fpt_unit_size))
        return nullptr;
    if (unlikely(ro.total_bytes != fpt_unit_size
            + fpt_unit_size * (size_t) ro.units[0].varlen.brutto))
        return nullptr;

    unsigned items = ro.units[0].varlen.tuple_items & fpt_lt_mask;
    return &ro.units[1 + items].field;
}

//----------------------------------------------------------------------

const fpt_field* fpt_begin(fpt_rw* pt) {
    return &pt->units[pt->head].field;
}

const fpt_field* fpt_end(fpt_rw* pt) {
    return &pt->units[pt->pivot].field;
}

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

#include "fast_positive/tuples_internal.h"

enum {
    fptu_unordered = 1,
    fptu_junk_header = 2,
    fptu_junk_data = 4,
    fptu_mesh = 8
};

static unsigned fptu_state(const fptu_rw *pt)
{
    const fptu_field *const begin = fptu_begin(pt);
    const fptu_field *const end = fptu_end(pt);
    const char *prev_payload = (const char *)end;
    unsigned prev_ct = 0;

    unsigned state = 0;
    for (const fptu_field *pf = end; --pf >= begin;) {
        if (pf->ct < prev_ct)
            state |= fptu_unordered;
        prev_ct = pf->ct;

        if (ct_is_dead(pf->ct)) {
            state |= (fptu_get_type(pf->ct) > fptu_uint16)
                         ? fptu_junk_header | fptu_junk_data
                         : fptu_junk_header;
        } else if (fptu_get_type(pf->ct) > fptu_uint16) {
            const char *payload = (const char *)fptu_field_payload(pf);
            if (payload < prev_payload)
                state |= fptu_mesh;
            prev_payload = payload;
        }
        if (state ==
            (fptu_unordered | fptu_junk_header | fptu_junk_data | fptu_mesh))
            break;
    }
    assert(fptu_is_ordered(begin, end) == ((state & fptu_unordered) == 0));
    return state;
}

void fptu_shrink(fptu_rw *pt)
{
    unsigned state = fptu_state(pt);
    if ((state & (fptu_junk_header | fptu_junk_data)) == 0)
        return;

    if (state & fptu_mesh) {
        // TODO: support for ordered tuples;
        assert(0 && "ordered/mesh tuples NOT yet supported");
    }

    fptu_field *begin = &pt->units[pt->head].field;
    void *pivot = &pt->units[pt->pivot];

    fptu_field f, *h = (fptu_field *)pivot;
    uint32_t *t = (uint32_t *)pivot;
    size_t shift;

    for (shift = 0; --h >= begin;) {
        f.header = h->header;
        if (ct_is_dead(f.ct)) {
            shift++;
            continue;
        }

        if (fptu_get_type(f.ct) > fptu_uint16) {
            size_t u = fptu_field_units(h);
            uint32_t *p = (uint32_t *)fptu_field_payload(h);
            assert(t <= p);
            if (t != p)
                memmove(t, p, units2bytes(u));
            size_t offset = t - h[shift].body;
            assert(offset <= fptu_limit);
            f.offset = offset;
            t += u;
        }
        if (h[shift].header != f.header)
            h[shift].header = f.header;
    }

    assert(t <= &pt->units[pt->end].data);
    pt->head += shift;
    pt->tail = t - &pt->units[0].data;
    pt->junk = 0;
}

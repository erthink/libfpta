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

#include "fast_positive/internals.h"

enum {
	fpt_unsorted    = 1,
	fpt_junk_header = 2,
	fpt_junk_data   = 4,
	fpt_mesh        = 8
};

static unsigned fpt_state(const fpt_rw* pt) {
	const fpt_field* begin = fpt_begin(pt);
	const fpt_field* end = fpt_end(pt);
	const char* last_payload = (const char*) end;
	unsigned last_ct = fpt_limit;

	unsigned state = 0;
	for(const fpt_field* pf = end; --pf >= begin; ) {
		if (pf->ct > last_ct)
			state |= fpt_unsorted;
		last_ct = pf->ct;

		if (ct_is_dead(pf->ct)) {
			state |= (fpt_get_type(pf->ct) > fpt_uint16)
					? fpt_junk_header | fpt_junk_data
					: fpt_junk_header;
		} else if (fpt_get_type(pf->ct) > fpt_uint16) {
			const char* payload = (const char*) fpt_field_payload(pf);
			if (payload < last_payload)
				state |= fpt_mesh;
			last_payload = payload;
		}
		if (state == (fpt_unsorted | fpt_junk_header | fpt_junk_data | fpt_mesh))
			break;
	}
	return state;
}

void fpt_shrink(fpt_rw* pt) {
	unsigned state = fpt_state(pt);
	if ((state & (fpt_junk_header | fpt_junk_data)) == 0)
		return;

	if (state & fpt_mesh) {
		// TODO: support for sorted tuples;
		assert(!"sorted/mesh tuples NOT yet supported");
	}

	fpt_field* begin = &pt->units[pt->head].field;
	void* pivot = &pt->units[pt->pivot];

	fpt_field f, *h = (fpt_field *) pivot;
	uint32_t *t = (uint32_t *) pivot;
	size_t shift;

	for (shift = 0; --h >= begin; ) {
		f.header = h->header;
		if (ct_is_dead(f.ct)) {
			shift++;
			continue;
		}

		if (fpt_get_type(f.ct) > fpt_uint16) {
			size_t u = fpt_field_units(h);
			uint32_t* p = (uint32_t*) fpt_field_payload(h);
			assert(t <= p);
			if (t != p)
				memmove(t, p, units2bytes(u));
			size_t offset = t - h[shift].body;
			assert(offset <= fpt_limit);
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

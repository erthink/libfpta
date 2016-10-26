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

__hot
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

__hot
const fpt_field* fpt_next(const fpt_field* from, const fpt_field* end,
						  unsigned column, int type_or_filter) {
	return fpt_first(from + 1, end, column, type_or_filter);
}

//----------------------------------------------------------------------

__hot
const fpt_field* fpt_first_ex(const fpt_field* begin, const fpt_field* end,
							  fpt_field_filter filter, void* context, void *param) {
	for (const fpt_field* pf = begin; pf < end; ++pf) {
		if (ct_is_dead(pf->ct))
			continue;
		if (filter(pf, context, param))
			return pf;
	}
	return end;
}

__hot
const fpt_field* fpt_next_ex(const fpt_field* from, const fpt_field* end,
							 fpt_field_filter filter, void* context, void *param) {
	return fpt_first_ex(from + 1, end, filter, context, param);
}

//----------------------------------------------------------------------

__hot
const fpt_field* fpt_begin_ro(fpt_ro ro) {
	if (unlikely(ro.total_bytes < fpt_unit_size))
		return nullptr;
	if (unlikely(ro.total_bytes != fpt_unit_size
			+ units2bytes(ro.units[0].varlen.brutto)))
		return nullptr;

	return &ro.units[1].field;
}

__hot
const fpt_field* fpt_end_ro(fpt_ro ro) {
	if (unlikely(ro.total_bytes < fpt_unit_size))
		return nullptr;
	if (unlikely(ro.total_bytes != fpt_unit_size
			+ units2bytes(ro.units[0].varlen.brutto)))
		return nullptr;

	unsigned items = ro.units[0].varlen.tuple_items & fpt_lt_mask;
	return &ro.units[1 + items].field;
}

//----------------------------------------------------------------------

__hot
const fpt_field* fpt_begin(const fpt_rw* pt) {
	return &pt->units[pt->head].field;
}

__hot
const fpt_field* fpt_end(const fpt_rw* pt) {
	return &pt->units[pt->pivot].field;
}

//----------------------------------------------------------------------

size_t fpt_field_count(const fpt_rw* pt, unsigned column, int type_or_filter) {
	const fpt_field* end = fpt_end(pt);
	const fpt_field* begin = fpt_begin(pt);
	const fpt_field* pf = fpt_first(begin, end, column, type_or_filter);

	size_t count;
	for(count = 0; pf != end; pf = fpt_next(pf, end, column, type_or_filter))
		count++;

	return count;
}

size_t fpt_field_count_ro(fpt_ro ro, unsigned column, int type_or_filter) {
	const fpt_field* end = fpt_end_ro(ro);
	const fpt_field* begin = fpt_begin_ro(ro);
	const fpt_field* pf = fpt_first(begin, end, column, type_or_filter);

	size_t count;
	for(count = 0; pf != end; pf = fpt_next(pf, end, column, type_or_filter))
		count++;

	return count;
}

size_t fpt_field_count_ex(const fpt_rw* pt, fpt_field_filter filter, void* context, void *param) {
	const fpt_field* end = fpt_end(pt);
	const fpt_field* begin = fpt_begin(pt);
	const fpt_field* pf = fpt_first_ex(begin, end, filter, context, param);

	size_t count;
	for(count = 0; pf != end; pf = fpt_next_ex(pf, end, filter, context, param))
		count++;

	return count;
}

size_t fpt_field_count_ro_ex(fpt_ro ro, fpt_field_filter filter, void* context, void *param) {
	const fpt_field* end = fpt_end_ro(ro);
	const fpt_field* begin = fpt_begin_ro(ro);
	const fpt_field* pf = fpt_first_ex(begin, end, filter, context, param);

	size_t count;
	for(count = 0; pf != end; pf = fpt_next_ex(pf, end, filter, context, param))
		count++;

	return count;
}

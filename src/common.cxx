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

const uint8_t fpt_internal_map_t2b[fpt_string] = {
	/* void */      0,
	/* uint16 */    0,

	/* int32 */     4,
	/* unt32 */     4,
	/* fp32 */      4,

	/* int64 */     8,
	/* uint64 */    8,
	/* fp64 */      8,

	/* 96 */        12,
	/* 128 */       16,
	/* 160 */       20,
	/* 192 */       24,
	/* 256 */       32
};

const uint8_t fpt_internal_map_t2u[fpt_string] = {
	/* void */      0,
	/* uint16 */    0,

	/* int32 */     1,
	/* unt32 */     1,
	/* fp32 */      1,

	/* int64 */     2,
	/* uint64 */    2,
	/* fp64 */      2,

	/* 96 */        3,
	/* 128 */       4,
	/* 160 */       5,
	/* 192 */       6,
	/* 256 */       8
};

__hot
size_t fpt_field_units(const fpt_field* pf) {
	unsigned type = fpt_get_type(pf->ct);
	if (likely(type < fpt_string)) {
		// fixed length type
		return fpt_internal_map_t2u[type];
	}

	// variable length type
	const fpt_payload* payload = fpt_field_payload(pf);
	if (type == fpt_string) {
		// length is'nt stored, but zero terminated
		return bytes2units(strlen(payload->cstr) + 1);
	} else {
		// length is stored
		return payload->other.varlen.brutto + (size_t) 1;
	}
}

//----------------------------------------------------------------------

__hot
const fpt_field* fpt_lookup_ro(fpt_ro ro, unsigned column, int type_or_filter) {
	if (unlikely(ro.total_bytes < fpt_unit_size))
		return nullptr;
	if (unlikely(ro.total_bytes != fpt_unit_size
			+ fpt_unit_size * (size_t) ro.units[0].varlen.brutto))
		return nullptr;
	if (unlikely(column > fpt_max_cols))
		return nullptr;

	unsigned items = ro.units[0].varlen.tuple_items & fpt_lt_mask;
	const fpt_field *begin = &ro.units[1].field;
	const fpt_field *end = begin + items;

	if (fpt_lx_mask & ro.units[0].varlen.tuple_items) {
		// TODO: support for sorted tuples
	}

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
	return nullptr;
}

__hot
fpt_field* fpt_lookup_ct(fpt_rw* pt, unsigned ct) {
	const fpt_field *begin = &pt->units[pt->head].field;
	const fpt_field *pivot = &pt->units[pt->pivot].field;
	for (const fpt_field* pf = begin; pf < pivot; ++pf) {
		if (pf->ct == ct)
			return (fpt_field *) pf;
	}
	return nullptr;
}

__hot
fpt_field* fpt_lookup(fpt_rw* pt, unsigned column, int type_or_filter) {
	if (unlikely(column > fpt_max_cols))
		return nullptr;

	if (type_or_filter & fpt_filter) {
		const fpt_field *begin = &pt->units[pt->head].field;
		const fpt_field *pivot = &pt->units[pt->pivot].field;
		for (const fpt_field* pf = begin; pf < pivot; ++pf) {
			if (fpt_ct_match(pf, column, type_or_filter))
				return (fpt_field *) pf;
		}
		return nullptr;
	}

	return fpt_lookup_ct(pt, fpt_pack_coltype(column, type_or_filter));
}

//----------------------------------------------------------------------

__hot
fpt_ro fpt_take_noshrink(fpt_rw* pt) {
	fpt_ro tuple;

	assert(pt->head > 0);
	ptrdiff_t items = pt->pivot - pt->head;
	fpt_payload* payload = (fpt_payload*) &pt->units[pt->head - 1];
	payload->other.varlen.brutto = pt->tail - pt->head;
	payload->other.varlen.tuple_items = items;
	// TODO: support for sorted tuples
	tuple.units = (const fpt_unit*) payload;
	tuple.total_bytes = (char*) &pt->units[pt->tail] - (char*) payload;
	return tuple;
}

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

static __hot
const char* fpt_field_check(const fpt_field* pf, const char* pivot,
							   const char *detent, size_t &payload_units) {
	payload_units = 0;
	if (unlikely(detent < (const char*) pf + fpt_unit_size))
		return "field.header > detent";

	unsigned type = fpt_get_type(pf->ct);
	if (type <= fpt_uint16)
		// without ex-data
		return nullptr;

	payload_units = 1;
	const fpt_payload* payload = fpt_field_payload(pf);
	if (unlikely((char*) payload < pivot))
		return "field.begin < tuple.pivot";

	size_t len;
	ptrdiff_t left = (char*) detent - (const char*) payload;
	if (type < fpt_string) {
		// fixed length type
		payload_units = fpt_internal_map_t2u[type];
		len = fpt_internal_map_t2b[type];
		if (unlikely((ptrdiff_t) len > left))
			return "field.end > detent";
		return nullptr;
	}

	if (unlikely(left < fpt_unit_size))
		return "field.varlen > detent";

	if (type == fpt_string) {
		// length is'nt stored, but zero terminated
		len = strnlen((const char*) payload, left) + 1;
		payload_units = bytes2units(len);
	} else {
		// length is stored
		payload_units += payload->other.varlen.brutto;
		len = units2bytes(payload_units);
	}

	if (unlikely(len > fpt_max_field_bytes))
		return "field.length > max_field_bytes";

	if (unlikely((ptrdiff_t) len > left))
		return "field.end > detent";

	if (unlikely(type & fpt_farray)) {
		// TODO
		return "arrays not yet supported";
	} else if (type == fpt_opaque) {
		len = payload->other.varlen.opaque_bytes;
		if (unlikely(payload_units != bytes2units(len) + 1))
			return "field.opaque_bytes != field.brutto";
	} else if (pf->ct == fpt_nested) {
		// TODO
		return "nested tuples not yet supported";
	}

	return nullptr;
}

const char* fpt_check_ro(fpt_ro ro) {
	if (ro.total_bytes == 0)
		// valid empty tuple
		return nullptr;

	if (unlikely(ro.units == nullptr))
		return "tuple.items.is_nullptr";

	if (unlikely(ro.total_bytes < fpt_unit_size))
		return "tuple.length_bytes < fpt_unit_size";

	if (unlikely(ro.total_bytes > fpt_max_tuple_bytes))
		return "tuple.length_bytes < max_bytes";

	if (unlikely(ro.total_bytes != units2bytes(1 + ro.units[0].varlen.brutto)))
		return "tuple.length_bytes != tuple.brutto";

	const char *detent = (const char*) ro.units + ro.total_bytes;
	size_t items = ro.units[0].varlen.tuple_items & fpt_lt_mask;
	if (unlikely(items > fpt_max_fields))
		return "tuple.items > fpt_max_fields";

	const fpt_field *scan = &ro.units[1].field;
	const char* pivot = (const char*) scan + units2bytes(items);
	if (unlikely(pivot > detent))
		return "tuple.pivot > tuple.end";

	if (fpt_lx_mask & ro.units[0].varlen.tuple_items) {
		// TODO: support for sorted tuples
	}

	size_t payload_total_bytes = 0;
	for (; (const char*) scan < pivot; ++scan) {
		size_t payload_units;
		const char* bug = fpt_field_check(scan, pivot, detent, payload_units);
		if (unlikely(bug))
			return bug;

		payload_total_bytes += units2bytes(payload_units);
		//if (ct_is_dead(scan->ct))
		//    return "tuple.has_junk";
	}

	if (unlikely(pivot + payload_total_bytes > detent))
		return "tuple.overlapped";

	if (unlikely(pivot + payload_total_bytes != detent))
		return "tuple.has_wholes";

	return nullptr;
}

const char* fpt_check(fpt_rw *pt) {
	if (unlikely(pt == nullptr))
		return "tuple.is_nullptr";

	if (unlikely(pt->head < 1))
		return "tuple.head < 1";

	if (unlikely(pt->head > pt->pivot))
		return "tuple.head > tuple.pivot";

	if (unlikely(pt->pivot > pt->tail))
		return "tuple.pivot > tuple.tail";

	if (unlikely(pt->tail > pt->end))
		return "tuple.tail > tuple.end";

	if (unlikely(pt->pivot - pt->head > fpt_max_fields))
		return "tuple.n_cols > fpt_max_fields";

	if (unlikely(pt->tail - pt->head > fpt_max_tuple_bytes/fpt_unit_size - 1))
		return "tuple.size > max_bytes";

	if (unlikely(pt->junk > pt->tail - pt->head))
		return "tuple.junk > tuple.size";

	const fpt_field *scan = &pt->units[pt->head].field;
	const char* pivot = (const char*) &pt->units[pt->pivot];
	const char* detent = (const char*) &pt->units[pt->tail];
	size_t payload_total_bytes = 0;
	size_t payload_junk_units = 0;
	size_t junk_items = 0;
	for (; (const char*) scan < pivot; ++scan) {
		size_t payload_units;
		const char* bug = fpt_field_check(scan, pivot, detent, payload_units);
		if (unlikely(bug))
			return bug;

		payload_total_bytes += units2bytes(payload_units);
		if (ct_is_dead(scan->ct)) {
			junk_items++;
			payload_junk_units += payload_units;
		}
	}

	if (unlikely(pivot + payload_total_bytes > detent))
		return "tuple.overlapped";

	if (unlikely(pt->junk != payload_junk_units + junk_items))
		return "tuple.junk != junk_items + junk_payload";

	if (unlikely(pivot + payload_total_bytes != detent))
		return "tuple.has_wholes";

	return nullptr;
}


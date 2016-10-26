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

size_t fpt_space(size_t items, size_t data_bytes) {
	if (items > fpt_max_fields)
		items = fpt_max_fields;
	if (data_bytes > fpt_max_tuple_bytes)
		data_bytes = fpt_max_tuple_bytes;

	return sizeof(fpt_rw) + items * fpt_unit_size
		+ FPT_ALIGN_CEIL(data_bytes, fpt_unit_size);
}

fpt_rw* fpt_init(void* space, size_t buffer_bytes, size_t items_limit) {
	if (unlikely(space == nullptr || items_limit > fpt_max_fields))
		return nullptr;

	if (unlikely(buffer_bytes < sizeof(fpt_rw) + fpt_unit_size * items_limit))
		return nullptr;

	if (unlikely(buffer_bytes > fpt_buffer_limit))
		return nullptr;

	fpt_rw *pt = (fpt_rw*) space;
	// make a empty tuple
	pt->end = (buffer_bytes - sizeof(fpt_rw)) / fpt_unit_size + 1;
	pt->head = pt->tail = pt->pivot = items_limit + 1;
	pt->junk = 0;
	return pt;
}

size_t fpt_space4items(const fpt_rw* pt) {
	return (pt->head > 0) ? pt->head - 1 : 0;
}

size_t fpt_space4data(const fpt_rw* pt) {
	return units2bytes(pt->end - pt->tail);
}

size_t fpt_junkspace(const fpt_rw* pt) {
	return units2bytes(pt->junk);
}

//----------------------------------------------------------------------

fpt_rw* fpt_fetch(fpt_ro ro, void* space, size_t buffer_bytes, unsigned more_items) {
	if (ro.total_bytes == 0)
		return fpt_init(space, buffer_bytes, more_items);

	if (unlikely(ro.units == nullptr))
		return nullptr;
	if (unlikely(ro.total_bytes < fpt_unit_size))
		return nullptr;
	if (unlikely(ro.total_bytes > fpt_max_tuple_bytes))
		return nullptr;
	if (unlikely(ro.total_bytes != units2bytes(1 + ro.units[0].varlen.brutto)))
		return nullptr;

	size_t items = ro.units[0].varlen.tuple_items & fpt_lt_mask;
	if (unlikely(items > fpt_max_fields))
		return nullptr;
	if (unlikely(space == nullptr || more_items > fpt_max_fields))
		return nullptr;
	if (unlikely(buffer_bytes > fpt_buffer_limit))
		return nullptr;

	const char *end = (const char*) ro.units + ro.total_bytes;
	const char *begin = (const char*) &ro.units[1];
	const char* pivot = (const char*) begin + units2bytes(items);
	if (unlikely(pivot > end))
		return nullptr;

	size_t reserve_items = items + more_items;
	if (reserve_items > fpt_max_fields)
		reserve_items = fpt_max_fields;

	ptrdiff_t payload_bytes = end - pivot;
	if (unlikely(buffer_bytes < sizeof(fpt_rw) + units2bytes(reserve_items)
			+ payload_bytes))
		return nullptr;

	fpt_rw *pt = (fpt_rw*) space;
	pt->end = (buffer_bytes - sizeof(fpt_rw)) / fpt_unit_size + 1;
	pt->pivot = reserve_items + 1;
	pt->head = pt->pivot - items;
	pt->tail = pt->pivot + (payload_bytes >> fpt_unit_shift);
	pt->junk = 0;

	memcpy(&pt->units[pt->head], begin, ro.total_bytes - fpt_unit_size);
	return pt;
}

size_t fpt_check_and_get_buffer_size(fpt_ro ro,
	unsigned more_items, unsigned more_payload, const char** error) {
	if (unlikely(error == nullptr))
		return ~(size_t)0;

	*error = fpt_check_ro(ro);
	if (unlikely(*error != nullptr))
		return 0;

	if (unlikely(more_items > fpt_max_fields)) {
		*error = "more_items > fpt_max_fields";
		return 0;
	}
	if (unlikely(more_payload > fpt_max_tuple_bytes)) {
		*error = "more_payload > fpt_max_tuple_bytes";
		return 0;
	}

	size_t items = ro.units[0].varlen.tuple_items & fpt_lt_mask;
	size_t payload_bytes = ro.total_bytes - units2bytes(items + 1);
	return fpt_space(items + more_items, payload_bytes + more_payload);
}

//----------------------------------------------------------------------

#include <stdlib.h>

// TODO: split out
fpt_rw* fpt_alloc(size_t items_limit, size_t data_bytes) {
	if (unlikely(items_limit > fpt_max_fields || data_bytes > fpt_max_tuple_bytes))
		return nullptr;

	size_t size = fpt_space(items_limit, data_bytes);
	void* buffer = malloc(size);
	if (unlikely(! buffer))
		return nullptr;

	fpt_rw* pt = fpt_init(buffer, size, items_limit);
	assert(pt != nullptr);

	return pt;
}

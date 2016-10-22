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

	if (unlikely(buffer_bytes > FPT_ALIGN_CEIL(sizeof(fpt_rw)
			+ fpt_max_tuple_bytes, CACHELINE_SIZE)))
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

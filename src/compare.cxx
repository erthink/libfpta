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

static __inline
int memcmp2sign(const void* a, const void* b, size_t bytes) {
	int compare = memcmp(a, b, bytes);
	if (compare == 0)
		return fpt_eq;
	return (compare < 0) ? fpt_lt : fpt_gt;
}

int fpt_cmp_96(fpt_ro ro, unsigned column, const uint8_t* value) {
	if (unlikely(value == nullptr))
		return fpt_ic;

	const fpt_field* pf = fpt_lookup_ro(ro, column, fpt_96);
	if (unlikely(pf == nullptr))
		return fpt_ic;

	return memcmp2sign(fpt_field_payload(pf)->fixed_opaque, value, 12);
}

int fpt_cmp_128(fpt_ro ro, unsigned column, const uint8_t* value) {
	if (unlikely(value == nullptr))
		return fpt_ic;

	const fpt_field* pf = fpt_lookup_ro(ro, column, fpt_128);
	if (unlikely(pf == nullptr))
		return fpt_ic;

	return memcmp2sign(fpt_field_payload(pf)->fixed_opaque, value, 16);
}

int fpt_cmp_160(fpt_ro ro, unsigned column, const uint8_t* value) {
	if (unlikely(value == nullptr))
		return fpt_ic;

	const fpt_field* pf = fpt_lookup_ro(ro, column, fpt_160);
	if (unlikely(pf == nullptr))
		return fpt_ic;

	return memcmp2sign(fpt_field_payload(pf)->fixed_opaque, value, 20);
}

int fpt_cmp_192(fpt_ro ro, unsigned column, const uint8_t* value) {
	if (unlikely(value == nullptr))
		return fpt_ic;

	const fpt_field* pf = fpt_lookup_ro(ro, column, fpt_192);
	if (unlikely(pf == nullptr))
		return fpt_ic;

	return memcmp2sign(fpt_field_payload(pf)->fixed_opaque, value, 24);
}

int fpt_cmp_256(fpt_ro ro, unsigned column, const uint8_t* value) {
	if (unlikely(value == nullptr))
		return fpt_ic;

	const fpt_field* pf = fpt_lookup_ro(ro, column, fpt_256);
	if (unlikely(pf == nullptr))
		return fpt_ic;

	return memcmp2sign(fpt_field_payload(pf)->fixed_opaque, value, 32);
}

//----------------------------------------------------------------------

int fpt_cmp_opaque(fpt_ro ro, unsigned column, const void* value, size_t bytes) {
	const fpt_field* pf = fpt_lookup_ro(ro, column, fpt_opaque);
	if (pf == nullptr)
		return bytes ? fpt_ic : fpt_eq;

	const struct iovec field = fpt_field_opaque(pf);
	if (field.iov_len != bytes)
		return (field.iov_len < bytes) ? fpt_lt : fpt_gt;

	return memcmp2sign(field.iov_base, value, bytes);
}

int fpt_cmp_opaque_iov(fpt_ro ro, unsigned column, const struct iovec value) {
	return fpt_cmp_opaque(ro, column, value.iov_base, value.iov_len);
}

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
int memcmp2bits(const void* a, const void* b, size_t bytes) {
	return fptu_cmp2bits(memcmp(a, b, bytes), 0);
}

int fptu_cmp_96(fptu_ro ro, unsigned column, const uint8_t* value) {
	if (unlikely(value == nullptr))
		return fptu_ic;

	const fptu_field* pf = fptu_lookup_ro(ro, column, fptu_96);
	if (unlikely(pf == nullptr))
		return fptu_ic;

	return memcmp2bits(fptu_field_payload(pf)->fixed_opaque, value, 12);
}

int fptu_cmp_128(fptu_ro ro, unsigned column, const uint8_t* value) {
	if (unlikely(value == nullptr))
		return fptu_ic;

	const fptu_field* pf = fptu_lookup_ro(ro, column, fptu_128);
	if (unlikely(pf == nullptr))
		return fptu_ic;

	return memcmp2bits(fptu_field_payload(pf)->fixed_opaque, value, 16);
}

int fptu_cmp_160(fptu_ro ro, unsigned column, const uint8_t* value) {
	if (unlikely(value == nullptr))
		return fptu_ic;

	const fptu_field* pf = fptu_lookup_ro(ro, column, fptu_160);
	if (unlikely(pf == nullptr))
		return fptu_ic;

	return memcmp2bits(fptu_field_payload(pf)->fixed_opaque, value, 20);
}

int fptu_cmp_192(fptu_ro ro, unsigned column, const uint8_t* value) {
	if (unlikely(value == nullptr))
		return fptu_ic;

	const fptu_field* pf = fptu_lookup_ro(ro, column, fptu_192);
	if (unlikely(pf == nullptr))
		return fptu_ic;

	return memcmp2bits(fptu_field_payload(pf)->fixed_opaque, value, 24);
}

int fptu_cmp_256(fptu_ro ro, unsigned column, const uint8_t* value) {
	if (unlikely(value == nullptr))
		return fptu_ic;

	const fptu_field* pf = fptu_lookup_ro(ro, column, fptu_256);
	if (unlikely(pf == nullptr))
		return fptu_ic;

	return memcmp2bits(fptu_field_payload(pf)->fixed_opaque, value, 32);
}

//----------------------------------------------------------------------

int fptu_cmp_opaque(fptu_ro ro, unsigned column, const void* value, size_t bytes) {
	const fptu_field* pf = fptu_lookup_ro(ro, column, fptu_opaque);
	if (pf == nullptr)
		return bytes ? fptu_ic : fptu_eq;

	const struct iovec field = fptu_field_opaque(pf);
	if (field.iov_len != bytes)
		return (field.iov_len < bytes) ? fptu_lt : fptu_gt;

	return memcmp2bits(field.iov_base, value, bytes);
}

int fptu_cmp_opaque_iov(fptu_ro ro, unsigned column, const struct iovec value) {
	return fptu_cmp_opaque(ro, column, value.iov_base, value.iov_len);
}

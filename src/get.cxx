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

int fpt_field_type(const fpt_field* pf) {
	if (unlikely(pf == nullptr))
		return fpt_null;

	return fpt_get_type(pf->ct);
}

int fpt_field_column(const fpt_field* pf) {
	if (unlikely(pf == nullptr || ct_is_dead(pf->ct)))
		return -1;

	return fpt_get_col(pf->ct);
}

//----------------------------------------------------------------------

uint16_t fpt_field_uint16(const fpt_field* pf) {
	if (unlikely(fpt_field_type(pf) != fpt_uint16))
		return 0;

	return pf->offset;
}

int32_t fpt_field_int32(const fpt_field* pf) {
	if (unlikely(fpt_field_type(pf) != fpt_int32))
		return 0;

	return fpt_field_payload(pf)->i32;
}

uint32_t fpt_field_uint32(const fpt_field* pf) {
	if (unlikely(fpt_field_type(pf) != fpt_uint32))
		return 0;

	return fpt_field_payload(pf)->u32;
}

int64_t fpt_field_int64(const fpt_field* pf) {
	if (unlikely(fpt_field_type(pf) != fpt_int64))
		return 0;

	return fpt_field_payload(pf)->i64;
}

uint64_t fpt_field_uint64(const fpt_field* pf) {
	if (unlikely(fpt_field_type(pf) != fpt_uint64))
		return 0;

	return fpt_field_payload(pf)->u64;
}

double fpt_field_fp64(const fpt_field* pf) {
	if (unlikely(fpt_field_type(pf) != fpt_fp64))
		return 0.0;

	return fpt_field_payload(pf)->fp64;
}

float fpt_field_fp32(const fpt_field* pf) {
	if (unlikely(fpt_field_type(pf) != fpt_fp32))
		return 0.0;

	return fpt_field_payload(pf)->fp32;
}

const char* fpt_field_cstr(const fpt_field* pf) {
	if (unlikely(fpt_field_type(pf) != fpt_string))
		return "";

	return fpt_field_payload(pf)->cstr;
}

const uint8_t* fpt_field_96(const fpt_field* pf) {
	if (unlikely(fpt_field_type(pf) != fpt_96))
		return nullptr;

	return fpt_field_payload(pf)->fixed_opaque;
}

const uint8_t* fpt_field_128(const fpt_field* pf) {
	if (unlikely(fpt_field_type(pf) != fpt_128))
		return nullptr;

	return fpt_field_payload(pf)->fixed_opaque;
}

const uint8_t* fpt_field_160(const fpt_field* pf) {
	if (unlikely(fpt_field_type(pf) != fpt_160))
		return nullptr;

	return fpt_field_payload(pf)->fixed_opaque;
}

const uint8_t* fpt_field_192(const fpt_field* pf) {
	if (unlikely(fpt_field_type(pf) != fpt_192))
		return nullptr;

	return fpt_field_payload(pf)->fixed_opaque;
}

const uint8_t* fpt_field_256(const fpt_field* pf) {
	if (unlikely(fpt_field_type(pf) != fpt_256))
		return nullptr;
	return fpt_field_payload(pf)->fixed_opaque;
}

struct iovec fpt_field_opaque(const fpt_field* pf) {
	iovec io;
	if (unlikely(fpt_field_type(pf) != fpt_opaque)) {
		io.iov_base = nullptr;
		io.iov_len = 0;
	} else {
		const fpt_payload* payload = fpt_field_payload(pf);
		io.iov_len = payload->other.varlen.opaque_bytes;
		io.iov_base = (void*) payload->other.data;
	}
	return io;
}

struct iovec fpt_field_as_iovec(const fpt_field* pf) {
	struct iovec opaque;
	const fpt_payload* payload;
	unsigned type = fpt_field_type(pf);

	switch(type) {
	default:
		if (likely(type < fpt_farray)) {
			assert(type < fpt_string);
			opaque.iov_len = fpt_internal_map_t2b[type];
			opaque.iov_base = (void*) fpt_field_payload(pf);
			break;
		}
		// TODO: array support
		payload = fpt_field_payload(pf);
		opaque.iov_base = (void*) payload->other.data;
		opaque.iov_len = units2bytes(payload->other.varlen.brutto);
		break;
	case fpt_null:
		opaque.iov_base = nullptr;
		opaque.iov_len = 0;
		break;
	case fpt_uint16:
		opaque.iov_base = (void*) &pf->offset;
		opaque.iov_len = 2;
		break;
	case fpt_opaque:
		payload = fpt_field_payload(pf);
		opaque.iov_len = payload->other.varlen.opaque_bytes;
		opaque.iov_base = (void*) payload->other.data;
		break;
	case fpt_string:
		payload = fpt_field_payload(pf);
		opaque.iov_len = strlen(payload->cstr) + 1;
		opaque.iov_base = (void*) payload->cstr;
		break;
	case fpt_nested:
		payload = fpt_field_payload(pf);
		opaque.iov_len = units2bytes(payload->other.varlen.brutto + (size_t) 1);
		opaque.iov_base = (void*) payload;
		break;
	}

	return opaque;
}

fpt_ro fpt_field_nested(const fpt_field* pf) {
	fpt_ro tuple;

	if (unlikely(fpt_field_type(pf) != fpt_nested)) {
		tuple.total_bytes = 0;
		tuple.units = nullptr;
		return tuple;
	}

	const fpt_payload* payload = fpt_field_payload(pf);
	tuple.total_bytes = units2bytes(payload->other.varlen.brutto + (size_t) 1);
	tuple.units = (const fpt_unit*) payload;
	return tuple;
}

//----------------------------------------------------------------------

uint16_t fpt_get_uint16(fpt_ro ro, unsigned column, int *error) {
	const fpt_field* pf = fpt_lookup_ro(ro, column, fpt_uint16);
	if (error)
		*error = pf ? fpt_ok : fpt_noent;
	return fpt_field_uint16(pf);
}

int32_t fpt_get_int32(fpt_ro ro, unsigned column, int *error) {
	const fpt_field* pf = fpt_lookup_ro(ro, column, fpt_int32);
	if (error)
		*error = pf ? fpt_ok : fpt_noent;
	return fpt_field_int32(pf);
}

uint32_t fpt_get_uint32(fpt_ro ro, unsigned column, int *error) {
	const fpt_field* pf = fpt_lookup_ro(ro, column, fpt_uint32);
	if (error)
		*error = pf ? fpt_ok : fpt_noent;
	return fpt_field_uint32(pf);
}

int64_t fpt_get_int64(fpt_ro ro, unsigned column, int *error) {
	const fpt_field* pf = fpt_lookup_ro(ro, column, fpt_int64);
	if (error)
		*error = pf ? fpt_ok : fpt_noent;
	return fpt_field_int64(pf);
}

uint64_t fpt_get_uint64(fpt_ro ro, unsigned column, int *error) {
	const fpt_field* pf = fpt_lookup_ro(ro, column, fpt_uint64);
	if (error)
		*error = pf ? fpt_ok : fpt_noent;
	return fpt_field_uint64(pf);
}

double fpt_get_fp64(fpt_ro ro, unsigned column, int *error) {
	const fpt_field* pf = fpt_lookup_ro(ro, column, fpt_fp64);
	if (error)
		*error = pf ? fpt_ok : fpt_noent;
	return fpt_field_fp64(pf);
}

float fpt_get_fp32(fpt_ro ro, unsigned column, int *error) {
	const fpt_field* pf = fpt_lookup_ro(ro, column, fpt_fp32);
	if (error)
		*error = pf ? fpt_ok : fpt_noent;
	return fpt_field_fp32(pf);
}

const uint8_t* fpt_get_96(fpt_ro ro, unsigned column, int *error) {
	const fpt_field* pf = fpt_lookup_ro(ro, column, fpt_96);
	if (error)
		*error = pf ? fpt_ok : fpt_noent;
	return fpt_field_96(pf);
}

const uint8_t* fpt_get_128(fpt_ro ro, unsigned column, int *error) {
	const fpt_field* pf = fpt_lookup_ro(ro, column, fpt_128);
	if (error)
		*error = pf ? fpt_ok : fpt_noent;
	return fpt_field_128(pf);
}

const uint8_t* fpt_get_160(fpt_ro ro, unsigned column, int *error) {
	const fpt_field* pf = fpt_lookup_ro(ro, column, fpt_160);
	if (error)
		*error = pf ? fpt_ok : fpt_noent;
	return fpt_field_160(pf);
}

const uint8_t* fpt_get_192(fpt_ro ro, unsigned column, int *error) {
	const fpt_field* pf = fpt_lookup_ro(ro, column, fpt_192);
	if (error)
		*error = pf ? fpt_ok : fpt_noent;
	return fpt_field_192(pf);
}

const uint8_t* fpt_get_256(fpt_ro ro, unsigned column, int *error) {
	const fpt_field* pf = fpt_lookup_ro(ro, column, fpt_256);
	if (error)
		*error = pf ? fpt_ok : fpt_noent;
	return fpt_field_256(pf);
}

const char* fpt_get_cstr(fpt_ro ro, unsigned column, int *error) {
	const fpt_field* pf = fpt_lookup_ro(ro, column, fpt_string);
	if (error)
		*error = pf ? fpt_ok : fpt_noent;
	return fpt_field_cstr(pf);
}

struct iovec fpt_get_opaque(fpt_ro ro, unsigned column, int *error) {
	const fpt_field* pf = fpt_lookup_ro(ro, column, fpt_opaque);
	if (error)
		*error = pf ? fpt_ok : fpt_noent;
	return fpt_field_opaque(pf);
}

fpt_ro fpt_get_nested(fpt_ro ro, unsigned column, int *error) {
	const fpt_field* pf = fpt_lookup_ro(ro, column, fpt_nested);
	if (error)
		*error = pf ? fpt_ok : fpt_noent;
	return fpt_field_nested(pf);
}

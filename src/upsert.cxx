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
fpt_field* fpt_find_dead(fpt_rw* pt, unsigned units) {
	fpt_field *end = &pt->units[pt->pivot].field;
	for (fpt_field *pf = &pt->units[pt->head].field; pf < end; ++pf) {
		if (ct_is_dead(pf->ct) && fpt_field_units(pf) == units)
			return pf;
	}
	return nullptr;
}

static __hot
fpt_field* fpt_append(fpt_rw* pt, unsigned ct, unsigned units) {
	fpt_field* pf = fpt_find_dead(pt, units);
	if (pf) {
		pf->ct = ct;
		assert(pt->junk > 1 + units);
		pt->junk -= 1 + units;
		return pf;
	}

	if (unlikely(pt->head < 2 || pt->tail + units > pt->end))
		return nullptr;

	pt->head -= 1;
	pf = &pt->units[pt->head].field;
	if (likely(units)) {
		size_t offset = &pt->units[pt->tail].data - pf->body;
		if (unlikely(offset > fpt_limit))
			return nullptr;
		pf->offset = offset;
		pt->tail += units;
	} else {
		pf->offset = -1;
	}

	pf->ct = ct;
	return pf;
}

static __hot
fpt_field* fpt_emplace(fpt_rw* pt, unsigned ct, unsigned units) {
	fpt_field* pf = fpt_lookup_ct(pt, ct);
	if (pf) {
		unsigned avail = fpt_field_units(pf);
		if (likely(avail == units))
			return pf;

		assert(pf->ct == ct);
		unsigned save_head = pt->head;
		unsigned save_tail = pt->tail;
		unsigned save_junk = pt->junk;

		fpt_erase_field(pt, pf);
		fpt_field* fresh = fpt_append(pt, ct, units);
		if (unlikely(fresh == nullptr)) {
			// undo erase
			// TODO: unit test for this case
			/* yes this is a badly hack,
			 * but on the other hand,
			 * it is reasonably and easy decision. */
			pf->ct = ct;
			assert(pt->head >= save_head);
			assert(pt->tail <= save_tail);
			assert(pt->junk >= save_junk);
			pt->head = save_head;
			pt->tail = save_tail;
			pt->junk = save_junk;
		}

		return fresh;
	}

	return fpt_append(pt, ct, units);
}

struct fpt_takeover_result {
	fpt_field* pf;
	int error;
};

static __hot
fpt_takeover_result fpt_takeover(fpt_rw* pt, unsigned ct, unsigned units) {
	fpt_takeover_result result;

	result.pf = fpt_lookup_ct(pt, ct);
	if (unlikely(result.pf == nullptr)) {
		result.error = fpt_noent;
		return result;
	}

	unsigned avail = fpt_field_units(result.pf);
	if (likely(avail == units)) {
		result.error = fpt_ok;
		return result;
	}

	fpt_erase_field(pt, result.pf);
	result.pf = fpt_append(pt, ct, units);
	result.error = likely(result.pf != nullptr) ? fpt_ok : fpt_enospc;
	return result;
}

static __inline
void fpt_cstrcpy(fpt_field* pf, size_t units, const char* value, size_t bytes) {
	uint32_t* payload = (uint32_t*) fpt_field_payload(pf);
	payload[units - 1] = 0; // clean last unit
	memcpy(payload, value, bytes);
}

//======================================================================

int fpt_upsert_null(fpt_rw* pt, unsigned col) {
	if (unlikely(col > fpt_max_cols))
		return fpt_einval;

	fpt_field* pf = fpt_emplace(pt, fpt_pack_coltype(col, fpt_null), 0);
	if (unlikely(pf == nullptr))
		return fpt_enospc;

	return fpt_ok;
}

int fpt_upsert_uint16(fpt_rw* pt, unsigned col, unsigned value) {
	if (unlikely(col > fpt_max_cols))
		return fpt_einval;

	fpt_field* pf = fpt_emplace(pt, fpt_pack_coltype(col, fpt_uint16), 0);
	if (unlikely(pf == nullptr))
		return fpt_enospc;

	pf->offset = value;
	return fpt_ok;
}

//----------------------------------------------------------------------

static
int fpt_upsert_32(fpt_rw* pt, unsigned ct, uint32_t value) {
	assert(ct_match_fixedsize(ct, 1));
	assert(! ct_is_dead(ct));

	fpt_field* pf = fpt_emplace(pt, ct, 1);
	if (unlikely(pf == nullptr))
		return fpt_enospc;

	fpt_field_payload(pf)->u32 = value;
	return fpt_ok;
}

int fpt_upsert_int32(fpt_rw* pt, unsigned col, int32_t value) {
	if (unlikely(col > fpt_max_cols))
		return fpt_einval;
	return fpt_upsert_32(pt, fpt_pack_coltype(col, fpt_int32), value);
}

int fpt_upsert_uint32(fpt_rw* pt, unsigned col, uint32_t value) {
	if (unlikely(col > fpt_max_cols))
		return fpt_einval;
	return fpt_upsert_32(pt, fpt_pack_coltype(col, fpt_uint32), value);
}

//----------------------------------------------------------------------

static
int fpt_upsert_64(fpt_rw* pt, unsigned ct, uint64_t value) {
	assert(ct_match_fixedsize(ct, 2));
	assert(! ct_is_dead(ct));

	fpt_field* pf = fpt_emplace(pt, ct, 2);
	if (unlikely(pf == nullptr))
		return fpt_enospc;

	fpt_field_payload(pf)->u64 = value;
	return fpt_ok;
}

int fpt_upsert_int64(fpt_rw* pt, unsigned col, int64_t value) {
	if (unlikely(col > fpt_max_cols))
		return fpt_einval;
	return fpt_upsert_64(pt, fpt_pack_coltype(col, fpt_int64), value);
}

int fpt_upsert_uint64(fpt_rw* pt, unsigned col, uint64_t value) {
	if (unlikely(col > fpt_max_cols))
		return fpt_einval;
	return fpt_upsert_64(pt, fpt_pack_coltype(col, fpt_uint64), value);
}

//----------------------------------------------------------------------

int fpt_upsert_fp32(fpt_rw* pt, unsigned col, float value) {
	if (unlikely(col > fpt_max_cols))
		return fpt_einval;

	union {
		uint32_t u32;
		float fp32;
	} v;

	v.fp32 = value;
	return fpt_upsert_32(pt, fpt_pack_coltype(col, fpt_fp32), v.u32);
}

int fpt_upsert_fp64(fpt_rw* pt, unsigned col, double value) {
	if (unlikely(col > fpt_max_cols))
		return fpt_einval;

	union {
		uint64_t u64;
		double fp64;
	} v;

	v.fp64 = value;
	return fpt_upsert_64(pt, fpt_pack_coltype(col, fpt_fp64), v.u64);
}

//----------------------------------------------------------------------

int fpt_upsert_96(fpt_rw* pt, unsigned col, const void* data) {
	if (unlikely(col > fpt_max_cols))
		return fpt_einval;

	fpt_field* pf = fpt_emplace(pt, fpt_pack_coltype(col, fpt_96), 3);
	if (unlikely(pf == nullptr))
		return fpt_enospc;

	memcpy(fpt_field_payload(pf), data, 12);
	return fpt_ok;
}

int fpt_upsert_128(fpt_rw* pt, unsigned col, const void* data) {
	if (unlikely(col > fpt_max_cols))
		return fpt_einval;

	fpt_field* pf = fpt_emplace(pt, fpt_pack_coltype(col, fpt_128), 4);
	if (unlikely(pf == nullptr))
		return fpt_enospc;

	memcpy(fpt_field_payload(pf), data, 16);
	return fpt_ok;
}

int fpt_upsert_160(fpt_rw* pt, unsigned col, const void* data) {
	if (unlikely(col > fpt_max_cols))
		return fpt_einval;

	fpt_field* pf = fpt_emplace(pt, fpt_pack_coltype(col, fpt_160), 5);
	if (unlikely(pf == nullptr))
		return fpt_enospc;

	memcpy(fpt_field_payload(pf), data, 20);
	return fpt_ok;
}

int fpt_upsert_192(fpt_rw* pt, unsigned col, const void* data) {
	if (unlikely(col > fpt_max_cols))
		return fpt_einval;

	fpt_field* pf = fpt_emplace(pt, fpt_pack_coltype(col, fpt_192), 6);
	if (unlikely(pf == nullptr))
		return fpt_enospc;

	memcpy(fpt_field_payload(pf), data, 24);
	return fpt_ok;
}

int fpt_upsert_256(fpt_rw* pt, unsigned col, const void* data) {
	if (unlikely(col > fpt_max_cols))
		return fpt_einval;

	fpt_field* pf = fpt_emplace(pt, fpt_pack_coltype(col, fpt_256), 8);
	if (unlikely(pf == nullptr))
		return fpt_enospc;

	memcpy(fpt_field_payload(pf), data, 32);
	return fpt_ok;
}

//----------------------------------------------------------------------

int fpt_upsert_cstr(fpt_rw* pt, unsigned col, const char* value) {
	if (unlikely(col > fpt_max_cols))
		return fpt_einval;

	if (unlikely(value == nullptr))
		value = "";

	size_t bytes = strlen(value) + 1;
	if (unlikely(bytes > fpt_max_field_bytes))
		return fpt_einval;

	size_t units = bytes2units(bytes);
	fpt_field* pf = fpt_emplace(pt, fpt_pack_coltype(col, fpt_string), units);
	if (unlikely(pf == nullptr))
		return fpt_enospc;

	fpt_cstrcpy(pf, units, value, bytes);
	return fpt_ok;
}

int fpt_upsert_opaque(fpt_rw* pt, unsigned col, const void* value, size_t bytes) {
	if (unlikely(col > fpt_max_cols))
		return fpt_einval;

	if (unlikely(bytes > fpt_max_opaque_bytes))
		return fpt_einval;

	size_t units = bytes2units(bytes) + 1;
	fpt_field* pf = fpt_emplace(pt, fpt_pack_coltype(col, fpt_opaque), units);
	if (unlikely(pf == nullptr))
		return fpt_enospc;

	fpt_payload* payload = fpt_field_payload(pf);
	payload->other.varlen.brutto = units - 1;
	payload->other.varlen.opaque_bytes = bytes;

	memcpy(payload->other.data, value, bytes);
	return fpt_ok;
}

int fpt_upsert_opaque_iov(fpt_rw* pt, unsigned column, const struct iovec value) {
	return fpt_upsert_opaque(pt, column, value.iov_base, value.iov_len);
}

//----------------------------------------------------------------------

//int fpt_upsert_array_int32(fpt_rw* pt, unsigned ct, size_t array_length, const int32_t* array_data);
//int fpt_upsert_array_uint32(fpt_rw* pt, unsigned ct, size_t array_length, const uint32_t* array_data);
//int fpt_upsert_array_int64(fpt_rw* pt, unsigned ct, size_t array_length, const int64_t* array_data);
//int fpt_upsert_array_uint64(fpt_rw* pt, unsigned ct, size_t array_length, const uint64_t* array_data);
//int fpt_upsert_array_str(fpt_rw* pt, unsigned ct, size_t array_length, const char* array_data[]);

//======================================================================

int fpt_update_uint16(fpt_rw* pt, unsigned col, unsigned value) {
	if (unlikely(col > fpt_max_cols))
		return fpt_einval;

	fpt_takeover_result result = fpt_takeover(pt, fpt_pack_coltype(col, fpt_uint16), 0);
	if (likely(result.error == fpt_ok)) {
		assert(result.pf != nullptr);
		result.pf->offset = value;
	}
	return result.error;
}

//----------------------------------------------------------------------

static
int fpt_update_32(fpt_rw* pt, unsigned ct, uint32_t value) {
	assert(ct_match_fixedsize(ct, 1));
	assert(! ct_is_dead(ct));

	fpt_takeover_result result = fpt_takeover(pt, ct, 1);
	if (likely(result.error == fpt_ok)) {
		assert(result.pf != nullptr);
		fpt_field_payload(result.pf)->u32 = value;
	}
	return result.error;
}

int fpt_update_int32(fpt_rw* pt, unsigned col, int32_t value) {
	if (unlikely(col > fpt_max_cols))
		return fpt_einval;
	return fpt_update_32(pt, fpt_pack_coltype(col, fpt_int32), value);
}

int fpt_update_uint32(fpt_rw* pt, unsigned col, uint32_t value) {
	if (unlikely(col > fpt_max_cols))
		return fpt_einval;
	return fpt_update_32(pt, fpt_pack_coltype(col, fpt_uint32), value);
}

//----------------------------------------------------------------------

static
int fpt_update_64(fpt_rw* pt, unsigned ct, uint64_t value) {
	assert(ct_match_fixedsize(ct, 2));
	assert(! ct_is_dead(ct));

	fpt_takeover_result result = fpt_takeover(pt, ct, 2);
	if (likely(result.error == fpt_ok)) {
		assert(result.pf != nullptr);
		fpt_field_payload(result.pf)->u64 = value;
	}
	return result.error;
}

int fpt_update_int64(fpt_rw* pt, unsigned col, int64_t value) {
	if (unlikely(col > fpt_max_cols))
		return fpt_einval;
	return fpt_update_64(pt, fpt_pack_coltype(col, fpt_int64), value);
}

int fpt_update_uint64(fpt_rw* pt, unsigned col, uint64_t value) {
	if (unlikely(col > fpt_max_cols))
		return fpt_einval;
	return fpt_update_64(pt, fpt_pack_coltype(col, fpt_uint64), value);
}

//----------------------------------------------------------------------

int fpt_update_fp32(fpt_rw* pt, unsigned col, float value) {
	if (unlikely(col > fpt_max_cols))
		return fpt_einval;

	union {
		uint32_t u32;
		float fp32;
	} v;

	v.fp32 = value;
	return fpt_update_32(pt, fpt_pack_coltype(col, fpt_fp32), v.u32);
}

int fpt_update_fp64(fpt_rw* pt, unsigned col, double value) {
	if (unlikely(col > fpt_max_cols))
		return fpt_einval;

	union {
		uint64_t u64;
		double fp64;
	} v;

	v.fp64 = value;
	return fpt_update_64(pt, fpt_pack_coltype(col, fpt_fp64), v.u64);
}

//----------------------------------------------------------------------

int fpt_update_96(fpt_rw* pt, unsigned col, const void* data) {
	if (unlikely(col > fpt_max_cols))
		return fpt_einval;

	fpt_takeover_result result = fpt_takeover(pt, fpt_pack_coltype(col, fpt_96), 3);
	if (likely(result.error == fpt_ok)) {
		assert(result.pf != nullptr);
		memcpy(fpt_field_payload(result.pf), data, 12);
	}
	return result.error;
}

int fpt_update_128(fpt_rw* pt, unsigned col, const void* data) {
	if (unlikely(col > fpt_max_cols))
		return fpt_einval;

	fpt_takeover_result result = fpt_takeover(pt, fpt_pack_coltype(col, fpt_128), 4);
	if (likely(result.error == fpt_ok)) {
		assert(result.pf != nullptr);
		memcpy(fpt_field_payload(result.pf), data, 16);
	}
	return result.error;
}

int fpt_update_160(fpt_rw* pt, unsigned col, const void* data) {
	if (unlikely(col > fpt_max_cols))
		return fpt_einval;

	fpt_takeover_result result = fpt_takeover(pt, fpt_pack_coltype(col, fpt_160), 5);
	if (likely(result.error == fpt_ok)) {
		assert(result.pf != nullptr);
		memcpy(fpt_field_payload(result.pf), data, 20);
	}
	return result.error;
}

int fpt_update_192(fpt_rw* pt, unsigned col, const void* data) {
	if (unlikely(col > fpt_max_cols))
		return fpt_einval;

	fpt_takeover_result result = fpt_takeover(pt, fpt_pack_coltype(col, fpt_192), 6);
	if (likely(result.error == fpt_ok)) {
		assert(result.pf != nullptr);
		memcpy(fpt_field_payload(result.pf), data, 24);
	}
	return result.error;
}

int fpt_update_256(fpt_rw* pt, unsigned col, const void* data) {
	if (unlikely(col > fpt_max_cols))
		return fpt_einval;

	fpt_takeover_result result = fpt_takeover(pt, fpt_pack_coltype(col, fpt_256), 8);
	if (likely(result.error == fpt_ok)) {
		assert(result.pf != nullptr);
		memcpy(fpt_field_payload(result.pf), data, 32);
	}
	return result.error;
}

//----------------------------------------------------------------------

int fpt_update_cstr(fpt_rw* pt, unsigned col, const char* value) {
	if (unlikely(col > fpt_max_cols))
		return fpt_einval;

	if (unlikely(value == nullptr))
		value = "";

	size_t bytes = strlen(value) + 1;
	if (unlikely(bytes > fpt_max_field_bytes))
		return fpt_einval;

	size_t units = bytes2units(bytes);
	fpt_takeover_result result = fpt_takeover(pt, fpt_pack_coltype(col, fpt_string), units);
	if (likely(result.error == fpt_ok)) {
		assert(result.pf != nullptr);
		fpt_cstrcpy(result.pf, units, value, bytes);
	}
	return result.error;
}

int fpt_update_opaque(fpt_rw* pt, unsigned col, const void* value, size_t bytes) {
	if (unlikely(col > fpt_max_cols))
		return fpt_einval;

	if (unlikely(bytes > fpt_max_opaque_bytes))
		return fpt_einval;

	size_t units = bytes2units(bytes) + 1;
	fpt_takeover_result result = fpt_takeover(pt, fpt_pack_coltype(col, fpt_opaque), units);
	if (likely(result.error == fpt_ok)) {
		assert(result.pf != nullptr);
		fpt_payload* payload = fpt_field_payload(result.pf);
		payload->other.varlen.brutto = units - 1;
		payload->other.varlen.opaque_bytes = bytes;
		memcpy(payload->other.data, value, bytes);
	}
	return result.error;
}

int fpt_update_opaque_iov(fpt_rw* pt, unsigned column, const struct iovec value) {
	return fpt_update_opaque(pt, column, value.iov_base, value.iov_len);
}

//======================================================================

int fpt_insert_uint16(fpt_rw* pt, unsigned col, unsigned value) {
	if (unlikely(col > fpt_max_cols))
		return fpt_einval;

	fpt_field* pf = fpt_append(pt, fpt_pack_coltype(col, fpt_uint16), 0);
	if (unlikely(pf == nullptr))
		return fpt_enospc;

	pf->offset = value;
	return fpt_ok;
}

//----------------------------------------------------------------------

static int fpt_insert_32(fpt_rw* pt, unsigned ct, uint32_t v) {
	assert(ct_match_fixedsize(ct, 1));
	assert(! ct_is_dead(ct));

	fpt_field* pf = fpt_append(pt, ct, 1);
	if (unlikely(pf == nullptr))
		return fpt_enospc;

	fpt_field_payload(pf)->u32 = v;
	return fpt_ok;
}

int fpt_insert_int32(fpt_rw* pt, unsigned col, int32_t value) {
	if (unlikely(col > fpt_max_cols))
		return fpt_einval;
	return fpt_insert_32(pt, fpt_pack_coltype(col, fpt_int32), value);
}

int fpt_insert_uint32(fpt_rw* pt, unsigned col, uint32_t value) {
	if (unlikely(col > fpt_max_cols))
		return fpt_einval;
	return fpt_insert_32(pt, fpt_pack_coltype(col, fpt_uint32), value);
}

//----------------------------------------------------------------------

static int fpt_insert_64(fpt_rw* pt, unsigned ct, uint64_t v) {
	assert(ct_match_fixedsize(ct, 2));
	assert(! ct_is_dead(ct));

	fpt_field* pf = fpt_append(pt, ct, 2);
	if (unlikely(pf == nullptr))
		return fpt_enospc;

	fpt_field_payload(pf)->u64 = v;
	return fpt_ok;
}

int fpt_insert_int64(fpt_rw* pt, unsigned col, int64_t value) {
	if (unlikely(col > fpt_max_cols))
		return fpt_einval;
	return fpt_insert_64(pt, fpt_pack_coltype(col, fpt_int64), value);
}

int fpt_insert_uint64(fpt_rw* pt, unsigned col, uint64_t value) {
	if (unlikely(col > fpt_max_cols))
		return fpt_einval;
	return fpt_insert_64(pt, fpt_pack_coltype(col, fpt_uint64), value);
}

//----------------------------------------------------------------------

int fpt_insert_fp32(fpt_rw* pt, unsigned col, float value) {
	if (unlikely(col > fpt_max_cols))
		return fpt_einval;

	union {
		uint32_t u32;
		float fp32;
	} v;

	v.fp32 = value;
	return fpt_insert_32(pt, fpt_pack_coltype(col, fpt_fp32), v.u32);
}

int fpt_insert_fp64(fpt_rw* pt, unsigned col, double value) {
	if (unlikely(col > fpt_max_cols))
		return fpt_einval;

	union {
		uint64_t u64;
		double fp64;
	} v;

	v.fp64 = value;
	return fpt_insert_64(pt, fpt_pack_coltype(col, fpt_fp64), v.u64);
}

//----------------------------------------------------------------------

int fpt_insert_96(fpt_rw* pt, unsigned col, const void* data) {
	if (unlikely(col > fpt_max_cols))
		return fpt_einval;

	fpt_field* pf = fpt_append(pt, fpt_pack_coltype(col, fpt_96), 3);
	if (unlikely(pf == nullptr))
		return fpt_enospc;

	memcpy(fpt_field_payload(pf), data, 12);
	return fpt_ok;
}

int fpt_insert_128(fpt_rw* pt, unsigned col, const void* data) {
	if (unlikely(col > fpt_max_cols))
		return fpt_einval;

	fpt_field* pf = fpt_append(pt, fpt_pack_coltype(col, fpt_128), 4);
	if (unlikely(pf == nullptr))
		return fpt_enospc;

	memcpy(fpt_field_payload(pf), data, 16);
	return fpt_ok;
}

int fpt_insert_160(fpt_rw* pt, unsigned col, const void* data) {
	if (unlikely(col > fpt_max_cols))
		return fpt_einval;

	fpt_field* pf = fpt_append(pt, fpt_pack_coltype(col, fpt_160), 5);
	if (unlikely(pf == nullptr))
		return fpt_enospc;

	memcpy(fpt_field_payload(pf), data, 20);
	return fpt_ok;
}

int fpt_insert_192(fpt_rw* pt, unsigned col, const void* data) {
	if (unlikely(col > fpt_max_cols))
		return fpt_einval;

	fpt_field* pf = fpt_append(pt, fpt_pack_coltype(col, fpt_192), 6);
	if (unlikely(pf == nullptr))
		return fpt_enospc;

	memcpy(fpt_field_payload(pf), data, 24);
	return fpt_ok;
}

int fpt_insert_256(fpt_rw* pt, unsigned col, const void* data) {
	if (unlikely(col > fpt_max_cols))
		return fpt_einval;

	fpt_field* pf = fpt_append(pt, fpt_pack_coltype(col, fpt_256), 8);
	if (unlikely(pf == nullptr))
		return fpt_enospc;

	memcpy(fpt_field_payload(pf), data, 32);
	return fpt_ok;
}

//----------------------------------------------------------------------

int fpt_insert_cstr(fpt_rw* pt, unsigned col, const char* value) {
	if (unlikely(col > fpt_max_cols))
		return fpt_einval;

	if (unlikely(value == nullptr))
		value = "";

	size_t bytes = strlen(value) + 1;
	if (unlikely(bytes > fpt_max_field_bytes))
		return fpt_einval;

	size_t units = bytes2units(bytes);
	fpt_field* pf = fpt_append(pt, fpt_pack_coltype(col, fpt_string), units);
	if (unlikely(pf == nullptr))
		return fpt_enospc;

	fpt_cstrcpy(pf, units, value, bytes);
	return fpt_ok;
}

int fpt_insert_opaque(fpt_rw* pt, unsigned col, const void* value, size_t bytes) {
	if (unlikely(col > fpt_max_cols))
		return fpt_einval;

	if (unlikely(bytes > fpt_max_opaque_bytes))
		return fpt_einval;

	size_t units = bytes2units(bytes) + 1;
	fpt_field* pf = fpt_append(pt, fpt_pack_coltype(col, fpt_opaque), units);
	if (unlikely(pf == nullptr))
		return fpt_enospc;

	fpt_payload* payload = fpt_field_payload(pf);
	payload->other.varlen.brutto = units - 1;
	payload->other.varlen.opaque_bytes = bytes;

	memcpy(payload->other.data, value, bytes);
	return fpt_ok;
}

int fpt_insert_opaque_iov(fpt_rw* pt, unsigned column, const struct iovec value) {
	return fpt_insert_opaque(pt, column, value.iov_base, value.iov_len);
}

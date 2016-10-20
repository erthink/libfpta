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

#include "fast_positive/tuples.h"
#include "internal.h"

#include <string.h>

static fpt_field* fpt_find_dead(fpt_rw* pt, unsigned units) {
    fpt_field *end = &pt->units[pt->pivot].field;
    for (fpt_field *pf = &pt->units[pt->head].field; pf < end; ++pf) {
        if (ct_is_dead(pf->ct) && fpt_field_units(pf) == units)
            return pf;
    }
    return nullptr;
}

static fpt_field* fpt_emplace(fpt_rw* pt, unsigned ct, unsigned units) {
    fpt_field* pf = fpt_lookup_ct(pt, ct);
    if (pf) {
        unsigned avail = fpt_field_units(pf);
        if (likely(avail == units))
            return pf;
        fpt_erase_field(pt, pf);
    }

    pf = fpt_find_dead(pt, units);
    if (pf) {
        pf->ct = ct;
        assert(pt->junk > 1 + units);
        pt->junk -= 1 + units;
        return pf;
    }

    if (pt->head < 2 || pt->end - pt->tail < units)
        return nullptr;

    pt->head -= 1;
    pf = &pt->units[pt->head].field;
    if (units) {
        pf->offset = &pt->units[pt->tail].data - pf->body;
        pt->tail += units;
    } else {
        pf->offset = 0xffff;
    }

    pf->ct = ct;
    return pf;
}

//----------------------------------------------------------------------

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

static int fpt_upsert_32(fpt_rw* pt, unsigned ct, uint32_t v) {
    assert(ct_match_fixedsize(ct, 1));
    assert(! ct_is_dead(ct));

    fpt_field* pf = fpt_emplace(pt, ct, 1);
    if (unlikely(pf == nullptr))
        return fpt_enospc;

    fpt_field_payload(pf)->u32 = v;
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

static int fpt_upsert_64(fpt_rw* pt, unsigned ct, uint64_t v) {
    assert(ct_match_fixedsize(ct, 2));
    assert(! ct_is_dead(ct));

    fpt_field* pf = fpt_emplace(pt, ct, 2);
    if (unlikely(pf == nullptr))
        return fpt_enospc;

    fpt_field_payload(pf)->u64 = v;
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

    size_t units = (bytes + 3) >> 2;
    fpt_field* pf = fpt_emplace(pt, fpt_pack_coltype(col, fpt_string), units);
    if (unlikely(pf == nullptr))
        return fpt_enospc;

    memcpy(fpt_field_payload(pf), value, bytes);
    return fpt_ok;
}

int fpt_upsert_opaque(fpt_rw* pt, unsigned col, const void* value, size_t bytes) {
    if (unlikely(col > fpt_max_cols))
        return fpt_einval;

    if (unlikely(bytes > fpt_max_field_bytes - 4))
        return fpt_einval;

    size_t units = (bytes + 7) >> 2;
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

//int fpt_upsert_array(fpt_rw* pt, unsigned ct, size_t array_length, void* array_data);
//int fpt_upsert_array_int32(fpt_rw* pt, unsigned ct, size_t array_length, const int32_t* array_data);
//int fpt_upsert_array_uint32(fpt_rw* pt, unsigned ct, size_t array_length, const uint32_t* array_data);
//int fpt_upsert_array_int64(fpt_rw* pt, unsigned ct, size_t array_length, const int64_t* array_data);
//int fpt_upsert_array_uint64(fpt_rw* pt, unsigned ct, size_t array_length, const uint64_t* array_data);
//int fpt_upsert_array_str(fpt_rw* pt, unsigned ct, size_t array_length, const char* array_data[]);

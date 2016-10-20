/*
 * Copyright 2016 libfptu AUTHORS: please see AUTHORS file.
 *
 * This file is part of libfptu, aka "Positive Tuples".
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

static __inline
unsigned fpt_get_col(uint16_t packed) {
    return packed >> fpt_co_shift;
}

static __inline
fpt_type fpt_get_type(unsigned packed) {
    return (fpt_type) (packed & fpt_ty_mask);
}

static __inline
unsigned fpt_pack_coltype(unsigned column, unsigned type) {
    assert(type <= fpt_ty_mask);
    assert(column <= fpt_max_cols);
    return type + (column << fpt_co_shift);
}

static __inline
bool fpt_ct_match(const fpt_field* pf, unsigned column, int type_or_filter) {
    if (fpt_get_col(pf->ct) != column)
        return false;
    if (type_or_filter & fpt_filter)
        return (type_or_filter & (1 << fpt_get_type(pf->ct))) ? true : false;
    return type_or_filter == fpt_get_type(pf->ct);
}

typedef union fpt_payload {
    uint32_t u32;
    int32_t  i32;
    uint64_t u64;
    int64_t  i64;
    float    fp32;
    double   fp64;
    char     cstr[4];
    uint8_t  fixed_opaque[8];
    struct {
        fpt_varlen varlen;
        uint32_t data[1];
    } other;
} fpt_payload;

static __inline
fpt_payload* fpt_field_payload(fpt_field* pf) {
    return (fpt_payload*) &pf->body[pf->offset];
}

#ifdef __cplusplus
static __inline
const fpt_payload* fpt_field_payload(const fpt_field* pf) {
    return (const fpt_payload*) &pf->body[pf->offset];
}
#endif /* __cplusplus */

extern const uint8_t fpt_internal_map_t2b[];
extern const uint8_t fpt_internal_map_t2u[];

static __inline
bool ct_is_fixedsize(unsigned ct) {
    return fpt_get_type(ct) < fpt_string;
}

static __inline
bool ct_is_dead(unsigned ct) {
    return ct >= (fpt_co_dead << fpt_co_shift);
}

static __inline
size_t ct_elem_size(unsigned ct) {
    unsigned type = fpt_get_type(ct);
    if (likely(ct_is_fixedsize(type)))
        return fpt_internal_map_t2b[type];

    /* fpt_opaque, fpt_string or fpt_farray.
     * at least 4 bytes for length or '\0'. */
    return 4;
}

static __inline
bool ct_match_fixedsize(unsigned ct, unsigned units) {
    return ct_is_fixedsize(ct)
        && units == fpt_internal_map_t2u[fpt_get_type(ct)];
}

size_t fpt_field_units(const fpt_field* pf);

static __inline
const void* fpt_ro_detent(fpt_ro ro) {
    return (char *) ro.sys.iov_base + ro.sys.iov_len;
}

static __inline
const void* fpt_detent(const fpt_rw* rw) {
    return &rw->units[rw->end];
}

fpt_field* fpt_lookup_ct(fpt_rw* pt, unsigned ct);

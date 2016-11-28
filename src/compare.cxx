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

#include "fast_positive/tuples_internal.h"

static __inline fptu_cmp cmpbin(const void *a, const void *b, size_t bytes)
{
    return fptu_int2cmp(memcmp(a, b, bytes));
}

fptu_cmp __hot fptu_cmp_binary(const void *left_data, size_t left_len,
                               const void *right_data, size_t right_len)
{
    int diff = memcmp(left_data, right_data, std::min(left_len, right_len));
    if (diff == 0)
        diff = fptu_diff2int(left_len, right_len);
    return fptu_int2cmp(diff);
}

//----------------------------------------------------------------------------

fptu_cmp fptu_cmp_96(fptu_ro ro, unsigned column, const uint8_t *value)
{
    if (unlikely(value == nullptr))
        return fptu_ic;

    const fptu_field *pf = fptu_lookup_ro(ro, column, fptu_96);
    if (unlikely(pf == nullptr))
        return fptu_ic;

    return cmpbin(fptu_field_payload(pf)->fixbin, value, 12);
}

fptu_cmp fptu_cmp_128(fptu_ro ro, unsigned column, const uint8_t *value)
{
    if (unlikely(value == nullptr))
        return fptu_ic;

    const fptu_field *pf = fptu_lookup_ro(ro, column, fptu_128);
    if (unlikely(pf == nullptr))
        return fptu_ic;

    return cmpbin(fptu_field_payload(pf)->fixbin, value, 16);
}

fptu_cmp fptu_cmp_160(fptu_ro ro, unsigned column, const uint8_t *value)
{
    if (unlikely(value == nullptr))
        return fptu_ic;

    const fptu_field *pf = fptu_lookup_ro(ro, column, fptu_160);
    if (unlikely(pf == nullptr))
        return fptu_ic;

    return cmpbin(fptu_field_payload(pf)->fixbin, value, 20);
}

fptu_cmp fptu_cmp_192(fptu_ro ro, unsigned column, const uint8_t *value)
{
    if (unlikely(value == nullptr))
        return fptu_ic;

    const fptu_field *pf = fptu_lookup_ro(ro, column, fptu_192);
    if (unlikely(pf == nullptr))
        return fptu_ic;

    return cmpbin(fptu_field_payload(pf)->fixbin, value, 24);
}

fptu_cmp fptu_cmp_256(fptu_ro ro, unsigned column, const uint8_t *value)
{
    if (unlikely(value == nullptr))
        return fptu_ic;

    const fptu_field *pf = fptu_lookup_ro(ro, column, fptu_256);
    if (unlikely(pf == nullptr))
        return fptu_ic;

    return cmpbin(fptu_field_payload(pf)->fixbin, value, 32);
}

//----------------------------------------------------------------------------

fptu_cmp fptu_cmp_opaque(fptu_ro ro, unsigned column, const void *value,
                         size_t bytes)
{
    const fptu_field *pf = fptu_lookup_ro(ro, column, fptu_opaque);
    if (pf == nullptr)
        return bytes ? fptu_ic : fptu_eq;

    const struct iovec iov = fptu_field_opaque(pf);
    return fptu_cmp_binary(iov.iov_base, iov.iov_len, value, bytes);
}

fptu_cmp fptu_cmp_opaque_iov(fptu_ro ro, unsigned column,
                             const struct iovec value)
{
    return fptu_cmp_opaque(ro, column, value.iov_base, value.iov_len);
}

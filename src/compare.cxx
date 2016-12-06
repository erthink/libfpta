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
#include <algorithm>

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

//----------------------------------------------------------------------------

static fptu_cmp fptu_cmp_fields_same_type(const fptu_field *left,
                                          const fptu_field *right)
{
    assert(left != nullptr && right != nullptr);
    assert(fptu_get_type(left->ct) == fptu_get_type(right->ct));

    auto payload_left = fptu_field_payload(left);
    auto payload_right = fptu_field_payload(right);

    switch (fptu_get_type(left->ct)) {
    case fptu_null:
        return fptu_eq;

    case fptu_uint16:
        return fptu_int2cmp(left->get_payload_uint16(),
                            right->get_payload_uint16());
    case fptu_int32:
        return fptu_int2cmp(payload_left->i32, payload_right->i32);
    case fptu_uint32:
        return fptu_int2cmp(payload_left->u32, payload_right->u32);
    case fptu_fp32:
        return fptu_int2cmp(payload_left->fp32, payload_right->fp32);

    case fptu_int64:
        return fptu_int2cmp(payload_left->i64, payload_right->i64);
    case fptu_uint64:
        return fptu_int2cmp(payload_left->u64, payload_right->u64);
    case fptu_fp64:
        return fptu_int2cmp(payload_left->fp64, payload_right->fp64);

    case fptu_96:
        return cmpbin(payload_left->fixbin, payload_right->fixbin, 12);
    case fptu_128:
        return cmpbin(payload_left->fixbin, payload_right->fixbin, 16);
    case fptu_160:
        return cmpbin(payload_left->fixbin, payload_right->fixbin, 20);
    case fptu_192:
        return cmpbin(payload_left->fixbin, payload_right->fixbin, 24);
    case fptu_256:
        return cmpbin(payload_left->fixbin, payload_right->fixbin, 32);

    case fptu_string:
        return fptu_int2cmp(strcmp(payload_left->cstr, payload_right->cstr));

    case fptu_opaque:
        return fptu_cmp_binary(payload_left->other.data,
                               payload_left->other.varlen.opaque_bytes,
                               payload_right->other.data,
                               payload_right->other.varlen.opaque_bytes);

    case fptu_nested:
        return fptu_cmp_tuples(fptu_field_nested(left),
                               fptu_field_nested(right));

    default:
        /* fptu_farray */
        // TODO: лексикографическое сравнение
        return fptu_ic;
    }
}

fptu_cmp fptu_cmp_fields(const fptu_field *left, const fptu_field *right)
{
    if (unlikely(left == nullptr))
        return right ? fptu_lt : fptu_eq;
    if (unlikely(right == nullptr))
        return fptu_gt;

    if (likely(fptu_get_type(left->ct) == fptu_get_type(right->ct)))
        return fptu_cmp_fields_same_type(left, right);

    // TODO: сравнение с кастингом?
    return fptu_ic;
}

//----------------------------------------------------------------------------

fptu_cmp fptu_cmp_tuples(fptu_ro left, fptu_ro right)
{
    // начало и конец дескрипторов слева
    auto l_begin = fptu_begin_ro(left);
    auto l_end = fptu_end_ro(left);
    auto l_size = l_end - l_begin;

    // начало и конец дескрипторов справа
    auto r_begin = fptu_begin_ro(right);
    auto r_end = fptu_end_ro(right);
    auto r_size = r_end - r_begin;

    if (likely(fptu_is_ordered(l_begin, l_end) &&
               fptu_is_ordered(r_begin, r_end))) {
        // TODO: fastpath если оба кортежа уже упорядоченные.
    } else {
        // TODO: account perfomance penalty.
    }

    // буфер на стеке под сортированные теги полей
    uint16_t buffer[l_size + r_size];

    // получаем отсортированные теги слева
    uint16_t *const tags_l_begin = buffer;
    uint16_t *const tags_l_end = fptu_tags(tags_l_begin, l_begin, l_end);
    assert(tags_l_end >= tags_l_begin && tags_l_end <= tags_l_end + l_size);

    // получаем отсортированные теги справа
    uint16_t *const tags_r_begin = tags_l_end;
    uint16_t *const tags_r_end = fptu_tags(tags_r_begin, r_begin, r_end);
    assert(tags_r_end >= tags_r_begin && tags_r_end <= tags_r_end + r_size);

    // идем по отсортированным тегам
    auto tags_l = tags_l_begin, tags_r = tags_r_begin;
    for (;;) {
        // если уперлись в конец слева или справа
        if (tags_l == tags_l_end) {
            if (tags_r == tags_r_end)
                return fptu_eq;
            return fptu_lt;
        } else if (tags_r == tags_r_end)
            return fptu_gt;

        // если слева и справа разные теги
        if (*tags_l != *tags_r)
            return fptu_int2cmp(*tags_l, *tags_r);

        // сканируем все поля с текущим тегом, их может быть несколько
        // (коллекции)
        const uint16_t tag = *tags_l;

        // ищем первое вхождение слева, оно обязано быть
        const fptu_field *field_l = l_begin;
        while (field_l->ct != tag)
            field_l++;
        assert(field_l < l_end);

        // ищем первое вхождение справа, оно обязано быть
        const fptu_field *field_r = r_begin;
        while (field_r->ct != tag)
            field_r++;
        assert(field_r < r_end);

        for (;;) {
            // сравниваем найденые экземпляры
            fptu_cmp cmp = fptu_cmp_fields_same_type(field_l, field_r);
            if (cmp != fptu_eq)
                return cmp;

            // ищем следующее слева
            while (++field_l < l_end && field_l->ct != tag)
                ;

            // ищем следующее справа
            while (++field_r < r_end && field_r->ct != tag)
                ;

            // если дошли до конца слева или справа
            if (field_l == l_end) {
                if (field_r == r_end)
                    return fptu_eq;
                return fptu_lt;
            } else if (field_r == r_end)
                return fptu_gt;
        }

        tags_l++;
        tags_r++;
    }
}

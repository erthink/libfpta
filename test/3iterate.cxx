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

#include <fast_positive/tuples.h>
#include <gtest/gtest.h>

#include <stdlib.h>

static bool field_filter_any(const fptu_field *, void *context, void *param)
{
    (void)context;
    (void)param;
    return true;
}

static bool field_filter_none(const fptu_field *, void *context, void *param)
{
    (void)context;
    (void)param;
    return false;
}

TEST(Iterate, Empty)
{
    char space_exactly_noitems[sizeof(fptu_rw)];
    fptu_rw *pt =
        fptu_init(space_exactly_noitems, sizeof(space_exactly_noitems), 0);
    ASSERT_NE(nullptr, pt);
    ASSERT_STREQ(nullptr, fptu_check(pt));
    EXPECT_EQ(0, fptu_space4items(pt));
    EXPECT_EQ(0, fptu_space4data(pt));
    EXPECT_EQ(0, fptu_junkspace(pt));
    ASSERT_EQ(fptu_end(pt), fptu_begin(pt));

    const fptu_field *end = fptu_end(pt);
    EXPECT_EQ(end, fptu_first(end, end, 0, fptu_any));
    EXPECT_EQ(end, fptu_next(end, end, 0, fptu_any));
    EXPECT_EQ(end,
              fptu_first_ex(end, end, field_filter_any, nullptr, nullptr));
    EXPECT_EQ(end,
              fptu_next_ex(end, end, field_filter_any, nullptr, nullptr));
    EXPECT_EQ(end,
              fptu_first_ex(end, end, field_filter_none, nullptr, nullptr));
    EXPECT_EQ(end,
              fptu_next_ex(end, end, field_filter_none, nullptr, nullptr));

    EXPECT_EQ(0, fptu_field_count(pt, 0, fptu_any));
    EXPECT_EQ(0, fptu_field_count_ex(pt, field_filter_any, nullptr, nullptr));
    EXPECT_EQ(0,
              fptu_field_count_ex(pt, field_filter_none, nullptr, nullptr));

    fptu_ro ro = fptu_take_noshrink(pt);
    ASSERT_STREQ(nullptr, fptu_check_ro(ro));
    ASSERT_EQ(fptu_end_ro(ro), fptu_begin_ro(ro));
    EXPECT_EQ(fptu_end(pt), fptu_end_ro(ro));
    EXPECT_EQ(fptu_begin(pt), fptu_begin_ro(ro));
    EXPECT_EQ(0, fptu_field_count_ro(ro, 0, fptu_any));
    EXPECT_EQ(0,
              fptu_field_count_ro_ex(ro, field_filter_any, nullptr, nullptr));
    EXPECT_EQ(
        0, fptu_field_count_ro_ex(ro, field_filter_none, nullptr, nullptr));
}

TEST(Iterate, Simple)
{
    char space[fptu_buffer_enought];
    fptu_rw *pt = fptu_init(space, sizeof(space), fptu_max_fields);
    ASSERT_NE(nullptr, pt);
    ASSERT_STREQ(nullptr, fptu_check(pt));

    EXPECT_EQ(fptu_ok, fptu_upsert_null(pt, 0));
    ASSERT_STREQ(nullptr, fptu_check(pt));
    EXPECT_EQ(1, fptu_end(pt) - fptu_begin(pt));

    const fptu_field *end = fptu_end(pt);
    const fptu_field *begin = fptu_begin(pt);
    fptu_ro ro = fptu_take_noshrink(pt);
    ASSERT_STREQ(nullptr, fptu_check_ro(ro));

    EXPECT_EQ(begin,
              fptu_first_ex(begin, end, field_filter_any, nullptr, nullptr));
    EXPECT_EQ(end,
              fptu_next_ex(begin, end, field_filter_any, nullptr, nullptr));
    EXPECT_EQ(begin, fptu_first(begin, end, 0, fptu_any));
    EXPECT_EQ(end, fptu_next(begin, end, 0, fptu_any));

    EXPECT_EQ(end,
              fptu_first_ex(begin, end, field_filter_none, nullptr, nullptr));
    EXPECT_EQ(end,
              fptu_next_ex(begin, end, field_filter_none, nullptr, nullptr));
    EXPECT_EQ(end, fptu_first(begin, end, 1, fptu_any));
    EXPECT_EQ(end, fptu_next(begin, end, 1, fptu_any));

    EXPECT_EQ(fptu_end(pt), fptu_end_ro(ro));
    EXPECT_EQ(fptu_begin(pt), fptu_begin_ro(ro));

    EXPECT_EQ(1, fptu_field_count(pt, 0, fptu_any));
    EXPECT_EQ(1, fptu_field_count_ex(pt, field_filter_any, nullptr, nullptr));
    EXPECT_EQ(1, fptu_field_count_ro(ro, 0, fptu_any));
    EXPECT_EQ(1,
              fptu_field_count_ro_ex(ro, field_filter_any, nullptr, nullptr));
    EXPECT_EQ(0,
              fptu_field_count_ex(pt, field_filter_none, nullptr, nullptr));
    EXPECT_EQ(
        0, fptu_field_count_ro_ex(ro, field_filter_none, nullptr, nullptr));

    EXPECT_EQ(fptu_ok, fptu_upsert_null(pt, 1));
    ASSERT_STREQ(nullptr, fptu_check(pt));
    end = fptu_end(pt);
    begin = fptu_begin(pt);
    ro = fptu_take_noshrink(pt);
    ASSERT_STREQ(nullptr, fptu_check_ro(ro));

    EXPECT_EQ(fptu_end(pt), fptu_end_ro(ro));
    EXPECT_EQ(fptu_begin(pt), fptu_begin_ro(ro));
    EXPECT_EQ(begin, fptu_first(begin, end, 1, fptu_any));
    EXPECT_EQ(end, fptu_next(begin, end, 1, fptu_any));

    EXPECT_EQ(1, fptu_field_count(pt, 0, fptu_any));
    EXPECT_EQ(1, fptu_field_count(pt, 1, fptu_any));
    EXPECT_EQ(2, fptu_field_count_ex(pt, field_filter_any, nullptr, nullptr));
    EXPECT_EQ(1, fptu_field_count_ro(ro, 0, fptu_any));
    EXPECT_EQ(1, fptu_field_count_ro(ro, 1, fptu_any));
    EXPECT_EQ(2,
              fptu_field_count_ro_ex(ro, field_filter_any, nullptr, nullptr));
    EXPECT_EQ(0,
              fptu_field_count_ex(pt, field_filter_none, nullptr, nullptr));
    EXPECT_EQ(
        0, fptu_field_count_ro_ex(ro, field_filter_none, nullptr, nullptr));

    for (unsigned n = 1; n < 11; n++) {
        SCOPED_TRACE("n = " + std::to_string(n));
        EXPECT_EQ(fptu_ok, fptu_insert_uint32(pt, 2, 42));
        ASSERT_STREQ(nullptr, fptu_check(pt));
        end = fptu_end(pt);
        begin = fptu_begin(pt);
        ro = fptu_take_noshrink(pt);
        ASSERT_STREQ(nullptr, fptu_check_ro(ro));

        EXPECT_EQ(fptu_end(pt), fptu_end_ro(ro));
        EXPECT_EQ(fptu_begin(pt), fptu_begin_ro(ro));

        EXPECT_EQ(1, fptu_field_count(pt, 0, fptu_any));
        EXPECT_EQ(1, fptu_field_count(pt, 1, fptu_any));
        EXPECT_EQ(n, fptu_field_count(pt, 2, fptu_any));
        EXPECT_EQ(n, fptu_field_count(pt, 2, fptu_uint32));
        EXPECT_EQ(0, fptu_field_count(pt, 2, fptu_null));
        EXPECT_EQ(2 + n, fptu_field_count_ex(pt, field_filter_any, nullptr,
                                             nullptr));
        EXPECT_EQ(
            0, fptu_field_count_ex(pt, field_filter_none, nullptr, nullptr));

        EXPECT_EQ(1, fptu_field_count_ro(ro, 0, fptu_any));
        EXPECT_EQ(1, fptu_field_count_ro(ro, 0, fptu_null));
        EXPECT_EQ(1, fptu_field_count_ro(ro, 1, fptu_any));
        EXPECT_EQ(n, fptu_field_count_ro(ro, 2, fptu_any));
        EXPECT_EQ(0, fptu_field_count_ro(ro, 3, fptu_uint32));
        EXPECT_EQ(2 + n, fptu_field_count_ro_ex(ro, field_filter_any, nullptr,
                                                nullptr));
        EXPECT_EQ(0, fptu_field_count_ro_ex(ro, field_filter_none, nullptr,
                                            nullptr));

        EXPECT_EQ(2 + n, fptu_end(pt) - fptu_begin(pt));
    }
}

TEST(Iterate, Filter)
{
    char space[fptu_buffer_enought];
    fptu_rw *pt = fptu_init(space, sizeof(space), fptu_max_fields);
    ASSERT_NE(nullptr, pt);
    ASSERT_STREQ(nullptr, fptu_check(pt));

    EXPECT_EQ(fptu_ok, fptu_upsert_null(pt, 9));
    EXPECT_EQ(fptu_ok, fptu_upsert_uint16(pt, 9, 2));
    EXPECT_EQ(fptu_ok, fptu_upsert_uint32(pt, 9, 3));
    EXPECT_EQ(fptu_ok, fptu_upsert_int32(pt, 9, 4));
    EXPECT_EQ(fptu_ok, fptu_upsert_int64(pt, 9, 5));
    EXPECT_EQ(fptu_ok, fptu_upsert_uint64(pt, 9, 6));
    EXPECT_EQ(fptu_ok, fptu_upsert_fp32(pt, 9, 7));
    EXPECT_EQ(fptu_ok, fptu_upsert_fp64(pt, 9, 8));
    EXPECT_EQ(fptu_ok, fptu_upsert_cstr(pt, 9, "cstr"));

    ASSERT_STREQ(nullptr, fptu_check(pt));
    // TODO: check array-only filter

    for (unsigned n = 0; n < 11; n++) {
        SCOPED_TRACE("n = " + std::to_string(n));

        EXPECT_EQ((n == 9) ? 1 : 0, fptu_field_count(pt, n, fptu_null));
        EXPECT_EQ((n == 9) ? 1 : 0, fptu_field_count(pt, n, fptu_uint16));
        EXPECT_EQ((n == 9) ? 1 : 0, fptu_field_count(pt, n, fptu_uint32));
        EXPECT_EQ((n == 9) ? 1 : 0, fptu_field_count(pt, n, fptu_uint64));
        EXPECT_EQ((n == 9) ? 1 : 0, fptu_field_count(pt, n, fptu_int32));
        EXPECT_EQ((n == 9) ? 1 : 0, fptu_field_count(pt, n, fptu_int64));
        EXPECT_EQ((n == 9) ? 1 : 0, fptu_field_count(pt, n, fptu_fp32));
        EXPECT_EQ((n == 9) ? 1 : 0, fptu_field_count(pt, n, fptu_fp64));
        EXPECT_EQ((n == 9) ? 1 : 0, fptu_field_count(pt, n, fptu_string));

        EXPECT_EQ((n == 9) ? 9 : 0, fptu_field_count(pt, n, fptu_any));
        EXPECT_EQ((n == 9) ? 2 : 0, fptu_field_count(pt, n, fptu_any_int));
        EXPECT_EQ((n == 9) ? 2 : 0, fptu_field_count(pt, n, fptu_any_uint));
        EXPECT_EQ((n == 9) ? 2 : 0, fptu_field_count(pt, n, fptu_any_fp));

        EXPECT_EQ(0, fptu_field_count(pt, n, fptu_opaque));
        EXPECT_EQ(0, fptu_field_count(pt, n, fptu_nested));
        EXPECT_EQ(0, fptu_field_count(pt, n, fptu_farray));
        EXPECT_EQ(0, fptu_field_count(pt, n, fptu_farray | fptu_null));
    }
}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

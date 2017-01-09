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
#include <gtest/gtest.h>

#include "shuffle6.hpp"
#include <algorithm>
#include <set>
#include <string>

TEST(Compare, FetchTags)
{
    char space[fptu_buffer_enought];
    ASSERT_TRUE(shuffle6::selftest());

    for (unsigned create_iter = 0; create_iter < (1 << 6); ++create_iter) {
        const unsigned create_mask = gray_code(create_iter);
        for (unsigned n = 0; n < shuffle6::factorial; ++n) {
            SCOPED_TRACE("shuffle #" + std::to_string(n) + ", create-mask " +
                         std::to_string(create_mask));

            shuffle6 shuffle(n);
            fptu_rw *pt = fptu_init(space, sizeof(space), fptu_max_fields);
            ASSERT_NE(nullptr, pt);

            std::set<unsigned> checker;
            std::string pattern;
            pattern.reserve(32);

            while (!shuffle.empty()) {
                int i = shuffle.next();
                if (create_mask & (1 << i)) {
                    switch (i) {
                    default:
                        ASSERT_TRUE(false);
                    case 0:
                    case 1:
                        EXPECT_EQ(FPTU_OK, fptu_insert_uint16(pt, 41, 0));
                        checker.insert(fptu_pack_coltype(41, fptu_uint16));
                        pattern += " x";
                        break;
                    case 2:
                    case 3:
                        EXPECT_EQ(FPTU_OK, fptu_insert_int32(pt, 42, 0));
                        checker.insert(fptu_pack_coltype(42, fptu_int32));
                        pattern += " y";
                        break;
                    case 4:
                    case 5:
                        EXPECT_EQ(FPTU_OK, fptu_insert_uint64(pt, 43, 0));
                        checker.insert(fptu_pack_coltype(43, fptu_uint64));
                        pattern += " z";
                        break;
                    }
                }
            }

            ASSERT_STREQ(nullptr, fptu_check(pt));
            fptu_ro ro = fptu_take_noshrink(pt);
            ASSERT_STREQ(nullptr, fptu_check_ro(ro));

            SCOPED_TRACE("pattern" + pattern);
            uint16_t tags[6 + 1];
            const auto end =
                fptu_tags(tags, fptu_begin_ro(ro), fptu_end_ro(ro));
            size_t count = end - tags;

            ASSERT_GE(6, count);
            ASSERT_EQ(checker.size(), count);
            ASSERT_TRUE(std::equal(checker.begin(), checker.end(), tags));
        }
    }
}

void probe(const fptu_rw *major_rw, const fptu_rw *minor_rw)
{
    EXPECT_STREQ(nullptr, fptu_check(major_rw));
    EXPECT_STREQ(nullptr, fptu_check(minor_rw));

    const auto major = fptu_take_noshrink(major_rw);
    const auto minor = fptu_take_noshrink(minor_rw);
    EXPECT_STREQ(nullptr, fptu_check_ro(major));
    EXPECT_STREQ(nullptr, fptu_check_ro(minor));

    EXPECT_EQ(fptu_eq, fptu_cmp_tuples(major, major));
    EXPECT_EQ(fptu_eq, fptu_cmp_tuples(minor, minor));
    EXPECT_EQ(fptu_gt, fptu_cmp_tuples(major, minor));
    EXPECT_EQ(fptu_lt, fptu_cmp_tuples(minor, major));
}

TEST(Compare, Base)
{
    char space4major[fptu_buffer_enought];
    fptu_rw *major =
        fptu_init(space4major, sizeof(space4major), fptu_max_fields);
    ASSERT_NE(nullptr, major);
    ASSERT_STREQ(nullptr, fptu_check(major));

    char space4minor[fptu_buffer_enought];
    fptu_rw *minor =
        fptu_init(space4minor, sizeof(space4minor), fptu_max_fields);
    ASSERT_NE(nullptr, minor);
    ASSERT_STREQ(nullptr, fptu_check(minor));

    // разное кол-во одинаковых полей
    EXPECT_EQ(FPTU_OK, fptu_insert_uint16(major, 0, 0));
    probe(major, minor);
    EXPECT_EQ(FPTU_OK, fptu_insert_uint16(major, 0, 0));
    EXPECT_EQ(FPTU_OK, fptu_insert_uint16(minor, 0, 0));
    probe(major, minor);
    EXPECT_EQ(FPTU_OK, fptu_insert_uint16(major, 0, 0));
    EXPECT_EQ(FPTU_OK, fptu_insert_uint16(minor, 0, 0));
    probe(major, minor);
    ASSERT_EQ(FPTU_OK, fptu_clear(major));
    ASSERT_EQ(FPTU_OK, fptu_clear(minor));

    // разные значения одинаковых полей
    EXPECT_EQ(FPTU_OK, fptu_insert_uint16(major, 0, 2));
    EXPECT_EQ(FPTU_OK, fptu_insert_uint16(minor, 0, 1));
    probe(major, minor);
    EXPECT_EQ(FPTU_OK, fptu_insert_uint16(minor, 0, INT16_MAX));
    probe(major, minor);
    ASSERT_EQ(FPTU_OK, fptu_clear(major));
    ASSERT_EQ(FPTU_OK, fptu_clear(minor));
}

TEST(Compare, Shuffle) {/* TODO */}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

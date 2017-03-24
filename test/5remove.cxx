/*
 * Copyright 2016-2017 libfptu authors: please see AUTHORS file.
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

#include "fptu_test.h"

#include "shuffle6.hpp"

static bool field_filter_any(const fptu_field *, void *context, void *param) {
  (void)context;
  (void)param;
  return true;
}

TEST(Remove, Base) {
  char space[fptu_buffer_enought];
  fptu_rw *pt = fptu_init(space, sizeof(space), fptu_max_fields);
  ASSERT_NE(nullptr, pt);

  // try to remove non-present field
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(0, fptu_erase(pt, 0, fptu_uint32));
  EXPECT_STREQ(nullptr, fptu_check(pt));

  // insert/delete one header-only field
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(FPTU_OK, fptu_insert_uint16(pt, 0, 0));
  EXPECT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(1, fptu_field_count_ex(pt, field_filter_any, nullptr, nullptr));
  EXPECT_EQ(1, fptu_erase(pt, 0, fptu_uint16));
  EXPECT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(0, fptu_erase(pt, 0, fptu_uint32));
  EXPECT_STREQ(nullptr, fptu_check(pt));

  EXPECT_EQ(0, fptu_field_count_ex(pt, field_filter_any, nullptr, nullptr));
  EXPECT_EQ(0, pt->junk);
  EXPECT_EQ(pt->pivot, pt->head);
  EXPECT_EQ(pt->pivot, pt->tail);

  // insert header-only a,b; then delete b,a
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(FPTU_OK, fptu_insert_uint16(pt, 0xA, 0));
  EXPECT_EQ(FPTU_OK, fptu_insert_uint16(pt, 0xB, 0));
  EXPECT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(2, fptu_field_count_ex(pt, field_filter_any, nullptr, nullptr));

  EXPECT_EQ(1, fptu_erase(pt, 0xB, fptu_uint16));
  EXPECT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(1, fptu_field_count_ex(pt, field_filter_any, nullptr, nullptr));
  EXPECT_EQ(0, pt->junk);

  EXPECT_EQ(1, fptu_erase(pt, 0xA, fptu_uint16));
  EXPECT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(0, fptu_field_count_ex(pt, field_filter_any, nullptr, nullptr));
  EXPECT_EQ(0, pt->junk);
  EXPECT_EQ(pt->pivot, pt->head);
  EXPECT_EQ(pt->pivot, pt->tail);

  // insert header-only a,b; then delete a,b
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(FPTU_OK, fptu_insert_uint16(pt, 0xA, 0));
  EXPECT_EQ(FPTU_OK, fptu_insert_uint16(pt, 0xB, 0));
  EXPECT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(2, fptu_field_count_ex(pt, field_filter_any, nullptr, nullptr));

  EXPECT_EQ(1, fptu_erase(pt, 0xA, fptu_uint16));
  EXPECT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(1, fptu_field_count_ex(pt, field_filter_any, nullptr, nullptr));
  EXPECT_EQ(1, pt->junk);

  EXPECT_EQ(1, fptu_erase(pt, 0xB, fptu_uint16));
  EXPECT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(0, fptu_field_count_ex(pt, field_filter_any, nullptr, nullptr));
  EXPECT_EQ(0, pt->junk);
  EXPECT_EQ(pt->pivot, pt->head);
  EXPECT_EQ(pt->pivot, pt->tail);

  // insert a,b; then delete b,a
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(FPTU_OK, fptu_insert_uint32(pt, 0xA, 0));
  EXPECT_EQ(FPTU_OK, fptu_insert_uint32(pt, 0xB, 0));
  EXPECT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(2, fptu_field_count_ex(pt, field_filter_any, nullptr, nullptr));

  EXPECT_EQ(1, fptu_erase(pt, 0xB, fptu_uint32));
  EXPECT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(1, fptu_field_count_ex(pt, field_filter_any, nullptr, nullptr));
  EXPECT_EQ(0, pt->junk);

  EXPECT_EQ(1, fptu_erase(pt, 0xA, fptu_uint32));
  EXPECT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(0, fptu_field_count_ex(pt, field_filter_any, nullptr, nullptr));
  EXPECT_EQ(0, pt->junk);
  EXPECT_EQ(pt->pivot, pt->head);
  EXPECT_EQ(pt->pivot, pt->tail);

  // insert a,b; then delete a,b
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(FPTU_OK, fptu_insert_uint32(pt, 0xA, 0));
  EXPECT_EQ(FPTU_OK, fptu_insert_uint32(pt, 0xB, 0));
  EXPECT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(2, fptu_field_count_ex(pt, field_filter_any, nullptr, nullptr));

  EXPECT_EQ(1, fptu_erase(pt, 0xA, fptu_uint32));
  EXPECT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(1, fptu_field_count_ex(pt, field_filter_any, nullptr, nullptr));
  EXPECT_EQ(2, pt->junk);

  EXPECT_EQ(1, fptu_erase(pt, 0xB, fptu_uint32));
  EXPECT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(0, fptu_field_count_ex(pt, field_filter_any, nullptr, nullptr));
  EXPECT_EQ(0, pt->junk);
  EXPECT_EQ(pt->pivot, pt->head);
  EXPECT_EQ(pt->pivot, pt->tail);
}

TEST(Remove, Serie) {
  char space[fptu_buffer_enought];
  fptu_rw *pt = fptu_init(space, sizeof(space), fptu_max_fields);
  ASSERT_NE(nullptr, pt);

  for (unsigned n = 1; n < 11; ++n) {
    ASSERT_STREQ(nullptr, fptu_check(pt));
    for (unsigned i = 0; i < n; ++i) {
      EXPECT_EQ(FPTU_OK, fptu_insert_uint16(pt, 0, i));
      EXPECT_EQ(FPTU_OK, fptu_insert_uint32(pt, 0, i));
      EXPECT_EQ(FPTU_OK, fptu_insert_uint32(pt, 1, i));
      EXPECT_EQ(FPTU_OK, fptu_insert_uint16(pt, 1, i));
    }
    ASSERT_STREQ(nullptr, fptu_check(pt));
    EXPECT_EQ(n * 4,
              fptu_field_count_ex(pt, field_filter_any, nullptr, nullptr));

    EXPECT_EQ(n, fptu_erase(pt, 1, fptu_filter | (1 << fptu_uint16)));
    EXPECT_STREQ(nullptr, fptu_check(pt));
    EXPECT_EQ(n * 3,
              fptu_field_count_ex(pt, field_filter_any, nullptr, nullptr));

    EXPECT_EQ(n, fptu_erase(pt, 1, fptu_filter | (1 << fptu_uint32)));
    EXPECT_STREQ(nullptr, fptu_check(pt));
    EXPECT_EQ(n * 2,
              fptu_field_count_ex(pt, field_filter_any, nullptr, nullptr));

    for (unsigned i = 0; i < n; ++i) {
      EXPECT_EQ(1, fptu_erase(pt, 0, fptu_uint16));
      EXPECT_STREQ(nullptr, fptu_check(pt));
      EXPECT_EQ((n - i) * 2 - 1,
                fptu_field_count_ex(pt, field_filter_any, nullptr, nullptr));

      EXPECT_EQ(1, fptu_erase(pt, 0, fptu_uint32));
      EXPECT_STREQ(nullptr, fptu_check(pt));
      EXPECT_EQ((n - i) * 2 - 2,
                fptu_field_count_ex(pt, field_filter_any, nullptr, nullptr));
    }

    EXPECT_STREQ(nullptr, fptu_check(pt));
    ASSERT_EQ(0, fptu_field_count_ex(pt, field_filter_any, nullptr, nullptr));
    ASSERT_EQ(0, fptu_junkspace(pt));
  }
}

TEST(Remove, Shuffle) {
  char space[fptu_buffer_enought];

  ASSERT_TRUE(shuffle6::selftest());

  for (unsigned create_iter = 0; create_iter < (1 << 6); ++create_iter) {
    unsigned create_mask = gray_code(create_iter);

    for (unsigned n = 0; n < shuffle6::factorial; ++n) {
      fptu_rw *pt = fptu_init(space, sizeof(space), fptu_max_fields);
      ASSERT_NE(nullptr, pt);

      SCOPED_TRACE("shuffle #" + std::to_string(n) + ", create-mask " +
                   std::to_string(create_mask));

      int created_count = 0;
      for (unsigned i = 0; i < 6; ++i) {
        if (create_mask & (1 << i)) {
          switch (i % 3) {
          default:
            assert(false);
          case 0:
            EXPECT_EQ(FPTU_OK, fptu_insert_uint16(pt, i, i));
            break;
          case 1:
            EXPECT_EQ(FPTU_OK, fptu_insert_uint32(pt, i, i));
            break;
          case 2:
            EXPECT_EQ(FPTU_OK, fptu_insert_uint64(pt, i, i));
            break;
          }
          created_count++;
        }
      }

      ASSERT_STREQ(nullptr, fptu_check(pt));
      EXPECT_EQ(0, fptu_junkspace(pt));
      EXPECT_EQ(created_count,
                fptu_field_count_ex(pt, field_filter_any, nullptr, nullptr));

      int removed_count = 0;
      shuffle6 order(n);
      while (!order.empty()) {
        unsigned i = order.next();
        SCOPED_TRACE("shuffle-item #" + std::to_string(i));
        ASSERT_TRUE(i < 6);

        int present = (create_mask & (1 << i)) ? 1 : 0;
        switch (i % 3) {
        default:
          assert(false);
        case 0:
          EXPECT_EQ(present, fptu_erase(pt, i, fptu_uint16));
          break;
        case 1:
          EXPECT_EQ(present, fptu_erase(pt, i, fptu_uint32));
          break;
        case 2:
          EXPECT_EQ(present, fptu_erase(pt, i, fptu_uint64));
          break;
        }
        removed_count += present;

        ASSERT_STREQ(nullptr, fptu_check(pt));
        ASSERT_EQ(created_count - removed_count,
                  fptu_field_count_ex(pt, field_filter_any, nullptr, nullptr));
      }

      ASSERT_EQ(0, fptu_junkspace(pt));
    }
  }
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

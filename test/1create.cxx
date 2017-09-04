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

TEST(Init, Invalid) {
  EXPECT_EQ(nullptr, fptu_init(nullptr, 0, 0));
  EXPECT_EQ(nullptr,
            fptu_init(nullptr, fptu_max_tuple_bytes / 2, fptu_max_fields / 2));
  EXPECT_EQ(nullptr, fptu_init(nullptr, fptu_max_tuple_bytes, fptu_max_fields));
  EXPECT_EQ(nullptr, fptu_init(nullptr, ~0u, ~0u));

  char space_exactly_noitems[sizeof(fptu_rw)];
  EXPECT_EQ(nullptr,
            fptu_init(space_exactly_noitems, sizeof(space_exactly_noitems), 1));
  EXPECT_EQ(nullptr, fptu_init(space_exactly_noitems,
                               sizeof(space_exactly_noitems), fptu_max_fields));
  EXPECT_EQ(nullptr, fptu_init(nullptr, sizeof(space_exactly_noitems), 0));
  EXPECT_NE(nullptr,
            fptu_init(space_exactly_noitems, sizeof(space_exactly_noitems), 0));
  EXPECT_EQ(nullptr, fptu_init(space_exactly_noitems,
                               sizeof(space_exactly_noitems) - 1, 0));
  EXPECT_EQ(nullptr, fptu_init(space_exactly_noitems, 0, 0));
  EXPECT_EQ(nullptr, fptu_init(space_exactly_noitems, 0, 1));
  EXPECT_EQ(nullptr, fptu_init(space_exactly_noitems, 0, fptu_max_fields));
  EXPECT_EQ(nullptr, fptu_init(space_exactly_noitems, 0, fptu_max_fields * 2));
  EXPECT_EQ(nullptr, fptu_init(space_exactly_noitems, 0, ~0u));

  char space_maximum[fptu_buffer_enough];
  EXPECT_EQ(nullptr, fptu_init(space_maximum, sizeof(space_maximum),
                               fptu_max_fields + 1));
  EXPECT_EQ(nullptr, fptu_init(nullptr, sizeof(space_maximum), 0));
  EXPECT_EQ(nullptr, fptu_init(space_exactly_noitems, ~0u, 1));
  ASSERT_EQ(nullptr, fptu_init(space_exactly_noitems, fptu_buffer_limit + 1,
                               fptu_max_fields));

  EXPECT_NE(nullptr, fptu_init(space_maximum, sizeof(space_maximum), 0));
  EXPECT_NE(nullptr, fptu_init(space_maximum, sizeof(space_maximum), 1));
  EXPECT_NE(nullptr, fptu_init(space_maximum, sizeof(space_maximum),
                               fptu_max_fields / 2));
  EXPECT_NE(nullptr,
            fptu_init(space_maximum, sizeof(space_maximum), fptu_max_fields));
}

TEST(Init, Base) {
  char space[fptu_buffer_enough];

  static const size_t extra_space_cases[] = {
      /* clang-format off */
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 42, sizeof(fptu_rw),
        fptu_max_tuple_bytes / 3, fptu_max_tuple_bytes / 2,
        fptu_max_tuple_bytes
      /* clang-format on */
  };

  static const unsigned items_cases[] = {
      /* clang-format off */
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 42, ~0u, fptu_max_fields / 3,
        fptu_max_fields / 2, fptu_max_fields, fptu_max_fields + 1,
        fptu_max_fields * 2
      /* clang-format on */
  };

  for (auto extra : extra_space_cases) {
    size_t bytes = sizeof(fptu_rw) + extra;
    ASSERT_LE(bytes, sizeof(space));

    for (auto items : items_cases) {
      SCOPED_TRACE("extra " + std::to_string(extra) + ", items " +
                   std::to_string(items));

      fptu_rw *pt = fptu_init(space, bytes, items);
      if (items > extra / 4 || items > fptu_max_fields) {
        EXPECT_EQ(nullptr, pt);
        continue;
      }
      ASSERT_NE(nullptr, pt);

      fptu_ro io = fptu_take_noshrink(pt);
      EXPECT_NE(nullptr, io.units);
      EXPECT_EQ(fptu_unit_size, io.total_bytes);

      EXPECT_EQ(items, fptu_space4items(pt));
      size_t avail =
          FPT_ALIGN_FLOOR(extra, fptu_unit_size) - fptu_unit_size * items;
      EXPECT_EQ(avail, fptu_space4data(pt));
      EXPECT_EQ(0u, fptu_junkspace(pt));

      EXPECT_STREQ(nullptr, fptu_check_ro(io));
      EXPECT_STREQ(nullptr, fptu_check(pt));
    }
  }
}

TEST(Init, Alloc) {
  fptu_rw *pt = fptu_alloc(7, 42);
  ASSERT_NE(nullptr, pt);
  ASSERT_STREQ(nullptr, fptu_check(pt));
  free(pt);
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

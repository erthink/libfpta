/*
 * Copyright 2016 libfpta authors: please see AUTHORS file.
 *
 * This file is part of libfpta, aka "Fast Positive Tables".
 *
 * libfpta is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * libfpta is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with libfpta.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "fast_positive/tables_internal.h"
#include <gtest/gtest.h>

#include "keygen.hpp"

TEST(Corny, NameValidate) {
  EXPECT_TRUE(fpta_name_validate("valid"));
  EXPECT_TRUE(fpta_name_validate("valid_valid"));
  EXPECT_TRUE(fpta_name_validate("valid_42"));

  EXPECT_FALSE(fpta_name_validate(""));
  EXPECT_FALSE(fpta_name_validate(nullptr));
  EXPECT_FALSE(fpta_name_validate("a_very_long_long_long_long_long_long_"
                                  "long_long_long_long_long_long_name"));

  EXPECT_FALSE(fpta_name_validate("no valid"));
  EXPECT_FALSE(fpta_name_validate("1nvalid"));
  EXPECT_FALSE(fpta_name_validate("inval.d"));
  EXPECT_FALSE(fpta_name_validate("_1nvalid"));
  EXPECT_FALSE(fpta_name_validate("invalid#"));
  EXPECT_FALSE(fpta_name_validate("invalid/"));
  EXPECT_FALSE(fpta_name_validate("invalid_ещераз"));
}

TEST(Corny, KeyGenerator) {
  scalar_range_stepper<float>::test(42);
  scalar_range_stepper<float>::test(43);
  scalar_range_stepper<double>::test(42);
  scalar_range_stepper<double>::test(43);
  scalar_range_stepper<uint16_t>::test(42);
  scalar_range_stepper<uint16_t>::test(43);
  scalar_range_stepper<uint32_t>::test(42);
  scalar_range_stepper<uint32_t>::test(43);
  scalar_range_stepper<int32_t>::test(42);
  scalar_range_stepper<int32_t>::test(43);
  scalar_range_stepper<int64_t>::test(42);
  scalar_range_stepper<int64_t>::test(43);

  string_keygen_test<false>(1, 3);
  string_keygen_test<true>(1, 3);
  string_keygen_test<false>(1, fpta_max_keylen);
  string_keygen_test<true>(1, fpta_max_keylen);
  string_keygen_test<false>(8, 8);
  string_keygen_test<true>(8, 8);

  fixbin_stepper<11>::test(42);
  fixbin_stepper<11>::test(43);
  varbin_stepper<fptu_cstr>::test(421);
  varbin_stepper<fptu_cstr>::test(512);
  varbin_stepper<fptu_opaque>::test(421);
  varbin_stepper<fptu_opaque>::test(512);
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

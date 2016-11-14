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

TEST(Name, Validate)
{
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

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

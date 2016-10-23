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

#include <gtest/gtest.h>
#include "fast_positive/internals.h"

TEST(Fetch, Invalid) {
	fpt_ro ro;
	ro.total_bytes = 0;
	ro.units = nullptr;

	EXPECT_EQ(nullptr, fpt_fetch(ro, nullptr, 0, 0, nullptr));
	EXPECT_EQ(nullptr, fpt_fetch(ro, nullptr, fpt_max_tuple_bytes/2, fpt_max_fields/2, nullptr));
	EXPECT_EQ(nullptr, fpt_fetch(ro, nullptr, fpt_max_tuple_bytes, fpt_max_fields, nullptr));
	EXPECT_EQ(nullptr, fpt_fetch(ro, nullptr, ~0u, ~0u, nullptr));

	char space_exactly_noitems[sizeof(fpt_rw)];
	EXPECT_EQ(nullptr, fpt_fetch(ro, space_exactly_noitems, sizeof(space_exactly_noitems), 1, nullptr));
	EXPECT_EQ(nullptr, fpt_fetch(ro, space_exactly_noitems, sizeof(space_exactly_noitems), fpt_max_fields, nullptr));
	EXPECT_EQ(nullptr, fpt_fetch(ro, nullptr, sizeof(space_exactly_noitems), 0, nullptr));
	EXPECT_EQ(nullptr, fpt_fetch(ro, space_exactly_noitems, sizeof(space_exactly_noitems) - 1, 0, nullptr));
	EXPECT_EQ(nullptr, fpt_fetch(ro, space_exactly_noitems, 0, 0, nullptr));
	EXPECT_EQ(nullptr, fpt_fetch(ro, space_exactly_noitems, 0, 1, nullptr));
	EXPECT_EQ(nullptr, fpt_fetch(ro, space_exactly_noitems, 0, fpt_max_fields, nullptr));
	EXPECT_EQ(nullptr, fpt_fetch(ro, space_exactly_noitems, 0, fpt_max_fields * 2, nullptr));
	EXPECT_EQ(nullptr, fpt_fetch(ro, space_exactly_noitems, 0, ~0u, nullptr));

	char space_maximum[sizeof(fpt_rw) + fpt_max_tuple_bytes];
	EXPECT_EQ(nullptr, fpt_fetch(ro, space_maximum, sizeof(space_maximum), fpt_max_fields + 1, nullptr));
	EXPECT_EQ(nullptr, fpt_fetch(ro, nullptr, sizeof(space_maximum), 0, nullptr));
	EXPECT_EQ(nullptr, fpt_fetch(ro, space_exactly_noitems, ~0u, 1, nullptr));
	EXPECT_EQ(nullptr, fpt_fetch(ro, space_exactly_noitems, fpt_max_tuple_bytes * 2, fpt_max_fields, nullptr));

	fpt_rw *pt;
	pt = fpt_fetch(ro, space_exactly_noitems, sizeof(space_exactly_noitems), 0, nullptr);
	EXPECT_NE(nullptr, pt);
	EXPECT_STREQ(nullptr, fpt_check(pt));
	pt = fpt_fetch(ro, space_maximum, sizeof(space_maximum), 0, nullptr);
	EXPECT_NE(nullptr, pt);
	EXPECT_STREQ(nullptr, fpt_check(pt));
	pt = fpt_fetch(ro, space_maximum, sizeof(space_maximum), 1, nullptr);
	EXPECT_NE(nullptr, pt);
	EXPECT_STREQ(nullptr, fpt_check(pt));
	pt = fpt_fetch(ro, space_maximum, sizeof(space_maximum), fpt_max_fields/2, nullptr);
	EXPECT_NE(nullptr, pt);
	EXPECT_STREQ(nullptr, fpt_check(pt));
	pt = fpt_fetch(ro, space_maximum, sizeof(space_maximum), fpt_max_fields, nullptr);
	EXPECT_NE(nullptr, pt);
	EXPECT_STREQ(nullptr, fpt_check(pt));
}

TEST(Fetch, Base) {
	//char space[sizeof(fpt_rw) + fpt_max_tuple_bytes];
	// TODO
}

TEST(Fetch, Alloc) {
	// TODO
}

int main(int argc, char** argv) {
	testing ::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}

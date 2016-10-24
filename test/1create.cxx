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

TEST(Init, Invalid) {
	EXPECT_EQ(nullptr, fpt_init(nullptr, 0, 0));
	EXPECT_EQ(nullptr, fpt_init(nullptr, fpt_max_tuple_bytes/2, fpt_max_fields/2));
	EXPECT_EQ(nullptr, fpt_init(nullptr, fpt_max_tuple_bytes, fpt_max_fields));
	EXPECT_EQ(nullptr, fpt_init(nullptr, ~0u, ~0u));

	char space_exactly_noitems[sizeof(fpt_rw)];
	EXPECT_EQ(nullptr, fpt_init(space_exactly_noitems, sizeof(space_exactly_noitems), 1));
	EXPECT_EQ(nullptr, fpt_init(space_exactly_noitems, sizeof(space_exactly_noitems), fpt_max_fields));
	EXPECT_EQ(nullptr, fpt_init(nullptr, sizeof(space_exactly_noitems), 0));
	EXPECT_NE(nullptr, fpt_init(space_exactly_noitems, sizeof(space_exactly_noitems), 0));
	EXPECT_EQ(nullptr, fpt_init(space_exactly_noitems, sizeof(space_exactly_noitems) - 1, 0));
	EXPECT_EQ(nullptr, fpt_init(space_exactly_noitems, 0, 0));
	EXPECT_EQ(nullptr, fpt_init(space_exactly_noitems, 0, 1));
	EXPECT_EQ(nullptr, fpt_init(space_exactly_noitems, 0, fpt_max_fields));
	EXPECT_EQ(nullptr, fpt_init(space_exactly_noitems, 0, fpt_max_fields * 2));
	EXPECT_EQ(nullptr, fpt_init(space_exactly_noitems, 0, ~0u));

	char space_maximum[fpt_buffer_enought];
	EXPECT_EQ(nullptr, fpt_init(space_maximum, sizeof(space_maximum), fpt_max_fields + 1));
	EXPECT_EQ(nullptr, fpt_init(nullptr, sizeof(space_maximum), 0));
	EXPECT_EQ(nullptr, fpt_init(space_exactly_noitems, ~0u, 1));
	ASSERT_EQ(nullptr, fpt_init(space_exactly_noitems, fpt_buffer_limit + 1, fpt_max_fields));

	EXPECT_NE(nullptr, fpt_init(space_maximum, sizeof(space_maximum), 0));
	EXPECT_NE(nullptr, fpt_init(space_maximum, sizeof(space_maximum), 1));
	EXPECT_NE(nullptr, fpt_init(space_maximum, sizeof(space_maximum), fpt_max_fields/2));
	EXPECT_NE(nullptr, fpt_init(space_maximum, sizeof(space_maximum), fpt_max_fields));
}

TEST(Init, Base) {
	char space[fpt_buffer_enought];

	static const size_t extra_space_cases[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 42,
		sizeof(fpt_rw), fpt_max_tuple_bytes/3, fpt_max_tuple_bytes/2, fpt_max_tuple_bytes};

	static const unsigned items_cases[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 42, ~0u,
		fpt_max_fields/3, fpt_max_fields/2, fpt_max_fields, fpt_max_fields + 1, fpt_max_fields * 2};

	for (auto extra : extra_space_cases) {
		size_t bytes = sizeof(fpt_rw) + extra;
		ASSERT_LE(bytes, sizeof(space));

		for (auto items : items_cases) {
			SCOPED_TRACE("extra " + std::to_string(extra) + ", items " + std::to_string(items));

			fpt_rw *pt = fpt_init(space, bytes, items);
			if (items > extra / 4 || items > fpt_max_fields) {
				EXPECT_EQ(nullptr, pt);
				continue;
			}
			ASSERT_NE(nullptr, pt);

			fpt_ro io = fpt_take_noshrink(pt);
			EXPECT_NE(nullptr, io.units);
			EXPECT_EQ(fpt_unit_size, io.total_bytes);

			EXPECT_EQ(items, fpt_space4items(pt));
			size_t avail = FPT_ALIGN_FLOOR(extra, fpt_unit_size) - fpt_unit_size * items;
			EXPECT_EQ(avail, fpt_space4data(pt));
			EXPECT_EQ(0, fpt_junkspace(pt));

			EXPECT_STREQ(nullptr, fpt_check_ro(io));
			EXPECT_STREQ(nullptr, fpt_check(pt));
		}
	}
}

TEST(Init, Alloc) {
	fpt_rw* pt = fpt_alloc(7, 42);
	ASSERT_NE(nullptr, pt);
	ASSERT_STREQ(nullptr, fpt_check(pt));
	free(pt);
}

int main(int argc, char** argv) {
	testing ::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}

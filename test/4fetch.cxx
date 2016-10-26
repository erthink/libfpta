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

	EXPECT_EQ(nullptr, fpt_fetch(ro, nullptr, 0, 0));
	EXPECT_EQ(nullptr, fpt_fetch(ro, nullptr, fpt_max_tuple_bytes/2, fpt_max_fields/2));
	EXPECT_EQ(nullptr, fpt_fetch(ro, nullptr, fpt_max_tuple_bytes, fpt_max_fields));
	EXPECT_EQ(nullptr, fpt_fetch(ro, nullptr, ~0u, ~0u));

	char space_exactly_noitems[sizeof(fpt_rw)];
	EXPECT_EQ(nullptr, fpt_fetch(ro, space_exactly_noitems, sizeof(space_exactly_noitems), 1));
	EXPECT_EQ(nullptr, fpt_fetch(ro, space_exactly_noitems, sizeof(space_exactly_noitems), fpt_max_fields));
	EXPECT_EQ(nullptr, fpt_fetch(ro, nullptr, sizeof(space_exactly_noitems), 0));
	EXPECT_EQ(nullptr, fpt_fetch(ro, space_exactly_noitems, sizeof(space_exactly_noitems) - 1, 0));
	EXPECT_EQ(nullptr, fpt_fetch(ro, space_exactly_noitems, 0, 0));
	EXPECT_EQ(nullptr, fpt_fetch(ro, space_exactly_noitems, 0, 1));
	EXPECT_EQ(nullptr, fpt_fetch(ro, space_exactly_noitems, 0, fpt_max_fields));
	EXPECT_EQ(nullptr, fpt_fetch(ro, space_exactly_noitems, 0, fpt_max_fields * 2));
	EXPECT_EQ(nullptr, fpt_fetch(ro, space_exactly_noitems, 0, ~0u));

	char space_maximum[sizeof(fpt_rw) + fpt_max_tuple_bytes];
	EXPECT_EQ(nullptr, fpt_fetch(ro, space_maximum, sizeof(space_maximum), fpt_max_fields + 1));
	EXPECT_EQ(nullptr, fpt_fetch(ro, nullptr, sizeof(space_maximum), 0));
	EXPECT_EQ(nullptr, fpt_fetch(ro, space_exactly_noitems, ~0u, 1));
	ASSERT_EQ(nullptr, fpt_fetch(ro, space_exactly_noitems, fpt_buffer_limit + 1, fpt_max_fields));

	fpt_rw *pt;
	pt = fpt_fetch(ro, space_exactly_noitems, sizeof(space_exactly_noitems), 0);
	ASSERT_NE(nullptr, pt);
	EXPECT_STREQ(nullptr, fpt_check(pt));
	pt = fpt_fetch(ro, space_maximum, sizeof(space_maximum), 0);
	ASSERT_NE(nullptr, pt);
	EXPECT_STREQ(nullptr, fpt_check(pt));
	pt = fpt_fetch(ro, space_maximum, sizeof(space_maximum), 1);
	ASSERT_NE(nullptr, pt);
	EXPECT_STREQ(nullptr, fpt_check(pt));
	pt = fpt_fetch(ro, space_maximum, sizeof(space_maximum), fpt_max_fields/2);
	ASSERT_NE(nullptr, pt);
	EXPECT_STREQ(nullptr, fpt_check(pt));
	pt = fpt_fetch(ro, space_maximum, sizeof(space_maximum), fpt_max_fields);
	ASSERT_NE(nullptr, pt);
	EXPECT_STREQ(nullptr, fpt_check(pt));
}

TEST(Fetch, Base) {
	char origin_space[fpt_buffer_enought];
	char fetched_space[fpt_buffer_enought];
	fpt_ro origin_ro, fetched_ro;
	fpt_rw *origin_pt, *fetched_pt;

	origin_pt = fpt_init(origin_space, sizeof(origin_space), fpt_max_fields);
	ASSERT_NE(nullptr, origin_pt);
	EXPECT_STREQ(nullptr, fpt_check(origin_pt));
	origin_ro = fpt_take_noshrink(origin_pt);
	ASSERT_STREQ(nullptr, fpt_check_ro(origin_ro));
	EXPECT_EQ(fpt_unit_size, origin_ro.total_bytes);

	// check empty without more-items
	fetched_pt = fpt_fetch(origin_ro, fetched_space, sizeof(fetched_space), 0);
	ASSERT_NE(nullptr, fetched_pt);
	EXPECT_STREQ(nullptr, fpt_check(fetched_pt));

	fetched_ro = fpt_take_noshrink(fetched_pt);
	ASSERT_STREQ(nullptr, fpt_check_ro(fetched_ro));
	ASSERT_EQ(origin_ro.total_bytes, fetched_ro.total_bytes);
	EXPECT_EQ(0, memcmp(origin_ro.units, fetched_ro.units, origin_ro.total_bytes));

	// check empty with max-more-items
	fetched_pt = fpt_fetch(origin_ro, fetched_space, sizeof(fetched_space), fpt_max_fields);
	ASSERT_NE(nullptr, fetched_pt);
	EXPECT_STREQ(nullptr, fpt_check(fetched_pt));

	fetched_ro = fpt_take_noshrink(fetched_pt);
	ASSERT_STREQ(nullptr, fpt_check_ro(fetched_ro));
	ASSERT_EQ(origin_ro.total_bytes, fetched_ro.total_bytes);
	EXPECT_EQ(0, memcmp(origin_ro.units, fetched_ro.units, origin_ro.total_bytes));

	EXPECT_EQ(origin_pt->end, fetched_pt->end);
	EXPECT_EQ(origin_pt->pivot, fetched_pt->pivot);
	EXPECT_EQ(origin_pt->tail, fetched_pt->tail);
	EXPECT_EQ(origin_pt->head, fetched_pt->head);
	EXPECT_EQ(origin_pt->junk, fetched_pt->junk);

	// adds header-only fields and check
	EXPECT_EQ(fpt_ok, fpt_insert_uint16(origin_pt, fpt_max_cols, 42));
	ASSERT_STREQ(nullptr, fpt_check(origin_pt));
	origin_ro = fpt_take_noshrink(origin_pt);
	ASSERT_STREQ(nullptr, fpt_check_ro(origin_ro));
	EXPECT_EQ(fpt_unit_size * 2, origin_ro.total_bytes);

	// check with max-more-items
	fetched_pt = fpt_fetch(origin_ro, fetched_space, sizeof(fetched_space), fpt_max_fields);
	ASSERT_NE(nullptr, fetched_pt);
	EXPECT_STREQ(nullptr, fpt_check(fetched_pt));

	fetched_ro = fpt_take_noshrink(fetched_pt);
	ASSERT_STREQ(nullptr, fpt_check_ro(fetched_ro));
	ASSERT_EQ(origin_ro.total_bytes, fetched_ro.total_bytes);
	EXPECT_EQ(0, memcmp(origin_ro.units, fetched_ro.units, origin_ro.total_bytes));

	EXPECT_EQ(origin_pt->end, fetched_pt->end);
	EXPECT_EQ(origin_pt->pivot, fetched_pt->pivot);
	EXPECT_EQ(origin_pt->tail, fetched_pt->tail);
	EXPECT_EQ(origin_pt->head, fetched_pt->head);
	EXPECT_EQ(origin_pt->junk, fetched_pt->junk);

	// check without more-items
	fetched_pt = fpt_fetch(origin_ro, fetched_space, sizeof(fetched_space), 0);
	ASSERT_NE(nullptr, fetched_pt);
	EXPECT_STREQ(nullptr, fpt_check(fetched_pt));

	fetched_ro = fpt_take_noshrink(fetched_pt);
	ASSERT_STREQ(nullptr, fpt_check_ro(fetched_ro));
	ASSERT_EQ(origin_ro.total_bytes, fetched_ro.total_bytes);
	EXPECT_EQ(0, memcmp(origin_ro.units, fetched_ro.units, origin_ro.total_bytes));

	// re-create, adds fileds with payload and check
	origin_pt = fpt_init(origin_space, sizeof(origin_space), fpt_max_fields);
	ASSERT_NE(nullptr, origin_pt);
	EXPECT_STREQ(nullptr, fpt_check(origin_pt));

	EXPECT_EQ(fpt_ok, fpt_insert_uint32(origin_pt, fpt_max_cols, 42));
	ASSERT_STREQ(nullptr, fpt_check(origin_pt));
	origin_ro = fpt_take_noshrink(origin_pt);
	ASSERT_STREQ(nullptr, fpt_check_ro(origin_ro));
	EXPECT_EQ(fpt_unit_size * 3, origin_ro.total_bytes);

	// check with max-more-items
	fetched_pt = fpt_fetch(origin_ro, fetched_space, sizeof(fetched_space), fpt_max_fields);
	ASSERT_NE(nullptr, fetched_pt);
	EXPECT_STREQ(nullptr, fpt_check(fetched_pt));

	fetched_ro = fpt_take_noshrink(fetched_pt);
	ASSERT_STREQ(nullptr, fpt_check_ro(fetched_ro));
	ASSERT_EQ(origin_ro.total_bytes, fetched_ro.total_bytes);
	EXPECT_EQ(0, memcmp(origin_ro.units, fetched_ro.units, origin_ro.total_bytes));

	EXPECT_EQ(origin_pt->end, fetched_pt->end);
	EXPECT_EQ(origin_pt->pivot, fetched_pt->pivot);
	EXPECT_EQ(origin_pt->tail, fetched_pt->tail);
	EXPECT_EQ(origin_pt->head, fetched_pt->head);
	EXPECT_EQ(origin_pt->junk, fetched_pt->junk);

	// check without more-items
	fetched_pt = fpt_fetch(origin_ro, fetched_space, sizeof(fetched_space), 0);
	ASSERT_NE(nullptr, fetched_pt);
	EXPECT_STREQ(nullptr, fpt_check(fetched_pt));

	fetched_ro = fpt_take_noshrink(fetched_pt);
	ASSERT_STREQ(nullptr, fpt_check_ro(fetched_ro));
	ASSERT_EQ(origin_ro.total_bytes, fetched_ro.total_bytes);
	EXPECT_EQ(0, memcmp(origin_ro.units, fetched_ro.units, origin_ro.total_bytes));
}

TEST(Fetch, Variate) {
	char origin_space[fpt_buffer_enought];
	char fetched_space[fpt_buffer_enought];
	fpt_ro origin_ro, fetched_ro;
	fpt_rw *origin_pt, *fetched_pt;

	static const size_t space_cases[] = {/*0, 1, 2, 3,*/ 4, 5, 6, 7, 8, 9, 42,
		sizeof(fpt_rw), fpt_max_tuple_bytes/3, fpt_max_tuple_bytes/2, fpt_max_tuple_bytes};

	static const unsigned items_cases[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 42, ~0u,
		fpt_max_fields/3, fpt_max_fields/2, fpt_max_fields, fpt_max_fields + 1, fpt_max_fields * 2};

	for (auto fetch_buffer_space : space_cases) {
		const size_t bytes = sizeof(fpt_rw) + fetch_buffer_space;
		ASSERT_LE(bytes, sizeof(fetched_space));

		for (auto more_items : items_cases) {

			origin_pt = fpt_init(origin_space, sizeof(origin_space), fpt_max_fields);
			ASSERT_NE(nullptr, origin_pt);
			EXPECT_STREQ(nullptr, fpt_check(origin_pt));
			origin_ro = fpt_take_noshrink(origin_pt);
			ASSERT_STREQ(nullptr, fpt_check_ro(origin_ro));
			EXPECT_EQ(fpt_unit_size, origin_ro.total_bytes);

			// check empty
			size_t origin_items = fpt_end_ro(origin_ro) - fpt_begin_ro(origin_ro);
			size_t origin_payload_bytes = origin_ro.total_bytes - units2bytes(origin_items) - fpt_unit_size;
			SCOPED_TRACE("origin.items " + std::to_string(origin_items)
				+ ", origin.payload_bytes " + std::to_string(origin_payload_bytes)
				+ ", fetch.buffer_space " + std::to_string(fetch_buffer_space)
				+ ", fetch.more_items " + std::to_string(more_items));
			fetched_pt = fpt_fetch(origin_ro, fetched_space, bytes, more_items);
			if (more_items > fpt_max_fields
					|| bytes < fpt_space(origin_items + more_items, origin_payload_bytes)) {
				EXPECT_EQ(nullptr, fetched_pt);
			} else {
				EXPECT_NE(nullptr, fetched_pt);
			}
			if (! fetched_pt)
				continue;

			EXPECT_GE(0, (int) fpt_check_and_get_buffer_size(origin_ro, more_items, 0, nullptr));
			const char* error = "clean me";
			EXPECT_GE(bytes, fpt_check_and_get_buffer_size(origin_ro, more_items, 0, &error));
			EXPECT_STREQ(nullptr, error);
			EXPECT_STREQ(nullptr, fpt_check(fetched_pt));
			fetched_ro = fpt_take_noshrink(fetched_pt);
			ASSERT_STREQ(nullptr, fpt_check_ro(fetched_ro));
			ASSERT_EQ(origin_ro.total_bytes, fetched_ro.total_bytes);
			EXPECT_EQ(0, memcmp(origin_ro.units, fetched_ro.units, origin_ro.total_bytes));

			if (more_items + origin_items >= fpt_max_fields) {
				EXPECT_EQ(origin_pt->pivot, fetched_pt->pivot);
				EXPECT_EQ(origin_pt->tail, fetched_pt->tail);
				EXPECT_EQ(origin_pt->head, fetched_pt->head);
				EXPECT_EQ(origin_pt->junk, fetched_pt->junk);
			}
			if (bytes == fpt_buffer_enought)
				EXPECT_EQ(origin_pt->end, fetched_pt->end);

			// adds header-only fields and check
			for (unsigned n = 1; n < 11; ++n) {
				SCOPED_TRACE("header-only, n = " + std::to_string(n));

				EXPECT_EQ(fpt_ok, fpt_insert_uint16(origin_pt, fpt_max_cols, n));
				ASSERT_STREQ(nullptr, fpt_check(origin_pt));
				origin_ro = fpt_take_noshrink(origin_pt);
				ASSERT_STREQ(nullptr, fpt_check_ro(origin_ro));
				EXPECT_EQ(fpt_unit_size * (n + 1), origin_ro.total_bytes);

				origin_items = fpt_end_ro(origin_ro) - fpt_begin_ro(origin_ro);
				origin_payload_bytes = origin_ro.total_bytes - units2bytes(origin_items) - fpt_unit_size;
				SCOPED_TRACE("origin.items " + std::to_string(origin_items)
					+ ", origin.payload_bytes " + std::to_string(origin_payload_bytes)
					+ ", fetch.space " + std::to_string(fetch_buffer_space)
					+ ", more_items " + std::to_string(more_items));
				fetched_pt = fpt_fetch(origin_ro, fetched_space, bytes, more_items);
				if (more_items > fpt_max_fields
						|| bytes < fpt_space(origin_items + more_items, origin_payload_bytes)) {
					EXPECT_EQ(nullptr, fetched_pt);
				} else {
					EXPECT_NE(nullptr, fetched_pt);
				}
				if (! fetched_pt)
					continue;

				EXPECT_GE(bytes, fpt_check_and_get_buffer_size(origin_ro, more_items, 0, &error));
				EXPECT_STREQ(nullptr, error);
				EXPECT_STREQ(nullptr, fpt_check(fetched_pt));
				fetched_ro = fpt_take_noshrink(fetched_pt);
				ASSERT_STREQ(nullptr, fpt_check_ro(fetched_ro));
				ASSERT_EQ(origin_ro.total_bytes, fetched_ro.total_bytes);
				EXPECT_EQ(0, memcmp(origin_ro.units, fetched_ro.units, origin_ro.total_bytes));

				if (more_items + origin_items >= fpt_max_fields) {
					EXPECT_EQ(origin_pt->pivot, fetched_pt->pivot);
					EXPECT_EQ(origin_pt->tail, fetched_pt->tail);
					EXPECT_EQ(origin_pt->head, fetched_pt->head);
					EXPECT_EQ(origin_pt->junk, fetched_pt->junk);
				}
				if (bytes == fpt_buffer_enought)
					EXPECT_EQ(origin_pt->end, fetched_pt->end);
			}

			origin_pt = fpt_init(origin_space, sizeof(origin_space), fpt_max_fields);
			ASSERT_NE(nullptr, origin_pt);
			EXPECT_STREQ(nullptr, fpt_check(origin_pt));

			// adds fileds with payload and check
			for (unsigned n = 1; n < 11; ++n) {
				SCOPED_TRACE("with-payload, n = " + std::to_string(n));

				EXPECT_EQ(fpt_ok, fpt_insert_uint32(origin_pt, fpt_max_cols, n));
				ASSERT_STREQ(nullptr, fpt_check(origin_pt));
				origin_ro = fpt_take_noshrink(origin_pt);
				ASSERT_STREQ(nullptr, fpt_check_ro(origin_ro));
				EXPECT_EQ(fpt_unit_size * (n + n + 1), origin_ro.total_bytes);

				origin_items = fpt_end_ro(origin_ro) - fpt_begin_ro(origin_ro);
				origin_payload_bytes = origin_ro.total_bytes - units2bytes(origin_items) - fpt_unit_size;
				SCOPED_TRACE("origin.items " + std::to_string(origin_items)
					+ ", origin.payload_bytes " + std::to_string(origin_payload_bytes)
					+ ", fetch.space " + std::to_string(fetch_buffer_space)
					+ ", more_items " + std::to_string(more_items));
				fetched_pt = fpt_fetch(origin_ro, fetched_space, bytes, more_items);
				if (more_items > fpt_max_fields
						|| bytes < fpt_space(origin_items + more_items, origin_payload_bytes)) {
					EXPECT_EQ(nullptr, fetched_pt);
				} else {
					EXPECT_NE(nullptr, fetched_pt);
				}
				if (! fetched_pt)
					continue;

				EXPECT_GE(bytes, fpt_check_and_get_buffer_size(origin_ro, more_items, 0, &error));
				EXPECT_STREQ(nullptr, error);
				EXPECT_STREQ(nullptr, fpt_check(fetched_pt));
				fetched_ro = fpt_take_noshrink(fetched_pt);
				ASSERT_STREQ(nullptr, fpt_check_ro(fetched_ro));
				ASSERT_EQ(origin_ro.total_bytes, fetched_ro.total_bytes);
				EXPECT_EQ(0, memcmp(origin_ro.units, fetched_ro.units, origin_ro.total_bytes));

				if (more_items + origin_items >= fpt_max_fields) {
					EXPECT_EQ(origin_pt->pivot, fetched_pt->pivot);
					EXPECT_EQ(origin_pt->tail, fetched_pt->tail);
					EXPECT_EQ(origin_pt->head, fetched_pt->head);
					EXPECT_EQ(origin_pt->junk, fetched_pt->junk);
				}
				if (bytes == fpt_buffer_enought)
					EXPECT_EQ(origin_pt->end, fetched_pt->end);
			}
		}
	}
}

int main(int argc, char** argv) {
	testing ::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}

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
#include <fast_positive/tuples.h>

#include <stdlib.h>

static bool field_filter_any(const fpt_field*, void *context, void *param) {
	(void) context;
	(void) param;
	return true;
}

static bool field_filter_none(const fpt_field*, void *context, void *param) {
	(void) context;
	(void) param;
	return false;
}

TEST(Iterate, Empty) {
	char space_exactly_noitems[sizeof(fpt_rw)];
	fpt_rw *pt = fpt_init(space_exactly_noitems, sizeof(space_exactly_noitems), 0);
	ASSERT_NE(nullptr, pt);
	ASSERT_STREQ(nullptr, fpt_check(pt));
	EXPECT_EQ(0, fpt_space4items(pt));
	EXPECT_EQ(0, fpt_space4data(pt));
	EXPECT_EQ(0, fpt_junkspace(pt));
	ASSERT_EQ(fpt_end(pt), fpt_begin(pt));

	const fpt_field* end = fpt_end(pt);
	EXPECT_EQ(end, fpt_first(end, end, 0, fpt_any));
	EXPECT_EQ(end, fpt_next(end, end, 0, fpt_any));
	EXPECT_EQ(end, fpt_first_ex(end, end, field_filter_any, nullptr, nullptr));
	EXPECT_EQ(end, fpt_next_ex(end, end, field_filter_any, nullptr, nullptr));
	EXPECT_EQ(end, fpt_first_ex(end, end, field_filter_none, nullptr, nullptr));
	EXPECT_EQ(end, fpt_next_ex(end, end, field_filter_none, nullptr, nullptr));

	EXPECT_EQ(0, fpt_field_count(pt, 0, fpt_any));
	EXPECT_EQ(0, fpt_field_count_ex(pt, field_filter_any, nullptr, nullptr));
	EXPECT_EQ(0, fpt_field_count_ex(pt, field_filter_none, nullptr, nullptr));

	fpt_ro ro = fpt_take_noshrink(pt);
	ASSERT_STREQ(nullptr, fpt_check_ro(ro));
	ASSERT_EQ(fpt_end_ro(ro), fpt_begin_ro(ro));
	EXPECT_EQ(fpt_end(pt), fpt_end_ro(ro));
	EXPECT_EQ(fpt_begin(pt), fpt_begin_ro(ro));
	EXPECT_EQ(0, fpt_field_count_ro(ro, 0, fpt_any));
	EXPECT_EQ(0, fpt_field_count_ro_ex(ro, field_filter_any, nullptr, nullptr));
	EXPECT_EQ(0, fpt_field_count_ro_ex(ro, field_filter_none, nullptr, nullptr));
}

TEST(Iterate, Simple) {
	char space[fpt_buffer_enought];
	fpt_rw *pt = fpt_init(space, sizeof(space), fpt_max_fields);
	ASSERT_NE(nullptr, pt);
	ASSERT_STREQ(nullptr, fpt_check(pt));

	EXPECT_EQ(fpt_ok, fpt_upsert_null(pt, 0));
	ASSERT_STREQ(nullptr, fpt_check(pt));
	EXPECT_EQ(1, fpt_end(pt) - fpt_begin(pt));

	const fpt_field* end = fpt_end(pt);
	const fpt_field* begin = fpt_begin(pt);
	fpt_ro ro = fpt_take_noshrink(pt);
	ASSERT_STREQ(nullptr, fpt_check_ro(ro));

	EXPECT_EQ(begin, fpt_first_ex(begin, end, field_filter_any, nullptr, nullptr));
	EXPECT_EQ(end, fpt_next_ex(begin, end, field_filter_any, nullptr, nullptr));
	EXPECT_EQ(begin, fpt_first(begin, end, 0, fpt_any));
	EXPECT_EQ(end, fpt_next(begin, end, 0, fpt_any));

	EXPECT_EQ(end, fpt_first_ex(begin, end, field_filter_none, nullptr, nullptr));
	EXPECT_EQ(end, fpt_next_ex(begin, end, field_filter_none, nullptr, nullptr));
	EXPECT_EQ(end, fpt_first(begin, end, 1, fpt_any));
	EXPECT_EQ(end, fpt_next(begin, end, 1, fpt_any));

	EXPECT_EQ(fpt_end(pt), fpt_end_ro(ro));
	EXPECT_EQ(fpt_begin(pt), fpt_begin_ro(ro));

	EXPECT_EQ(1, fpt_field_count(pt, 0, fpt_any));
	EXPECT_EQ(1, fpt_field_count_ex(pt, field_filter_any, nullptr, nullptr));
	EXPECT_EQ(1, fpt_field_count_ro(ro, 0, fpt_any));
	EXPECT_EQ(1, fpt_field_count_ro_ex(ro, field_filter_any, nullptr, nullptr));
	EXPECT_EQ(0, fpt_field_count_ex(pt, field_filter_none, nullptr, nullptr));
	EXPECT_EQ(0, fpt_field_count_ro_ex(ro, field_filter_none, nullptr, nullptr));

	EXPECT_EQ(fpt_ok, fpt_upsert_null(pt, 1));
	ASSERT_STREQ(nullptr, fpt_check(pt));
	end = fpt_end(pt);
	begin = fpt_begin(pt);
	ro = fpt_take_noshrink(pt);
	ASSERT_STREQ(nullptr, fpt_check_ro(ro));

	EXPECT_EQ(fpt_end(pt), fpt_end_ro(ro));
	EXPECT_EQ(fpt_begin(pt), fpt_begin_ro(ro));
	EXPECT_EQ(begin, fpt_first(begin, end, 1, fpt_any));
	EXPECT_EQ(end, fpt_next(begin, end, 1, fpt_any));

	EXPECT_EQ(1, fpt_field_count(pt, 0, fpt_any));
	EXPECT_EQ(1, fpt_field_count(pt, 1, fpt_any));
	EXPECT_EQ(2, fpt_field_count_ex(pt, field_filter_any, nullptr, nullptr));
	EXPECT_EQ(1, fpt_field_count_ro(ro, 0, fpt_any));
	EXPECT_EQ(1, fpt_field_count_ro(ro, 1, fpt_any));
	EXPECT_EQ(2, fpt_field_count_ro_ex(ro, field_filter_any, nullptr, nullptr));
	EXPECT_EQ(0, fpt_field_count_ex(pt, field_filter_none, nullptr, nullptr));
	EXPECT_EQ(0, fpt_field_count_ro_ex(ro, field_filter_none, nullptr, nullptr));

	for(unsigned n = 1; n < 11; n++) {
		SCOPED_TRACE("n = " + std::to_string(n));
		EXPECT_EQ(fpt_ok, fpt_insert_uint32(pt, 2, 42));
		ASSERT_STREQ(nullptr, fpt_check(pt));
		end = fpt_end(pt);
		begin = fpt_begin(pt);
		ro = fpt_take_noshrink(pt);
		ASSERT_STREQ(nullptr, fpt_check_ro(ro));

		EXPECT_EQ(fpt_end(pt), fpt_end_ro(ro));
		EXPECT_EQ(fpt_begin(pt), fpt_begin_ro(ro));

		EXPECT_EQ(1, fpt_field_count(pt, 0, fpt_any));
		EXPECT_EQ(1, fpt_field_count(pt, 1, fpt_any));
		EXPECT_EQ(n, fpt_field_count(pt, 2, fpt_any));
		EXPECT_EQ(n, fpt_field_count(pt, 2, fpt_uint32));
		EXPECT_EQ(0, fpt_field_count(pt, 2, fpt_null));
		EXPECT_EQ(2 + n, fpt_field_count_ex(pt, field_filter_any, nullptr, nullptr));
		EXPECT_EQ(0, fpt_field_count_ex(pt, field_filter_none, nullptr, nullptr));

		EXPECT_EQ(1, fpt_field_count_ro(ro, 0, fpt_any));
		EXPECT_EQ(1, fpt_field_count_ro(ro, 0, fpt_null));
		EXPECT_EQ(1, fpt_field_count_ro(ro, 1, fpt_any));
		EXPECT_EQ(n, fpt_field_count_ro(ro, 2, fpt_any));
		EXPECT_EQ(0, fpt_field_count_ro(ro, 3, fpt_uint32));
		EXPECT_EQ(2 + n, fpt_field_count_ro_ex(ro, field_filter_any, nullptr, nullptr));
		EXPECT_EQ(0, fpt_field_count_ro_ex(ro, field_filter_none, nullptr, nullptr));

		EXPECT_EQ(2 + n, fpt_end(pt) - fpt_begin(pt));
	}
}

TEST(Iterate, Filter) {
	char space[fpt_buffer_enought];
	fpt_rw *pt = fpt_init(space, sizeof(space), fpt_max_fields);
	ASSERT_NE(nullptr, pt);
	ASSERT_STREQ(nullptr, fpt_check(pt));

	EXPECT_EQ(fpt_ok, fpt_upsert_null(pt, 9));
	EXPECT_EQ(fpt_ok, fpt_upsert_uint16(pt, 9, 2));
	EXPECT_EQ(fpt_ok, fpt_upsert_uint32(pt, 9, 3));
	EXPECT_EQ(fpt_ok, fpt_upsert_int32(pt, 9, 4));
	EXPECT_EQ(fpt_ok, fpt_upsert_int64(pt, 9, 5));
	EXPECT_EQ(fpt_ok, fpt_upsert_uint64(pt, 9, 6));
	EXPECT_EQ(fpt_ok, fpt_upsert_fp32(pt, 9, 7));
	EXPECT_EQ(fpt_ok, fpt_upsert_fp64(pt, 9, 8));
	EXPECT_EQ(fpt_ok, fpt_upsert_cstr(pt, 9, "cstr"));

	ASSERT_STREQ(nullptr, fpt_check(pt));
	// TODO: check array-only filter

	for(unsigned n = 0; n < 11; n++) {
		SCOPED_TRACE("n = " + std::to_string(n));

		EXPECT_EQ((n == 9) ? 1 : 0, fpt_field_count(pt, n, fpt_null));
		EXPECT_EQ((n == 9) ? 1 : 0, fpt_field_count(pt, n, fpt_uint16));
		EXPECT_EQ((n == 9) ? 1 : 0, fpt_field_count(pt, n, fpt_uint32));
		EXPECT_EQ((n == 9) ? 1 : 0, fpt_field_count(pt, n, fpt_uint64));
		EXPECT_EQ((n == 9) ? 1 : 0, fpt_field_count(pt, n, fpt_int32));
		EXPECT_EQ((n == 9) ? 1 : 0, fpt_field_count(pt, n, fpt_int64));
		EXPECT_EQ((n == 9) ? 1 : 0, fpt_field_count(pt, n, fpt_fp32));
		EXPECT_EQ((n == 9) ? 1 : 0, fpt_field_count(pt, n, fpt_fp64));
		EXPECT_EQ((n == 9) ? 1 : 0, fpt_field_count(pt, n, fpt_string));

		EXPECT_EQ((n == 9) ? 9 : 0, fpt_field_count(pt, n, fpt_any));
		EXPECT_EQ((n == 9) ? 2 : 0, fpt_field_count(pt, n, fpt_any_int));
		EXPECT_EQ((n == 9) ? 2 : 0, fpt_field_count(pt, n, fpt_any_uint));
		EXPECT_EQ((n == 9) ? 2 : 0, fpt_field_count(pt, n, fpt_any_fp));

		EXPECT_EQ(0, fpt_field_count(pt, n, fpt_opaque));
		EXPECT_EQ(0, fpt_field_count(pt, n, fpt_nested));
		EXPECT_EQ(0, fpt_field_count(pt, n, fpt_farray));
		EXPECT_EQ(0, fpt_field_count(pt, n, fpt_farray | fpt_null));
	}
}

int main(int argc, char** argv) {
	testing ::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}

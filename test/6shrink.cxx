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

#include "shuffle6.hpp"

static bool field_filter_any(const fpt_field*, void *context, void *param) {
	(void) context;
	(void) param;
	return true;
}

TEST(Shrink, Base) {
	char space[fpt_buffer_enought];
	fpt_rw *pt = fpt_init(space, sizeof(space), fpt_max_fields);
	ASSERT_NE(nullptr, pt);
	ASSERT_STREQ(nullptr, fpt_check(pt));

	// shrink empty
	fpt_shrink(pt);
	ASSERT_STREQ(nullptr, fpt_check(pt));

	// shrink one header-only field
	EXPECT_EQ(fpt_ok, fpt_insert_uint16(pt, 0xA, 0xAA42));
	EXPECT_STREQ(nullptr, fpt_check(pt));
	EXPECT_EQ(1, fpt_field_count_ex(pt, field_filter_any, nullptr, nullptr));
	fpt_shrink(pt);
	ASSERT_STREQ(nullptr, fpt_check(pt));
	EXPECT_EQ(1, fpt_field_count_ex(pt, field_filter_any, nullptr, nullptr));
	EXPECT_EQ(0, pt->junk);

	// add one more header-only and erase first
	EXPECT_EQ(fpt_ok, fpt_insert_uint16(pt, 0xB, 0xBB43));
	EXPECT_EQ(1, fpt_erase(pt, 0xA, fpt_uint16));
	EXPECT_STREQ(nullptr, fpt_check(pt));
	EXPECT_EQ(1, fpt_field_count_ex(pt, field_filter_any, nullptr, nullptr));
	EXPECT_EQ(1, pt->junk);
	fpt_shrink(pt);
	ASSERT_STREQ(nullptr, fpt_check(pt));
	EXPECT_EQ(1, fpt_field_count_ex(pt, field_filter_any, nullptr, nullptr));
	EXPECT_EQ(0, pt->junk);
	fpt_field* fp = fpt_lookup(pt, 0xB, fpt_uint16);
	ASSERT_NE(nullptr, fp);
	EXPECT_EQ(0xBB43, fpt_field_uint16(fp));

	// add thrid field and erase previous
	EXPECT_EQ(fpt_ok, fpt_insert_uint32(pt, 0xC, 42));
	EXPECT_EQ(1, fpt_erase(pt, 0xB, fpt_uint16));
	EXPECT_STREQ(nullptr, fpt_check(pt));
	EXPECT_EQ(1, fpt_field_count_ex(pt, field_filter_any, nullptr, nullptr));
	EXPECT_EQ(1, pt->junk);
	fpt_shrink(pt);
	ASSERT_STREQ(nullptr, fpt_check(pt));
	EXPECT_EQ(1, fpt_field_count_ex(pt, field_filter_any, nullptr, nullptr));
	EXPECT_EQ(0, pt->junk);
	fp = fpt_lookup(pt, 0xC, fpt_uint32);
	ASSERT_NE(nullptr, fp);
	EXPECT_EQ(42, fpt_field_uint32(fp));

	// add fourth field and erase previous
	EXPECT_EQ(fpt_ok, fpt_insert_int64(pt, 0xD, -555));
	EXPECT_EQ(1, fpt_erase(pt, 0xC, fpt_uint32));
	EXPECT_STREQ(nullptr, fpt_check(pt));
	EXPECT_EQ(1, fpt_field_count_ex(pt, field_filter_any, nullptr, nullptr));
	EXPECT_EQ(2, pt->junk);
	fpt_shrink(pt);
	ASSERT_STREQ(nullptr, fpt_check(pt));
	EXPECT_EQ(1, fpt_field_count_ex(pt, field_filter_any, nullptr, nullptr));
	EXPECT_EQ(0, pt->junk);
	fp = fpt_lookup(pt, 0xD, fpt_int64);
	ASSERT_NE(nullptr, fp);
	EXPECT_EQ(-555, fpt_field_int64(fp));
}

TEST(Shrink, Shuffle) {
	char space[fpt_buffer_enought];

	ASSERT_TRUE(shuffle6::selftest());

	for (unsigned create_iter = 0; create_iter < (1 << 6); ++create_iter) {
		const unsigned create_mask = gray_code(create_iter);
		for (unsigned n = 0; n < shuffle6::factorial; ++n) {
			shuffle6 order(n);
			while(! order.empty()) {
				fpt_rw *pt = fpt_init(space, sizeof(space), fpt_max_fields);
				ASSERT_NE(nullptr, pt);

				int count = 0;
				for (int i = 0; i < 6; ++i) {
					if (create_mask & (1 << i)) {
						switch(i % 3) {
						default:
							assert(false);
						case 0:
							EXPECT_EQ(fpt_ok, fpt_insert_uint16(pt, i, 7717 * i));
							break;
						case 1:
							EXPECT_EQ(fpt_ok, fpt_insert_int32(pt, i, -14427139 * i));
							break;
						case 2:
							EXPECT_EQ(fpt_ok, fpt_insert_uint64(pt, i, 53299271467827031 * i));
							break;
						}
						count++;
					}
				}

				ASSERT_STREQ(nullptr, fpt_check(pt));
				EXPECT_EQ(0, fpt_junkspace(pt));
				EXPECT_EQ(count, fpt_field_count_ex(pt, field_filter_any, nullptr, nullptr));

				int present_mask = create_mask;
				int i = order.next();
				ASSERT_TRUE(i < 6);

				int present = (create_mask & (1 << i)) ? 1 : 0;
				switch(i % 3) {
				default:
					assert(false);
				case 0:
					EXPECT_EQ(present, fpt_erase(pt, i, fpt_uint16));
					break;
				case 1:
					EXPECT_EQ(present, fpt_erase(pt, i, fpt_int32));
					break;
				case 2:
					EXPECT_EQ(present, fpt_erase(pt, i, fpt_uint64));
					break;
				}

				if (present) {
					assert(count >= present);
					count -= 1;
					present_mask -= 1 << i;
				}

				SCOPED_TRACE("shuffle #" + std::to_string(n)
					+ ", create-mask " + std::to_string(create_mask)
					+ ", shuffle-item #" + std::to_string(i)
					+ ", present-mask #" + std::to_string(present_mask));

				ASSERT_STREQ(nullptr, fpt_check(pt));
				ASSERT_EQ(count,
					fpt_field_count_ex(pt, field_filter_any, nullptr, nullptr));

				fpt_shrink(pt);
				ASSERT_STREQ(nullptr, fpt_check(pt));
				ASSERT_EQ(count,
					fpt_field_count_ex(pt, field_filter_any, nullptr, nullptr));
				EXPECT_EQ(0, pt->junk);

				if (count) {
					for (int i = 0; i < 6; ++i) {
						if (present_mask & (1 << i)) {
							fpt_field* fp;
							switch(i % 3) {
							default:
								assert(false);
							case 0:
								fp = fpt_lookup(pt, i, fpt_uint16);
								ASSERT_NE(nullptr, fp);
								EXPECT_EQ(7717 * i, fpt_field_uint16(fp));
								break;
							case 1:
								fp = fpt_lookup(pt, i, fpt_int32);
								ASSERT_NE(nullptr, fp);
								EXPECT_EQ(-14427139 * i, fpt_field_int32(fp));
								break;
							case 2:
								fp = fpt_lookup(pt, i, fpt_uint64);
								ASSERT_NE(nullptr, fp);
								EXPECT_EQ(53299271467827031 * i, fpt_field_uint64(fp));
								break;
							}
						}
					}
				}
			}
		}
	}
}

int main(int argc, char** argv) {
	testing ::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}

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

TEST(Trivia, Apriory) {
	ASSERT_EQ(sizeof(uint16_t) * CHAR_BIT, fpt_bits);
	ASSERT_EQ(fpt_unit_size * CHAR_BIT / 2, fpt_bits);
	ASSERT_EQ(UINT16_MAX, (uint16_t) -1ll);
	ASSERT_EQ(UINT32_MAX, (uint32_t) -1ll);
	ASSERT_GE(UINT16_MAX, fpt_limit);
	ASSERT_EQ((uint16_t) ~0ul, fpt_limit);
	ASSERT_TRUE(FPT_IS_POWER2(fpt_bits));
	ASSERT_TRUE(FPT_IS_POWER2(fpt_unit_size));
	ASSERT_EQ(fpt_unit_size, 1 << fpt_unit_shift);

	ASSERT_EQ(fpt_bits, fpt_typeid_bits + fpt_ct_reserve_bits + fpt_co_bits);
	ASSERT_EQ(fpt_bits, fpt_lx_bits + fpt_lt_bits);

	ASSERT_LE(fpt_max_cols, fpt_max_fields);
	ASSERT_LE(fpt_max_field_bytes, fpt_limit * fpt_unit_size);
	ASSERT_LE(fpt_max_opaque_bytes, fpt_max_field_bytes - fpt_unit_size);

	ASSERT_LE(fpt_max_array, fpt_max_fields);
	ASSERT_LE(fpt_max_array, fpt_max_field_bytes / fpt_unit_size - 1);
	ASSERT_GE(fpt_max_field_bytes, fpt_max_fields * fpt_unit_size);
	ASSERT_GE(fpt_max_tuple_bytes, fpt_max_field_bytes + fpt_unit_size * 2);
	ASSERT_GE(fpt_max_tuple_bytes, (fpt_max_fields + 1) * fpt_unit_size * 2);
	ASSERT_LE(fpt_buffer_enought, fpt_buffer_limit);

	ASSERT_EQ(fpt_ty_mask, fpt_farray | fpt_nested);
	ASSERT_GT(fpt_fr_mask, fpt_ty_mask);
	ASSERT_LT(fpt_fr_mask, 1 << fpt_co_shift);
	ASSERT_GT(fpt_limit, fpt_max_cols <<  fpt_co_shift);

	ASSERT_GT(fpt_filter, fpt_ty_mask);
	ASSERT_EQ(fpt_filter, fpt_filter & fpt_any);

	ASSERT_EQ(0, fpt_internal_map_t2b[fpt_null]);
	ASSERT_EQ(0, fpt_internal_map_t2b[fpt_uint16]);
	ASSERT_EQ(0, fpt_internal_map_t2b[fpt_16]);

	ASSERT_EQ(4, fpt_internal_map_t2b[fpt_int32]);
	ASSERT_EQ(4, fpt_internal_map_t2b[fpt_uint32]);
	ASSERT_EQ(4, fpt_internal_map_t2b[fpt_fp32]);
	ASSERT_EQ(4, fpt_internal_map_t2b[fpt_32]);

	ASSERT_EQ(8, fpt_internal_map_t2b[fpt_int64]);
	ASSERT_EQ(8, fpt_internal_map_t2b[fpt_uint64]);
	ASSERT_EQ(8, fpt_internal_map_t2b[fpt_fp64]);
	ASSERT_EQ(8, fpt_internal_map_t2b[fpt_64]);

	ASSERT_EQ(12, fpt_internal_map_t2b[fpt_96]);
	ASSERT_EQ(16, fpt_internal_map_t2b[fpt_128]);
	ASSERT_EQ(20, fpt_internal_map_t2b[fpt_160]);
	ASSERT_EQ(24, fpt_internal_map_t2b[fpt_192]);
	ASSERT_EQ(32, fpt_internal_map_t2b[fpt_256]);

	ASSERT_EQ(0, fpt_internal_map_t2u[fpt_null]);
	ASSERT_EQ(0, fpt_internal_map_t2u[fpt_uint16]);
	ASSERT_EQ(0, fpt_internal_map_t2u[fpt_16]);

	ASSERT_EQ(1, fpt_internal_map_t2u[fpt_int32]);
	ASSERT_EQ(1, fpt_internal_map_t2u[fpt_uint32]);
	ASSERT_EQ(1, fpt_internal_map_t2u[fpt_fp32]);
	ASSERT_EQ(1, fpt_internal_map_t2u[fpt_32]);

	ASSERT_EQ(2, fpt_internal_map_t2u[fpt_int64]);
	ASSERT_EQ(2, fpt_internal_map_t2u[fpt_uint64]);
	ASSERT_EQ(2, fpt_internal_map_t2u[fpt_fp64]);
	ASSERT_EQ(2, fpt_internal_map_t2u[fpt_64]);

	ASSERT_EQ(3, fpt_internal_map_t2u[fpt_96]);
	ASSERT_EQ(4, fpt_internal_map_t2u[fpt_128]);
	ASSERT_EQ(5, fpt_internal_map_t2u[fpt_160]);
	ASSERT_EQ(6, fpt_internal_map_t2u[fpt_192]);
	ASSERT_EQ(8, fpt_internal_map_t2u[fpt_256]);

	ASSERT_EQ(4, sizeof(fpt_varlen));
	ASSERT_EQ(4, sizeof(fpt_field));
	//ASSERT_EQ(8, sizeof(fpt_value));
	ASSERT_EQ(sizeof(struct iovec), sizeof(fpt_ro));

	ASSERT_EQ(sizeof(fpt_rw), fpt_space(0, 0));
}

TEST(Trivia, ColType) {
	unsigned ct;
	ct = fpt_pack_coltype(0, fpt_null);
	ASSERT_EQ(0, ct);
	ASSERT_GT(fpt_limit, ct);
	EXPECT_EQ(0, fpt_get_col(ct));
	EXPECT_EQ(fpt_null, fpt_get_type(ct));

	ct = fpt_pack_coltype(42, fpt_int64);
	ASSERT_NE(0, ct);
	ASSERT_GT(fpt_limit, ct);
	EXPECT_EQ(42, fpt_get_col(ct));
	EXPECT_EQ(fpt_int64, fpt_get_type(ct));

	ct = fpt_pack_coltype(fpt_max_cols, fpt_string | fpt_farray);
	ASSERT_NE(0, ct);
	ASSERT_GT(fpt_limit, ct);
	EXPECT_EQ(fpt_max_cols, fpt_get_col(ct));
	EXPECT_EQ(fpt_string | fpt_farray, fpt_get_type(ct));
}

int main(int argc, char** argv) {
	testing ::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}

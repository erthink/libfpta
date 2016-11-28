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

#include "fast_positive/tuples_internal.h"
#include <gtest/gtest.h>

TEST(Trivia, Apriory)
{
    ASSERT_EQ(sizeof(uint16_t) * CHAR_BIT, fptu_bits);
    ASSERT_EQ(fptu_unit_size * CHAR_BIT / 2, fptu_bits);
    ASSERT_EQ(UINT16_MAX, (uint16_t)-1ll);
    ASSERT_EQ(UINT32_MAX, (uint32_t)-1ll);
    ASSERT_GE(UINT16_MAX, fptu_limit);
    ASSERT_EQ((uint16_t)~0ul, fptu_limit);
    ASSERT_TRUE(FPT_IS_POWER2(fptu_bits));
    ASSERT_TRUE(FPT_IS_POWER2(fptu_unit_size));
    ASSERT_EQ(fptu_unit_size, 1 << fptu_unit_shift);

    ASSERT_EQ(fptu_bits,
              fptu_typeid_bits + fptu_ct_reserve_bits + fptu_co_bits);
    ASSERT_EQ(fptu_bits, fptu_lx_bits + fptu_lt_bits);

    ASSERT_LE(fptu_max_cols, fptu_max_fields);
    ASSERT_LE(fptu_max_field_bytes, fptu_limit * fptu_unit_size);
    ASSERT_LE(fptu_max_opaque_bytes, fptu_max_field_bytes - fptu_unit_size);

    ASSERT_LE(fptu_max_array, fptu_max_fields);
    ASSERT_LE(fptu_max_array, fptu_max_field_bytes / fptu_unit_size - 1);
    ASSERT_GE(fptu_max_field_bytes, fptu_max_fields * fptu_unit_size);
    ASSERT_GE(fptu_max_tuple_bytes,
              fptu_max_field_bytes + fptu_unit_size * 2);
    ASSERT_GE(fptu_max_tuple_bytes,
              (fptu_max_fields + 1) * fptu_unit_size * 2);
    ASSERT_LE(fptu_buffer_enought, fptu_buffer_limit);

    ASSERT_EQ(fptu_ty_mask, fptu_farray | fptu_nested);
    ASSERT_GT(fptu_fr_mask, fptu_ty_mask);
    ASSERT_LT(fptu_fr_mask, 1 << fptu_co_shift);
    ASSERT_GT(fptu_limit, fptu_max_cols << fptu_co_shift);

    ASSERT_GT(fptu_filter, fptu_ty_mask);
    ASSERT_EQ(fptu_filter, fptu_filter & fptu_any);

    ASSERT_EQ(0, fptu_internal_map_t2b[fptu_null]);
    ASSERT_EQ(0, fptu_internal_map_t2b[fptu_uint16]);
    ASSERT_EQ(0, fptu_internal_map_t2b[fptu_16]);

    ASSERT_EQ(4, fptu_internal_map_t2b[fptu_int32]);
    ASSERT_EQ(4, fptu_internal_map_t2b[fptu_uint32]);
    ASSERT_EQ(4, fptu_internal_map_t2b[fptu_fp32]);
    ASSERT_EQ(4, fptu_internal_map_t2b[fptu_32]);

    ASSERT_EQ(8, fptu_internal_map_t2b[fptu_int64]);
    ASSERT_EQ(8, fptu_internal_map_t2b[fptu_uint64]);
    ASSERT_EQ(8, fptu_internal_map_t2b[fptu_fp64]);
    ASSERT_EQ(8, fptu_internal_map_t2b[fptu_64]);

    ASSERT_EQ(12, fptu_internal_map_t2b[fptu_96]);
    ASSERT_EQ(16, fptu_internal_map_t2b[fptu_128]);
    ASSERT_EQ(20, fptu_internal_map_t2b[fptu_160]);
    ASSERT_EQ(24, fptu_internal_map_t2b[fptu_192]);
    ASSERT_EQ(32, fptu_internal_map_t2b[fptu_256]);

    ASSERT_EQ(0, fptu_internal_map_t2u[fptu_null]);
    ASSERT_EQ(0, fptu_internal_map_t2u[fptu_uint16]);
    ASSERT_EQ(0, fptu_internal_map_t2u[fptu_16]);

    ASSERT_EQ(1, fptu_internal_map_t2u[fptu_int32]);
    ASSERT_EQ(1, fptu_internal_map_t2u[fptu_uint32]);
    ASSERT_EQ(1, fptu_internal_map_t2u[fptu_fp32]);
    ASSERT_EQ(1, fptu_internal_map_t2u[fptu_32]);

    ASSERT_EQ(2, fptu_internal_map_t2u[fptu_int64]);
    ASSERT_EQ(2, fptu_internal_map_t2u[fptu_uint64]);
    ASSERT_EQ(2, fptu_internal_map_t2u[fptu_fp64]);
    ASSERT_EQ(2, fptu_internal_map_t2u[fptu_64]);

    ASSERT_EQ(3, fptu_internal_map_t2u[fptu_96]);
    ASSERT_EQ(4, fptu_internal_map_t2u[fptu_128]);
    ASSERT_EQ(5, fptu_internal_map_t2u[fptu_160]);
    ASSERT_EQ(6, fptu_internal_map_t2u[fptu_192]);
    ASSERT_EQ(8, fptu_internal_map_t2u[fptu_256]);

    ASSERT_EQ(4, sizeof(fptu_varlen));
    ASSERT_EQ(4, sizeof(fptu_field));
    ASSERT_EQ(4, sizeof(fptu_unit));
    ASSERT_EQ(sizeof(struct iovec), sizeof(fptu_ro));

    ASSERT_EQ(sizeof(fptu_rw), fptu_space(0, 0));
}

TEST(Trivia, ColType)
{
    unsigned ct;
    ct = fptu_pack_coltype(0, fptu_null);
    ASSERT_EQ(0, ct);
    ASSERT_GT(fptu_limit, ct);
    EXPECT_EQ(0, fptu_get_col(ct));
    EXPECT_EQ(fptu_null, fptu_get_type(ct));

    ct = fptu_pack_coltype(42, fptu_int64);
    ASSERT_NE(0, ct);
    ASSERT_GT(fptu_limit, ct);
    EXPECT_EQ(42, fptu_get_col(ct));
    EXPECT_EQ(fptu_int64, fptu_get_type(ct));

    ct = fptu_pack_coltype(fptu_max_cols, fptu_string | fptu_farray);
    ASSERT_NE(0, ct);
    ASSERT_GT(fptu_limit, ct);
    EXPECT_EQ(fptu_max_cols, fptu_get_col(ct));
    EXPECT_EQ(fptu_string | fptu_farray, fptu_get_type(ct));
}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

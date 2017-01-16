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
#include <cmath>
#include <gtest/gtest.h>

const auto ms100 = fptu_time::ms2fractional(100);

TEST(Trivia, Apriory) {
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
  ASSERT_GE(fptu_max_tuple_bytes, fptu_max_field_bytes + fptu_unit_size * 2);
  ASSERT_GE(fptu_max_tuple_bytes, (fptu_max_fields + 1) * fptu_unit_size * 2);
  ASSERT_LE(fptu_buffer_enought, fptu_buffer_limit);

  ASSERT_EQ(fptu_ty_mask, fptu_farray | fptu_nested);
  ASSERT_GT(fptu_fr_mask, fptu_ty_mask);
  ASSERT_LT(fptu_fr_mask, 1 << fptu_co_shift);
  ASSERT_GT(fptu_limit, fptu_max_cols << fptu_co_shift);

  ASSERT_GT(fptu_filter, fptu_ty_mask);
  ASSERT_EQ(fptu_filter, fptu_filter & fptu_any);

  ASSERT_EQ(0, ct_elem_size(fptu_null));
  ASSERT_EQ(0, ct_elem_size(fptu_uint16));
  ASSERT_EQ(0, ct_elem_size(fptu_16));

  ASSERT_EQ(4, ct_elem_size(fptu_int32));
  ASSERT_EQ(4, ct_elem_size(fptu_uint32));
  ASSERT_EQ(4, ct_elem_size(fptu_fp32));
  ASSERT_EQ(4, ct_elem_size(fptu_32));

  ASSERT_EQ(8, ct_elem_size(fptu_int64));
  ASSERT_EQ(8, ct_elem_size(fptu_uint64));
  ASSERT_EQ(8, ct_elem_size(fptu_fp64));
  ASSERT_EQ(8, ct_elem_size(fptu_64));

  ASSERT_EQ(12, ct_elem_size(fptu_96));
  ASSERT_EQ(16, ct_elem_size(fptu_128));
  ASSERT_EQ(20, ct_elem_size(fptu_160));
  ASSERT_EQ(8, ct_elem_size(fptu_datetime));
  ASSERT_EQ(32, ct_elem_size(fptu_256));

  ASSERT_EQ(bytes2units(ct_elem_size(fptu_null)),
            fptu_internal_map_t2u[fptu_null]);
  ASSERT_EQ(bytes2units(ct_elem_size(fptu_uint16)),
            fptu_internal_map_t2u[fptu_uint16]);
  ASSERT_EQ(bytes2units(ct_elem_size(fptu_16)),
            fptu_internal_map_t2u[fptu_16]);

  ASSERT_EQ(bytes2units(ct_elem_size(fptu_int32)),
            fptu_internal_map_t2u[fptu_int32]);
  ASSERT_EQ(bytes2units(ct_elem_size(fptu_uint32)),
            fptu_internal_map_t2u[fptu_uint32]);
  ASSERT_EQ(bytes2units(ct_elem_size(fptu_fp32)),
            fptu_internal_map_t2u[fptu_fp32]);
  ASSERT_EQ(bytes2units(ct_elem_size(fptu_32)),
            fptu_internal_map_t2u[fptu_32]);

  ASSERT_EQ(bytes2units(ct_elem_size(fptu_int64)),
            fptu_internal_map_t2u[fptu_int64]);
  ASSERT_EQ(bytes2units(ct_elem_size(fptu_uint64)),
            fptu_internal_map_t2u[fptu_uint64]);
  ASSERT_EQ(bytes2units(ct_elem_size(fptu_fp64)),
            fptu_internal_map_t2u[fptu_fp64]);
  ASSERT_EQ(bytes2units(ct_elem_size(fptu_64)),
            fptu_internal_map_t2u[fptu_64]);

  ASSERT_EQ(bytes2units(ct_elem_size(fptu_96)),
            fptu_internal_map_t2u[fptu_96]);
  ASSERT_EQ(bytes2units(ct_elem_size(fptu_128)),
            fptu_internal_map_t2u[fptu_128]);
  ASSERT_EQ(bytes2units(ct_elem_size(fptu_160)),
            fptu_internal_map_t2u[fptu_160]);
  ASSERT_EQ(bytes2units(ct_elem_size(fptu_datetime)),
            fptu_internal_map_t2u[fptu_datetime]);
  ASSERT_EQ(bytes2units(ct_elem_size(fptu_256)),
            fptu_internal_map_t2u[fptu_256]);

  ASSERT_EQ(4, sizeof(fptu_varlen));
  ASSERT_EQ(4, sizeof(fptu_field));
  ASSERT_EQ(4, sizeof(fptu_unit));
  ASSERT_EQ(sizeof(struct iovec), sizeof(fptu_ro));

  ASSERT_EQ(sizeof(fptu_rw), fptu_space(0, 0));
}

TEST(Trivia, ColType) {
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

  ct = fptu_pack_coltype(fptu_max_cols, fptu_cstr | fptu_farray);
  ASSERT_NE(0, ct);
  ASSERT_GT(fptu_limit, ct);
  EXPECT_EQ(fptu_max_cols, fptu_get_col(ct));
  EXPECT_EQ(fptu_cstr | fptu_farray, fptu_get_type(ct));
}

TEST(Trivia, cmp2int) {
  EXPECT_EQ(0, fptu_cmp2int(41, 41));
  EXPECT_EQ(1, fptu_cmp2int(42, 41));
  EXPECT_EQ(-1, fptu_cmp2int(41, 42));

  EXPECT_EQ(0, fptu_cmp2int(-41, -41));
  EXPECT_EQ(1, fptu_cmp2int(0, -41));
  EXPECT_EQ(-1, fptu_cmp2int(-41, 0));

  EXPECT_EQ(1, fptu_cmp2int(42, -42));
  EXPECT_EQ(-1, fptu_cmp2int(-42, 42));
}

TEST(Trivia, cmp2lge) {
  EXPECT_EQ(fptu_eq, fptu_cmp2lge(41, 41));
  EXPECT_EQ(fptu_gt, fptu_cmp2lge(42, 41));
  EXPECT_EQ(fptu_lt, fptu_cmp2lge(41, 42));

  EXPECT_EQ(fptu_eq, fptu_cmp2lge(-41, -41));
  EXPECT_EQ(fptu_gt, fptu_cmp2lge(0, -41));
  EXPECT_EQ(fptu_lt, fptu_cmp2lge(-41, 0));

  EXPECT_EQ(fptu_gt, fptu_cmp2lge(42, -42));
  EXPECT_EQ(fptu_lt, fptu_cmp2lge(-42, 42));
}

TEST(Trivia, diff2lge) {
  EXPECT_EQ(fptu_eq, fptu_diff2lge(0));
  EXPECT_EQ(fptu_gt, fptu_diff2lge(1));
  EXPECT_EQ(fptu_gt, fptu_diff2lge(INT_MAX));
  EXPECT_EQ(fptu_gt, fptu_diff2lge(LONG_MAX));
  EXPECT_EQ(fptu_gt, fptu_diff2lge(ULONG_MAX));
  EXPECT_EQ(fptu_lt, fptu_diff2lge(-1));
  EXPECT_EQ(fptu_lt, fptu_diff2lge(INT_MIN));
  EXPECT_EQ(fptu_lt, fptu_diff2lge(LONG_MIN));
}

TEST(Trivia, iovec) {
  ASSERT_EQ(sizeof(struct iovec), sizeof(fptu_ro));

  fptu_ro serialized;
  serialized.sys.iov_len = 42;
  serialized.sys.iov_base = &serialized;

  ASSERT_EQ(&serialized.total_bytes, &serialized.sys.iov_len);
  ASSERT_EQ(sizeof(serialized.total_bytes), sizeof(serialized.sys.iov_len));
  ASSERT_EQ(serialized.total_bytes, serialized.sys.iov_len);

  ASSERT_EQ((void *)&serialized.units, &serialized.sys.iov_base);
  ASSERT_EQ(sizeof(serialized.units), sizeof(serialized.sys.iov_base));
  ASSERT_EQ(serialized.units, serialized.sys.iov_base);
}

//----------------------------------------------------------------------------

TEST(Trivia, time_ns2fractional) {
  const double scale = exp2(32) / 1e9;
  for (int base_2log = 0; base_2log < 32; ++base_2log) {
    for (int offset_42 = -42; offset_42 <= 42; ++offset_42) {
      SCOPED_TRACE("base_2log " + std::to_string(base_2log) + ", offset_42 " +
                   std::to_string(offset_42));
      uint32_t ns = (1 << base_2log) + offset_42;
      if (ns >= 1e9)
        continue;
      SCOPED_TRACE("ns " + std::to_string(ns) + ", factional " +
                   std::to_string(ns * scale));
      uint32_t probe = floor(ns * scale);
      ASSERT_EQ(probe, fptu_time::ns2fractional(ns));
    }
  }
}

TEST(Trivia, time_fractional2ns) {
  const double scale = 1e9 / exp2(32);
  for (int base_2log = 0; base_2log < 32; ++base_2log) {
    for (int offset_42 = -42; offset_42 <= 42; ++offset_42) {
      SCOPED_TRACE("base_2log " + std::to_string(base_2log) + ", offset_42 " +
                   std::to_string(offset_42));
      uint32_t fractional = (1 << base_2log) + offset_42;
      SCOPED_TRACE("fractional " + std::to_string(fractional) + ", ns " +
                   std::to_string(fractional * scale));
      uint32_t probe = floor(fractional * scale);
      ASSERT_EQ(probe, fptu_time::fractional2ns(fractional));
    }
  }
}

TEST(Trivia, time_us2fractional) {
  const double scale = exp2(32) / 1e6;
  for (int base_2log = 0; base_2log < 32; ++base_2log) {
    for (int offset_42 = -42; offset_42 <= 42; ++offset_42) {
      SCOPED_TRACE("base_2log " + std::to_string(base_2log) + ", offset_42 " +
                   std::to_string(offset_42));
      uint32_t us = (1 << base_2log) + offset_42;
      if (us >= 1e6)
        continue;
      SCOPED_TRACE("us " + std::to_string(us) + ", factional " +
                   std::to_string(us * scale));
      uint32_t probe = floor(us * scale);
      ASSERT_EQ(probe, fptu_time::us2fractional(us));
    }
  }
}

TEST(Trivia, time_fractional2us) {
  const double scale = 1e6 / exp2(32);
  for (int base_2log = 0; base_2log < 32; ++base_2log) {
    for (int offset_42 = -42; offset_42 <= 42; ++offset_42) {
      SCOPED_TRACE("base_2log " + std::to_string(base_2log) + ", offset_42 " +
                   std::to_string(offset_42));
      uint32_t fractional = (1 << base_2log) + offset_42;
      SCOPED_TRACE("fractional " + std::to_string(fractional) + ", us " +
                   std::to_string(fractional * scale));
      uint32_t probe = floor(fractional * scale);
      ASSERT_EQ(probe, fptu_time::fractional2us(fractional));
    }
  }
}

TEST(Trivia, time_ms2fractional) {
  const double scale = exp2(32) / 1e3;
  for (int base_2log = 0; base_2log < 32; ++base_2log) {
    for (int offset_42 = -42; offset_42 <= 42; ++offset_42) {
      SCOPED_TRACE("base_2log " + std::to_string(base_2log) + ", offset_42 " +
                   std::to_string(offset_42));
      uint32_t ms = (1 << base_2log) + offset_42;
      if (ms >= 1e3)
        continue;
      SCOPED_TRACE("ms " + std::to_string(ms) + ", factional " +
                   std::to_string(ms * scale));
      uint32_t probe = floor(ms * scale);
      ASSERT_EQ(probe, fptu_time::ms2fractional(ms));
    }
  }
}

TEST(Trivia, time_fractional2ms) {
  const double scale = 1e3 / exp2(32);
  for (int base_2log = 0; base_2log < 32; ++base_2log) {
    for (int offset_42 = -42; offset_42 <= 42; ++offset_42) {
      SCOPED_TRACE("base_2log " + std::to_string(base_2log) + ", offset_42 " +
                   std::to_string(offset_42));
      uint32_t fractional = (1 << base_2log) + offset_42;
      SCOPED_TRACE("fractional " + std::to_string(fractional) + ", ms " +
                   std::to_string(fractional * scale));
      uint32_t probe = floor(fractional * scale);
      ASSERT_EQ(probe, fptu_time::fractional2ms(fractional));
    }
  }
}

TEST(Trivia, time_coarse) {
  auto prev = fptu_now_coarse();
  for (auto n = 0; n < 42; ++n) {
    auto now = fptu_now_coarse();
    ASSERT_GE(now.fixedpoint, prev.fixedpoint);
    prev = now;
    usleep(137);
  }
}

TEST(Trivia, time_fine) {
  auto prev = fptu_now_fine();
  for (auto n = 0; n < 42; ++n) {
    auto now = fptu_now_fine();
    ASSERT_GE(now.fixedpoint, prev.fixedpoint);
    prev = now;
    usleep(137);
  }
}

TEST(Trivia, time_coarse_vs_fine) {
  for (auto n = 0; n < 42; ++n) {
    auto coarse = fptu_now_coarse();
    auto fine = fptu_now_fine();
    ASSERT_GE(fine.fixedpoint, coarse.fixedpoint);
    ASSERT_GT(ms100, fine.fixedpoint - coarse.fixedpoint);
    usleep(137);
  }
}

namespace std {
template <typename T> std::string to_hex(const T &v) {
  std::stringstream stream;
  stream << std::setfill('0') << std::setw(sizeof(T) * 2) << std::hex << v;
  return stream.str();
}
}

TEST(Trivia, time_grain) {
  for (int grain = -32; grain < 0; ++grain) {
    SCOPED_TRACE("grain " + std::to_string(grain));

    auto prev = fptu_now(grain);
    for (auto n = 0; n < 42; ++n) {
      auto grained = fptu_now(grain);
      ASSERT_GE(grained.fixedpoint, prev.fixedpoint);
      prev = grained;
      auto fine = fptu_now_fine();
      SCOPED_TRACE("grained.hex " + std::to_hex(grained.fractional) +
                   ", fine.hex " + std::to_hex(fine.fractional));
      ASSERT_GE(fine.fixedpoint, grained.fixedpoint);
      for (int bit = 0; - bit > grain; ++bit) {
        SCOPED_TRACE("bit " + std::to_string(bit));
        EXPECT_EQ(0, grained.fractional & (1 << bit));
      }
      usleep(37);
    }
  }
}

//----------------------------------------------------------------------------

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

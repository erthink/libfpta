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

#include <fast_positive/tuples.h>
#include <gtest/gtest.h>

#include <stdlib.h>

TEST(Upsert, InvalidColumn) {
  char space_exactly_noitems[sizeof(fptu_rw)];
  fptu_rw *pt =
      fptu_init(space_exactly_noitems, sizeof(space_exactly_noitems), 0);
  ASSERT_NE(nullptr, pt);
  ASSERT_STREQ(nullptr, fptu_check(pt));

  // column is a stub, expect EINVAL
  const unsigned inval_col = fptu_max_cols + 1;
  EXPECT_EQ(FPTU_EINVAL, fptu_upsert_null(pt, inval_col));

  EXPECT_EQ(FPTU_EINVAL, fptu_upsert_uint16(pt, inval_col, 0));
  EXPECT_EQ(FPTU_EINVAL, fptu_insert_uint16(pt, inval_col, 0));
  EXPECT_EQ(FPTU_EINVAL, fptu_update_uint16(pt, inval_col, 0));

  EXPECT_EQ(FPTU_EINVAL, fptu_upsert_uint32(pt, inval_col, 0));
  EXPECT_EQ(FPTU_EINVAL, fptu_insert_uint32(pt, inval_col, 0));
  EXPECT_EQ(FPTU_EINVAL, fptu_update_uint32(pt, inval_col, 0));

  EXPECT_EQ(FPTU_EINVAL, fptu_upsert_int32(pt, inval_col, 0));
  EXPECT_EQ(FPTU_EINVAL, fptu_insert_int32(pt, inval_col, 0));
  EXPECT_EQ(FPTU_EINVAL, fptu_update_int32(pt, inval_col, 0));

  EXPECT_EQ(FPTU_EINVAL, fptu_upsert_uint64(pt, inval_col, 0));
  EXPECT_EQ(FPTU_EINVAL, fptu_insert_uint64(pt, inval_col, 0));
  EXPECT_EQ(FPTU_EINVAL, fptu_update_uint64(pt, inval_col, 0));

  EXPECT_EQ(FPTU_EINVAL, fptu_upsert_int64(pt, inval_col, 0));
  EXPECT_EQ(FPTU_EINVAL, fptu_insert_int64(pt, inval_col, 0));
  EXPECT_EQ(FPTU_EINVAL, fptu_update_int64(pt, inval_col, 0));

  EXPECT_EQ(FPTU_EINVAL, fptu_upsert_fp64(pt, inval_col, 0));
  EXPECT_EQ(FPTU_EINVAL, fptu_insert_fp64(pt, inval_col, 0));
  EXPECT_EQ(FPTU_EINVAL, fptu_update_fp64(pt, inval_col, 0));

  EXPECT_EQ(FPTU_EINVAL, fptu_upsert_fp32(pt, inval_col, 0));
  EXPECT_EQ(FPTU_EINVAL, fptu_insert_fp32(pt, inval_col, 0));
  EXPECT_EQ(FPTU_EINVAL, fptu_update_fp32(pt, inval_col, 0));

  EXPECT_EQ(FPTU_EINVAL, fptu_upsert_96(pt, inval_col, "96"));
  EXPECT_EQ(FPTU_EINVAL, fptu_insert_96(pt, inval_col, "96"));
  EXPECT_EQ(FPTU_EINVAL, fptu_update_96(pt, inval_col, "96"));

  EXPECT_EQ(FPTU_EINVAL, fptu_upsert_128(pt, inval_col, "128"));
  EXPECT_EQ(FPTU_EINVAL, fptu_insert_128(pt, inval_col, "128"));
  EXPECT_EQ(FPTU_EINVAL, fptu_update_128(pt, inval_col, "128"));

  EXPECT_EQ(FPTU_EINVAL, fptu_upsert_160(pt, inval_col, "160"));
  EXPECT_EQ(FPTU_EINVAL, fptu_insert_160(pt, inval_col, "160"));
  EXPECT_EQ(FPTU_EINVAL, fptu_update_160(pt, inval_col, "160"));

  EXPECT_EQ(FPTU_EINVAL, fptu_upsert_192(pt, inval_col, "192"));
  EXPECT_EQ(FPTU_EINVAL, fptu_insert_192(pt, inval_col, "192"));
  EXPECT_EQ(FPTU_EINVAL, fptu_update_192(pt, inval_col, "192"));

  EXPECT_EQ(FPTU_EINVAL, fptu_upsert_256(pt, inval_col, "256"));
  EXPECT_EQ(FPTU_EINVAL, fptu_insert_256(pt, inval_col, "256"));
  EXPECT_EQ(FPTU_EINVAL, fptu_update_256(pt, inval_col, "256"));

  EXPECT_EQ(FPTU_EINVAL, fptu_upsert_cstr(pt, inval_col, "cstr"));
  EXPECT_EQ(FPTU_EINVAL, fptu_insert_cstr(pt, inval_col, "cstr"));
  EXPECT_EQ(FPTU_EINVAL, fptu_update_cstr(pt, inval_col, "cstr"));

  EXPECT_EQ(FPTU_EINVAL, fptu_upsert_opaque(pt, inval_col, "data", 4));
  EXPECT_EQ(FPTU_EINVAL, fptu_insert_opaque(pt, inval_col, "data", 4));
  EXPECT_EQ(FPTU_EINVAL, fptu_update_opaque(pt, inval_col, "data", 4));

  EXPECT_EQ(0, fptu_space4items(pt));
  EXPECT_EQ(0, fptu_space4data(pt));
  EXPECT_EQ(0, fptu_junkspace(pt));
  EXPECT_STREQ(nullptr, fptu_check(pt));
}

TEST(Upsert, ZeroSpace) {
  char space_exactly_noitems[sizeof(fptu_rw)];
  fptu_rw *pt =
      fptu_init(space_exactly_noitems, sizeof(space_exactly_noitems), 0);
  ASSERT_NE(nullptr, pt);
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(0, fptu_space4items(pt));
  EXPECT_EQ(0, fptu_space4data(pt));
  EXPECT_EQ(0, fptu_junkspace(pt));

  // upsert_xyz() expect no-space
  EXPECT_EQ(FPTU_ENOSPACE, fptu_upsert_null(pt, fptu_max_cols));
  EXPECT_EQ(FPTU_ENOSPACE, fptu_upsert_uint16(pt, 0, 0));
  EXPECT_EQ(FPTU_ENOSPACE, fptu_upsert_uint32(pt, 1, 0));
  EXPECT_EQ(FPTU_ENOSPACE, fptu_upsert_int32(pt, 42, 0));
  EXPECT_EQ(FPTU_ENOSPACE, fptu_upsert_uint64(pt, 111, 0));
  EXPECT_EQ(FPTU_ENOSPACE, fptu_upsert_int64(pt, fptu_max_cols / 3, 0));
  EXPECT_EQ(FPTU_ENOSPACE, fptu_upsert_fp64(pt, fptu_max_cols - 3, 0));
  EXPECT_EQ(FPTU_ENOSPACE, fptu_upsert_fp32(pt, fptu_max_cols - 4, 0));
  EXPECT_EQ(FPTU_ENOSPACE, fptu_upsert_96(pt, fptu_max_cols / 2, "96"));
  EXPECT_EQ(FPTU_ENOSPACE, fptu_upsert_128(pt, 257, "128"));
  EXPECT_EQ(FPTU_ENOSPACE, fptu_upsert_160(pt, 7, "160"));
  EXPECT_EQ(FPTU_ENOSPACE, fptu_upsert_192(pt, 8, "192"));
  EXPECT_EQ(FPTU_ENOSPACE, fptu_upsert_256(pt, fptu_max_cols - 2, "256"));
  EXPECT_EQ(FPTU_ENOSPACE, fptu_upsert_cstr(pt, fptu_max_cols - 1, "cstr"));
  EXPECT_EQ(FPTU_ENOSPACE, fptu_upsert_opaque(pt, fptu_max_cols, "data", 4));

  EXPECT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(0, fptu_space4items(pt));
  EXPECT_EQ(0, fptu_space4data(pt));
  EXPECT_EQ(0, fptu_junkspace(pt));

  // insert_xyz() expect no-space
  EXPECT_EQ(FPTU_ENOSPACE, fptu_insert_uint16(pt, 0, 0));
  EXPECT_EQ(FPTU_ENOSPACE, fptu_insert_uint32(pt, 1, 0));
  EXPECT_EQ(FPTU_ENOSPACE, fptu_insert_int32(pt, 42, 0));
  EXPECT_EQ(FPTU_ENOSPACE, fptu_insert_uint64(pt, 111, 0));
  EXPECT_EQ(FPTU_ENOSPACE, fptu_insert_int64(pt, fptu_max_cols / 3, 0));
  EXPECT_EQ(FPTU_ENOSPACE, fptu_insert_fp64(pt, fptu_max_cols - 3, 0));
  EXPECT_EQ(FPTU_ENOSPACE, fptu_insert_fp32(pt, fptu_max_cols - 4, 0));
  EXPECT_EQ(FPTU_ENOSPACE, fptu_insert_96(pt, fptu_max_cols / 2, "96"));
  EXPECT_EQ(FPTU_ENOSPACE, fptu_insert_128(pt, 257, "128"));
  EXPECT_EQ(FPTU_ENOSPACE, fptu_insert_160(pt, 7, "160"));
  EXPECT_EQ(FPTU_ENOSPACE, fptu_insert_192(pt, 8, "192"));
  EXPECT_EQ(FPTU_ENOSPACE, fptu_insert_256(pt, fptu_max_cols - 2, "256"));
  EXPECT_EQ(FPTU_ENOSPACE, fptu_insert_cstr(pt, fptu_max_cols - 1, "cstr"));
  EXPECT_EQ(FPTU_ENOSPACE, fptu_insert_opaque(pt, fptu_max_cols, "data", 4));

  EXPECT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(0, fptu_space4items(pt));
  EXPECT_EQ(0, fptu_space4data(pt));
  EXPECT_EQ(0, fptu_junkspace(pt));

  // update_xyz() expect no-entry
  EXPECT_EQ(FPTU_ENOFIELD, fptu_update_uint16(pt, 0, 0));
  EXPECT_EQ(FPTU_ENOFIELD, fptu_update_uint32(pt, 1, 0));
  EXPECT_EQ(FPTU_ENOFIELD, fptu_update_int32(pt, 42, 0));
  EXPECT_EQ(FPTU_ENOFIELD, fptu_update_uint64(pt, 111, 0));
  EXPECT_EQ(FPTU_ENOFIELD, fptu_update_int64(pt, fptu_max_cols / 3, 0));
  EXPECT_EQ(FPTU_ENOFIELD, fptu_update_fp64(pt, fptu_max_cols - 3, 0));
  EXPECT_EQ(FPTU_ENOFIELD, fptu_update_fp32(pt, fptu_max_cols - 4, 0));
  EXPECT_EQ(FPTU_ENOFIELD, fptu_update_96(pt, fptu_max_cols / 2, "96"));
  EXPECT_EQ(FPTU_ENOFIELD, fptu_update_128(pt, 257, "128"));
  EXPECT_EQ(FPTU_ENOFIELD, fptu_update_160(pt, 7, "160"));
  EXPECT_EQ(FPTU_ENOFIELD, fptu_update_192(pt, 8, "192"));
  EXPECT_EQ(FPTU_ENOFIELD, fptu_update_256(pt, fptu_max_cols - 2, "256"));
  EXPECT_EQ(FPTU_ENOFIELD, fptu_update_cstr(pt, fptu_max_cols - 1, "cstr"));
  EXPECT_EQ(FPTU_ENOFIELD, fptu_update_opaque(pt, fptu_max_cols, "data", 4));

  EXPECT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(0, fptu_space4items(pt));
  EXPECT_EQ(0, fptu_space4data(pt));
  EXPECT_EQ(0, fptu_junkspace(pt));
}

static const uint8_t pattern[256] = {
    /* clang-format off */
    177, 85,  188, 146, 222, 148, 10,  7,   241, 57,  199, 43,  106, 240, 124,
    237, 220, 230, 197, 76,  116, 153, 205, 221, 28,  2,   31,  233, 58,  60,
    159, 228, 109, 20,  66,  214, 111, 15,  18,  44,  208, 72,  249, 210, 113,
    212, 165, 1,   225, 174, 164, 204, 45,  130, 82,  80,  99,  138, 48,  167,
    78,  14,  149, 207, 103, 178, 223, 25,  163, 118, 139, 122, 37,  119, 182,
    26,  4,   236, 96,  64,  196, 75,  29,  95,  252, 33,  185, 87,  110, 202,
    200, 125, 93,  55,  84,  105, 89,  215, 161, 211, 154, 86,  39,  145, 77,
    190, 147, 136, 108, 132, 107, 172, 229, 83,  187, 226, 160, 155, 242, 133,
    23,  8,   6,   151, 184, 195, 17,  16,  140, 191, 131, 156, 61,  239, 127,
    181, 94,  176, 27,  81,  235, 141, 69,  47,  170, 74,  168, 88,  56,  193,
    68,  209, 104, 143, 52,  53,  46,  115, 158, 100, 243, 213, 247, 34,  62,
    238, 203, 232, 92,  49,  54,  42,  245, 171, 227, 123, 24,  186, 63,  112,
    135, 183, 254, 5,   198, 13,  216, 73,  219, 173, 255, 121, 79,  137, 150,
    12,  162, 41,  206, 217, 231, 120, 59,  128, 101, 51,  201, 253, 35,  194,
    166, 70,  71,  11,  189, 50,  234, 218, 30,  0,   134, 32,  152, 90,  19,
    224, 3,   250, 98,  169, 102, 38,  142, 91,  117, 180, 175, 246, 9,   129,
    114, 244, 67,  157, 21,  144, 126, 40,  179, 36,  192, 248, 22,  65,  251,
    97
    /* clang-format on */
};

TEST(Upsert, Base) {
  fptu_rw *pt = fptu_alloc(15, 38 * 4);
  ASSERT_NE(nullptr, pt);
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(15, fptu_space4items(pt));
  EXPECT_EQ(38 * 4, fptu_space4data(pt));
  EXPECT_EQ(0, fptu_junkspace(pt));

  EXPECT_EQ(FPTU_OK, fptu_upsert_null(pt, fptu_max_cols));
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(14, fptu_space4items(pt));
  EXPECT_EQ(38 * 4, fptu_space4data(pt));

  EXPECT_EQ(FPTU_OK, fptu_upsert_uint16(pt, 0, 0x8001));
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(13, fptu_space4items(pt));
  EXPECT_EQ(38 * 4, fptu_space4data(pt));

  EXPECT_EQ(FPTU_OK, fptu_upsert_uint32(pt, 1, 1354824703));
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(12, fptu_space4items(pt));
  EXPECT_EQ(38 * 4 - 4, fptu_space4data(pt));

  EXPECT_EQ(FPTU_OK, fptu_upsert_int32(pt, 42, -8782211));
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(11, fptu_space4items(pt));
  EXPECT_EQ(38 * 4 - 8, fptu_space4data(pt));

  EXPECT_EQ(FPTU_OK, fptu_upsert_uint64(pt, 111, 15047220096467327));
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(10, fptu_space4items(pt));
  EXPECT_EQ(38 * 4 - 16, fptu_space4data(pt));

  EXPECT_EQ(FPTU_OK,
            fptu_upsert_int64(pt, fptu_max_cols / 3, -60585001468255361));
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(9, fptu_space4items(pt));
  EXPECT_EQ(38 * 4 - 24, fptu_space4data(pt));

  EXPECT_EQ(FPTU_OK,
            fptu_upsert_fp64(pt, fptu_max_cols - 3, 3.14159265358979323846));
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(8, fptu_space4items(pt));
  EXPECT_EQ(38 * 4 - 32, fptu_space4data(pt));

  EXPECT_EQ(FPTU_OK,
            fptu_upsert_fp32(pt, fptu_max_cols - 4, 2.7182818284590452354));
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(7, fptu_space4items(pt));
  EXPECT_EQ(38 * 4 - 36, fptu_space4data(pt));

  static const uint8_t *_96 = pattern;
  static const uint8_t *_128 = _96 + 12;
  static const uint8_t *_160 = _128 + 16;
  static const uint8_t *_192 = _160 + 20;
  static const uint8_t *_256 = _192 + 24;
  ASSERT_LT(32, pattern + sizeof(pattern) - _256);

  EXPECT_EQ(FPTU_OK, fptu_upsert_96(pt, fptu_max_cols / 2, _96));
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(6, fptu_space4items(pt));
  EXPECT_EQ(38 * 4 - 48, fptu_space4data(pt));

  EXPECT_EQ(FPTU_OK, fptu_upsert_128(pt, 257, _128));
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(5, fptu_space4items(pt));
  EXPECT_EQ(38 * 4 - 64, fptu_space4data(pt));

  EXPECT_EQ(FPTU_OK, fptu_upsert_160(pt, 7, _160));
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(4, fptu_space4items(pt));
  EXPECT_EQ(38 * 4 - 84, fptu_space4data(pt));

  EXPECT_EQ(FPTU_OK, fptu_upsert_192(pt, 8, _192));
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(3, fptu_space4items(pt));
  EXPECT_EQ(38 * 4 - 108, fptu_space4data(pt));

  EXPECT_EQ(FPTU_OK, fptu_upsert_256(pt, fptu_max_cols - 2, _256));
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(2, fptu_space4items(pt));
  EXPECT_EQ(38 * 4 - 140, fptu_space4data(pt));

  EXPECT_EQ(FPTU_OK, fptu_upsert_cstr(pt, fptu_max_cols - 1, "abc"));
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(1, fptu_space4items(pt));
  EXPECT_EQ(8, fptu_space4data(pt));

  EXPECT_EQ(FPTU_OK, fptu_upsert_cstr(pt, 42, "cstr"));
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(0, fptu_space4items(pt));
  EXPECT_EQ(0, fptu_space4data(pt));
  EXPECT_EQ(0, fptu_junkspace(pt));

  // update present column, expect no error
  EXPECT_EQ(FPTU_OK, fptu_upsert_null(pt, fptu_max_cols));

  EXPECT_EQ(0, fptu_space4items(pt));
  EXPECT_EQ(0, fptu_space4data(pt));
  EXPECT_EQ(0, fptu_junkspace(pt));
  EXPECT_EQ(FPTU_ENOSPACE, fptu_upsert_null(pt, 33));

  EXPECT_EQ(0, fptu_space4items(pt));
  EXPECT_EQ(0, fptu_space4data(pt));
  EXPECT_EQ(0, fptu_junkspace(pt));
  ASSERT_STREQ(nullptr, fptu_check(pt));

  fptu_ro ro = fptu_take_noshrink(pt);
  ASSERT_STREQ(nullptr, fptu_check_ro(ro));
  EXPECT_EQ((1 + 15 + 38) * 4, ro.total_bytes);

  EXPECT_EQ(0x8001, fptu_get_uint16(ro, 0, nullptr));
  EXPECT_EQ(1354824703, fptu_get_uint32(ro, 1, nullptr));
  EXPECT_EQ(-8782211, fptu_get_int32(ro, 42, nullptr));
  EXPECT_EQ(15047220096467327, fptu_get_uint64(ro, 111, nullptr));
  EXPECT_EQ(-60585001468255361,
            fptu_get_int64(ro, fptu_max_cols / 3, nullptr));
  EXPECT_EQ(3.1415926535897932,
            fptu_get_fp64(ro, fptu_max_cols - 3, nullptr));
  EXPECT_EQ((float)2.7182818284590452354,
            fptu_get_fp32(ro, fptu_max_cols - 4, nullptr));
  EXPECT_STREQ("abc", fptu_get_cstr(ro, fptu_max_cols - 1, nullptr));
  EXPECT_STREQ("cstr", fptu_get_cstr(ro, 42, nullptr));
  EXPECT_EQ(fptu_eq, fptu_cmp_96(ro, fptu_max_cols / 2, _96));
  EXPECT_EQ(fptu_eq, fptu_cmp_128(ro, 257, _128));
  EXPECT_EQ(fptu_eq, fptu_cmp_160(ro, 7, _160));
  EXPECT_EQ(fptu_eq, fptu_cmp_192(ro, 8, _192));
  EXPECT_EQ(fptu_eq, fptu_cmp_256(ro, fptu_max_cols - 2, _256));

  EXPECT_EQ(0, fptu_space4items(pt));
  EXPECT_EQ(0, fptu_space4data(pt));
  EXPECT_EQ(0, fptu_junkspace(pt));
  ASSERT_STREQ(nullptr, fptu_check(pt));
  free(pt);
}

struct iovec opaque_iov(unsigned col, unsigned n, unsigned salt) {
  struct iovec value;
  value.iov_base = (void *)(pattern + (n * salt) % 223);
  value.iov_len = 4 + ((n + col) & 3) * 4;
  return value;
}

TEST(Upsert, Overwrite) {
  fptu_rw *pt = fptu_alloc(3 + 3 * 2, (1 + 2 + (2 + 3 + 4) * 2 + 5) * 4);
  ASSERT_NE(nullptr, pt);
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(9, fptu_space4items(pt));
  EXPECT_EQ(0, fptu_junkspace(pt));

  const unsigned m = 46091;
  unsigned n = 1;
  EXPECT_EQ(FPTU_OK, fptu_upsert_opaque_iov(pt, 0, opaque_iov(0, n, 23)));
  EXPECT_EQ(FPTU_OK, fptu_upsert_opaque_iov(pt, 1, opaque_iov(1, n, 37)));
  EXPECT_EQ(FPTU_OK, fptu_upsert_opaque_iov(pt, 2, opaque_iov(2, n, 41)));
  EXPECT_EQ(FPTU_OK, fptu_upsert_uint16(pt, 3, n * m));
  EXPECT_EQ(FPTU_OK, fptu_upsert_uint32(pt, 4, n * m * m));
  EXPECT_EQ(FPTU_OK, fptu_upsert_uint64(pt, 5, n * m * m * m));
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(3, fptu_space4items(pt));
  EXPECT_EQ(0, fptu_junkspace(pt));

  fptu_ro ro = fptu_take_noshrink(pt);
  ASSERT_STREQ(nullptr, fptu_check_ro(ro));
  EXPECT_EQ(fptu_eq, fptu_cmp_opaque_iov(ro, 0, opaque_iov(0, n, 23)));
  EXPECT_EQ(fptu_eq, fptu_cmp_opaque_iov(ro, 1, opaque_iov(1, n, 37)));
  EXPECT_EQ(fptu_eq, fptu_cmp_opaque_iov(ro, 2, opaque_iov(2, n, 41)));
  EXPECT_EQ((uint16_t)(n * m), fptu_get_uint16(ro, 3, nullptr));
  EXPECT_EQ((uint32_t)(n * m * m), fptu_get_uint32(ro, 4, nullptr));
  EXPECT_EQ((uint64_t)(n * m * m * m), fptu_get_uint64(ro, 5, nullptr));

  while (n < 1001) {
    unsigned p = n++;

    // overwrite field#3 and check all
    SCOPED_TRACE("touch field #3, uint16_t, n " + std::to_string(n));
    ASSERT_EQ(FPTU_OK, fptu_upsert_uint16(pt, 3, n * m));
    ASSERT_STREQ(nullptr, fptu_check(pt));
    ro = fptu_take_noshrink(pt);
    ASSERT_STREQ(nullptr, fptu_check_ro(ro));
    EXPECT_EQ(fptu_eq, fptu_cmp_opaque_iov(ro, 0, opaque_iov(0, p, 23)));
    EXPECT_EQ(fptu_eq, fptu_cmp_opaque_iov(ro, 1, opaque_iov(1, p, 37)));
    EXPECT_EQ(fptu_eq, fptu_cmp_opaque_iov(ro, 2, opaque_iov(2, p, 41)));
    EXPECT_EQ((uint16_t)(n * m), fptu_get_uint16(ro, 3, nullptr));
    EXPECT_EQ((uint32_t)(p * m * m), fptu_get_uint32(ro, 4, nullptr));
    EXPECT_EQ((uint64_t)(p * m * m * m), fptu_get_uint64(ro, 5, nullptr));

    // overwrite field#5 and check all
    SCOPED_TRACE("touch field #5, uint64_t, n " + std::to_string(n));
    ASSERT_EQ(FPTU_OK, fptu_upsert_uint64(pt, 5, n * m * m * m));
    ASSERT_STREQ(nullptr, fptu_check(pt));
    ro = fptu_take_noshrink(pt);
    ASSERT_STREQ(nullptr, fptu_check_ro(ro));
    EXPECT_EQ(fptu_eq, fptu_cmp_opaque_iov(ro, 0, opaque_iov(0, p, 23)));
    EXPECT_EQ(fptu_eq, fptu_cmp_opaque_iov(ro, 1, opaque_iov(1, p, 37)));
    EXPECT_EQ(fptu_eq, fptu_cmp_opaque_iov(ro, 2, opaque_iov(2, p, 41)));
    EXPECT_EQ((uint16_t)(n * m), fptu_get_uint16(ro, 3, nullptr));
    EXPECT_EQ((uint32_t)(p * m * m), fptu_get_uint32(ro, 4, nullptr));
    EXPECT_EQ((uint64_t)(n * m * m * m), fptu_get_uint64(ro, 5, nullptr));

    // overwrite field#0 and check all
    SCOPED_TRACE("touch field #0, len " +
                 std::to_string(opaque_iov(0, p, 23).iov_len) + "=>" +
                 std::to_string(opaque_iov(0, n, 23).iov_len) + ", n " +
                 std::to_string(n) + ", space " +
                 std::to_string(fptu_space4items(pt)) + "/" +
                 std::to_string(fptu_space4data(pt)) + ", junk " +
                 std::to_string(fptu_junkspace(pt)));
    ASSERT_EQ(FPTU_OK, fptu_upsert_opaque_iov(pt, 0, opaque_iov(0, n, 23)));
    ASSERT_STREQ(nullptr, fptu_check(pt));
    ro = fptu_take_noshrink(pt);
    ASSERT_STREQ(nullptr, fptu_check_ro(ro));
    EXPECT_EQ(fptu_eq, fptu_cmp_opaque_iov(ro, 0, opaque_iov(0, n, 23)));
    EXPECT_EQ(fptu_eq, fptu_cmp_opaque_iov(ro, 1, opaque_iov(1, p, 37)));
    EXPECT_EQ(fptu_eq, fptu_cmp_opaque_iov(ro, 2, opaque_iov(2, p, 41)));
    EXPECT_EQ((uint16_t)(n * m), fptu_get_uint16(ro, 3, nullptr));
    EXPECT_EQ((uint32_t)(p * m * m), fptu_get_uint32(ro, 4, nullptr));
    EXPECT_EQ((uint64_t)(n * m * m * m), fptu_get_uint64(ro, 5, nullptr));

    // overwrite field#2 and check all
    SCOPED_TRACE("touch field #2, len " +
                 std::to_string(opaque_iov(2, p, 41).iov_len) + "=>" +
                 std::to_string(opaque_iov(2, n, 41).iov_len) + ", space " +
                 std::to_string(fptu_space4items(pt)) + "/" +
                 std::to_string(fptu_space4data(pt)) + ", junk " +
                 std::to_string(fptu_junkspace(pt)));
    ASSERT_EQ(FPTU_OK, fptu_upsert_opaque_iov(pt, 2, opaque_iov(2, n, 41)));
    ASSERT_STREQ(nullptr, fptu_check(pt));
    ro = fptu_take_noshrink(pt);
    ASSERT_STREQ(nullptr, fptu_check_ro(ro));
    EXPECT_EQ(fptu_eq, fptu_cmp_opaque_iov(ro, 0, opaque_iov(0, n, 23)));
    EXPECT_EQ(fptu_eq, fptu_cmp_opaque_iov(ro, 1, opaque_iov(1, p, 37)));
    EXPECT_EQ(fptu_eq, fptu_cmp_opaque_iov(ro, 2, opaque_iov(2, n, 41)));
    EXPECT_EQ((uint16_t)(n * m), fptu_get_uint16(ro, 3, nullptr));
    EXPECT_EQ((uint32_t)(p * m * m), fptu_get_uint32(ro, 4, nullptr));
    EXPECT_EQ((uint64_t)(n * m * m * m), fptu_get_uint64(ro, 5, nullptr));

    // overwrite field#4 and check all
    SCOPED_TRACE("touch field #4, uint32_t, n " + std::to_string(n));
    ASSERT_EQ(FPTU_OK, fptu_upsert_uint32(pt, 4, n * m * m));
    ASSERT_STREQ(nullptr, fptu_check(pt));
    ro = fptu_take_noshrink(pt);
    ASSERT_STREQ(nullptr, fptu_check_ro(ro));
    EXPECT_EQ(fptu_eq, fptu_cmp_opaque_iov(ro, 0, opaque_iov(0, n, 23)));
    EXPECT_EQ(fptu_eq, fptu_cmp_opaque_iov(ro, 1, opaque_iov(1, p, 37)));
    EXPECT_EQ(fptu_eq, fptu_cmp_opaque_iov(ro, 2, opaque_iov(2, n, 41)));
    EXPECT_EQ((uint16_t)(n * m), fptu_get_uint16(ro, 3, nullptr));
    EXPECT_EQ((uint32_t)(n * m * m), fptu_get_uint32(ro, 4, nullptr));
    EXPECT_EQ((uint64_t)(n * m * m * m), fptu_get_uint64(ro, 5, nullptr));

    // overwrite field#1 and check all
    SCOPED_TRACE("touch field #1, len " +
                 std::to_string(opaque_iov(1, p, 37).iov_len) + "=>" +
                 std::to_string(opaque_iov(1, n, 37).iov_len) + ", space " +
                 std::to_string(fptu_space4items(pt)) + "/" +
                 std::to_string(fptu_space4data(pt)) + ", junk " +
                 std::to_string(fptu_junkspace(pt)));
    ASSERT_EQ(FPTU_OK, fptu_upsert_opaque_iov(pt, 1, opaque_iov(1, n, 37)));
    ASSERT_STREQ(nullptr, fptu_check(pt));
    ro = fptu_take_noshrink(pt);
    ASSERT_STREQ(nullptr, fptu_check_ro(ro));
    EXPECT_EQ(fptu_eq, fptu_cmp_opaque_iov(ro, 0, opaque_iov(0, n, 23)));
    EXPECT_EQ(fptu_eq, fptu_cmp_opaque_iov(ro, 1, opaque_iov(1, n, 37)));
    EXPECT_EQ(fptu_eq, fptu_cmp_opaque_iov(ro, 2, opaque_iov(2, n, 41)));
    EXPECT_EQ((uint16_t)(n * m), fptu_get_uint16(ro, 3, nullptr));
    EXPECT_EQ((uint32_t)(n * m * m), fptu_get_uint32(ro, 4, nullptr));
    EXPECT_EQ((uint64_t)(n * m * m * m), fptu_get_uint64(ro, 5, nullptr));
  }

  ASSERT_STREQ(nullptr, fptu_check(pt));
  free(pt);
}

TEST(Upsert, InsertUpdate) {
  const unsigned items_limit = 29;
  const unsigned bytes_limit = 38 * 4 * 2 + 8;
  fptu_rw *pt = fptu_alloc(items_limit, bytes_limit);
  ASSERT_NE(nullptr, pt);
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(items_limit, fptu_space4items(pt));
  EXPECT_EQ(bytes_limit, fptu_space4data(pt));
  EXPECT_EQ(0, fptu_junkspace(pt));

  unsigned bytes_used = 0;
  EXPECT_EQ(FPTU_OK, fptu_upsert_null(pt, 0));
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(items_limit - 1, fptu_space4items(pt));
  EXPECT_EQ(bytes_limit - bytes_used, fptu_space4data(pt));
  EXPECT_EQ(0, fptu_junkspace(pt));

  // update_xyz() expect no-entry
  EXPECT_EQ(FPTU_ENOFIELD, fptu_update_uint16(pt, 0, 0));
  EXPECT_EQ(FPTU_ENOFIELD, fptu_update_uint32(pt, 0, 0));
  EXPECT_EQ(FPTU_ENOFIELD, fptu_update_int32(pt, 0, 0));
  EXPECT_EQ(FPTU_ENOFIELD, fptu_update_uint64(pt, 0, 0));
  EXPECT_EQ(FPTU_ENOFIELD, fptu_update_int64(pt, 0, 0));
  EXPECT_EQ(FPTU_ENOFIELD, fptu_update_fp64(pt, 0, 0));
  EXPECT_EQ(FPTU_ENOFIELD, fptu_update_fp32(pt, 0, 0));
  EXPECT_EQ(FPTU_ENOFIELD, fptu_update_96(pt, 0, "96"));
  EXPECT_EQ(FPTU_ENOFIELD, fptu_update_128(pt, 0, "128"));
  EXPECT_EQ(FPTU_ENOFIELD, fptu_update_160(pt, 0, "160"));
  EXPECT_EQ(FPTU_ENOFIELD, fptu_update_192(pt, 0, "192"));
  EXPECT_EQ(FPTU_ENOFIELD, fptu_update_256(pt, 0, "256"));
  EXPECT_EQ(FPTU_ENOFIELD, fptu_update_cstr(pt, 0, "cstr"));
  EXPECT_EQ(FPTU_ENOFIELD, fptu_update_opaque(pt, 0, "data", 4));

  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(items_limit - 1, fptu_space4items(pt));
  EXPECT_EQ(bytes_limit - bytes_used, fptu_space4data(pt));
  EXPECT_EQ(0, fptu_junkspace(pt));

  //------------------------------------------------------------------
  fptu_ro ro;

  // insert the first copy of field(0, uint16)
  EXPECT_EQ(FPTU_OK, fptu_insert_uint16(pt, 0, 0x8001));
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(items_limit - 2, fptu_space4items(pt));
  EXPECT_EQ(bytes_limit - bytes_used, fptu_space4data(pt));
  ro = fptu_take_noshrink(pt);
  ASSERT_STREQ(nullptr, fptu_check_ro(ro));
  EXPECT_EQ(0x8001, fptu_get_uint16(ro, 0, nullptr));
  // insert the second copy of field(0, uint16)
  EXPECT_EQ(FPTU_OK, fptu_insert_uint16(pt, 0, 0x8001 + 43));
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(items_limit - 3, fptu_space4items(pt));
  EXPECT_EQ(bytes_limit - bytes_used, fptu_space4data(pt));
  ro = fptu_take_noshrink(pt);
  ASSERT_STREQ(nullptr, fptu_check_ro(ro));
  EXPECT_EQ(0x8001 + 43, fptu_get_uint16(ro, 0, nullptr));
  EXPECT_EQ(0, fptu_junkspace(pt));

  // insert the first copy of field(0, uint32)
  EXPECT_EQ(FPTU_OK, fptu_insert_uint32(pt, 0, 1354824703));
  bytes_used += 4;
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(items_limit - 4, fptu_space4items(pt));
  EXPECT_EQ(bytes_limit - bytes_used, fptu_space4data(pt));
  ro = fptu_take_noshrink(pt);
  ASSERT_STREQ(nullptr, fptu_check_ro(ro));
  EXPECT_EQ(1354824703, fptu_get_uint32(ro, 0, nullptr));
  // insert the second copy of field(0, uint32)
  EXPECT_EQ(FPTU_OK, fptu_insert_uint32(pt, 0, 1354824703 + 43));
  bytes_used += 4;
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(items_limit - 5, fptu_space4items(pt));
  EXPECT_EQ(bytes_limit - bytes_used, fptu_space4data(pt));
  ro = fptu_take_noshrink(pt);
  ASSERT_STREQ(nullptr, fptu_check_ro(ro));
  EXPECT_EQ(1354824703 + 43, fptu_get_uint32(ro, 0, nullptr));
  EXPECT_EQ(0, fptu_junkspace(pt));

  // insert the first copy of field(0, int32)
  EXPECT_EQ(FPTU_OK, fptu_insert_int32(pt, 0, -8782211));
  bytes_used += 4;
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(items_limit - 6, fptu_space4items(pt));
  EXPECT_EQ(bytes_limit - bytes_used, fptu_space4data(pt));
  ro = fptu_take_noshrink(pt);
  ASSERT_STREQ(nullptr, fptu_check_ro(ro));
  EXPECT_EQ(-8782211, fptu_get_int32(ro, 0, nullptr));
  // insert the second copy of field(0, int32)
  EXPECT_EQ(FPTU_OK, fptu_insert_int32(pt, 0, -8782211 + 43));
  bytes_used += 4;
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(items_limit - 7, fptu_space4items(pt));
  EXPECT_EQ(bytes_limit - bytes_used, fptu_space4data(pt));
  ro = fptu_take_noshrink(pt);
  ASSERT_STREQ(nullptr, fptu_check_ro(ro));
  EXPECT_EQ(-8782211 + 43, fptu_get_int32(ro, 0, nullptr));
  EXPECT_EQ(0, fptu_junkspace(pt));

  // insert the first copy of field(0, uint64)
  EXPECT_EQ(FPTU_OK, fptu_insert_uint64(pt, 0, 15047220096467327));
  bytes_used += 8;
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(items_limit - 8, fptu_space4items(pt));
  EXPECT_EQ(bytes_limit - bytes_used, fptu_space4data(pt));
  ro = fptu_take_noshrink(pt);
  ASSERT_STREQ(nullptr, fptu_check_ro(ro));
  EXPECT_EQ(15047220096467327, fptu_get_uint64(ro, 0, nullptr));
  // insert the second copy of field(0, uint64)
  EXPECT_EQ(FPTU_OK, fptu_insert_uint64(pt, 0, 15047220096467327 + 43));
  bytes_used += 8;
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(items_limit - 9, fptu_space4items(pt));
  EXPECT_EQ(bytes_limit - bytes_used, fptu_space4data(pt));
  ro = fptu_take_noshrink(pt);
  ASSERT_STREQ(nullptr, fptu_check_ro(ro));
  EXPECT_EQ(15047220096467327 + 43, fptu_get_uint64(ro, 0, nullptr));
  EXPECT_EQ(0, fptu_junkspace(pt));

  // insert the first copy of field(0, int64)
  EXPECT_EQ(FPTU_OK, fptu_insert_int64(pt, 0, -60585001468255361));
  bytes_used += 8;
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(items_limit - 10, fptu_space4items(pt));
  EXPECT_EQ(bytes_limit - bytes_used, fptu_space4data(pt));
  ro = fptu_take_noshrink(pt);
  ASSERT_STREQ(nullptr, fptu_check_ro(ro));
  EXPECT_EQ(-60585001468255361, fptu_get_int64(ro, 0, nullptr));
  // insert the second copy of field(0, int64)
  EXPECT_EQ(FPTU_OK, fptu_insert_int64(pt, 0, -60585001468255361 + 43));
  bytes_used += 8;
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(items_limit - 11, fptu_space4items(pt));
  EXPECT_EQ(bytes_limit - bytes_used, fptu_space4data(pt));
  ro = fptu_take_noshrink(pt);
  ASSERT_STREQ(nullptr, fptu_check_ro(ro));
  EXPECT_EQ(-60585001468255361 + 43, fptu_get_int64(ro, 0, nullptr));
  EXPECT_EQ(0, fptu_junkspace(pt));

  // insert the first copy of field(0, fp64)
  EXPECT_EQ(FPTU_OK, fptu_insert_fp64(pt, 0, 3.14159265358979323846));
  bytes_used += 8;
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(items_limit - 12, fptu_space4items(pt));
  EXPECT_EQ(bytes_limit - bytes_used, fptu_space4data(pt));
  ro = fptu_take_noshrink(pt);
  ASSERT_STREQ(nullptr, fptu_check_ro(ro));
  EXPECT_EQ(3.1415926535897932, fptu_get_fp64(ro, 0, nullptr));
  // insert the second copy of field(0, fp64)
  EXPECT_EQ(FPTU_OK, fptu_insert_fp64(pt, 0, 3.14159265358979323846 + 43));
  bytes_used += 8;
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(items_limit - 13, fptu_space4items(pt));
  EXPECT_EQ(bytes_limit - bytes_used, fptu_space4data(pt));
  ro = fptu_take_noshrink(pt);
  ASSERT_STREQ(nullptr, fptu_check_ro(ro));
  EXPECT_EQ(3.1415926535897932 + 43, fptu_get_fp64(ro, 0, nullptr));
  EXPECT_EQ(0, fptu_junkspace(pt));

  // insert the first copy of field(0, fp32)
  EXPECT_EQ(FPTU_OK, fptu_insert_fp32(pt, 0, 2.7182818284590452354));
  bytes_used += 4;
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(items_limit - 14, fptu_space4items(pt));
  EXPECT_EQ(bytes_limit - bytes_used, fptu_space4data(pt));
  ro = fptu_take_noshrink(pt);
  ASSERT_STREQ(nullptr, fptu_check_ro(ro));
  EXPECT_EQ((float)2.7182818284590452354, fptu_get_fp32(ro, 0, nullptr));
  // insert the second copy of field(0, fp32)
  EXPECT_EQ(FPTU_OK, fptu_insert_fp32(pt, 0, 2.7182818284590452354 + 43));
  bytes_used += 4;
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(items_limit - 15, fptu_space4items(pt));
  EXPECT_EQ(bytes_limit - bytes_used, fptu_space4data(pt));
  ro = fptu_take_noshrink(pt);
  ASSERT_STREQ(nullptr, fptu_check_ro(ro));
  EXPECT_EQ((float)2.7182818284590452354 + 43, fptu_get_fp32(ro, 0, nullptr));
  EXPECT_EQ(0, fptu_junkspace(pt));

  static const uint8_t *_96 = pattern + 42;
  static const uint8_t *_128 = _96 + 12;
  static const uint8_t *_160 = _128 + 16;
  static const uint8_t *_192 = _160 + 20;
  static const uint8_t *_256 = _192 + 24;
  ASSERT_LT(32, pattern + sizeof(pattern) - 43 - _256);

  // insert the first copy of field(0, _96)
  EXPECT_EQ(FPTU_OK, fptu_insert_96(pt, 0, _96));
  bytes_used += 96 / 8;
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(items_limit - 16, fptu_space4items(pt));
  EXPECT_EQ(bytes_limit - bytes_used, fptu_space4data(pt));
  ro = fptu_take_noshrink(pt);
  ASSERT_STREQ(nullptr, fptu_check_ro(ro));
  EXPECT_EQ(fptu_eq, fptu_cmp_96(ro, 0, _96));
  EXPECT_EQ(0, memcmp(_96, fptu_get_96(ro, 0, nullptr), 96 / 8));
  // insert the second copy of field(0, _96)
  EXPECT_EQ(FPTU_OK, fptu_insert_96(pt, 0, _96 + 43));
  bytes_used += 96 / 8;
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(items_limit - 17, fptu_space4items(pt));
  EXPECT_EQ(bytes_limit - bytes_used, fptu_space4data(pt));
  ro = fptu_take_noshrink(pt);
  ASSERT_STREQ(nullptr, fptu_check_ro(ro));
  EXPECT_EQ(fptu_eq, fptu_cmp_96(ro, 0, _96 + 43));
  EXPECT_EQ(0, memcmp(_96 + 43, fptu_get_96(ro, 0, nullptr), 96 / 8));
  EXPECT_EQ(0, fptu_junkspace(pt));

  // insert the first copy of field(0, _128)
  EXPECT_EQ(FPTU_OK, fptu_insert_128(pt, 0, _128));
  bytes_used += 128 / 8;
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(items_limit - 18, fptu_space4items(pt));
  EXPECT_EQ(bytes_limit - bytes_used, fptu_space4data(pt));
  ro = fptu_take_noshrink(pt);
  ASSERT_STREQ(nullptr, fptu_check_ro(ro));
  EXPECT_EQ(fptu_eq, fptu_cmp_128(ro, 0, _128));
  EXPECT_EQ(0, memcmp(_128, fptu_get_128(ro, 0, nullptr), 128 / 8));
  // insert the second copy of field(0, _128)
  EXPECT_EQ(FPTU_OK, fptu_insert_128(pt, 0, _128 + 43));
  bytes_used += 128 / 8;
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(items_limit - 19, fptu_space4items(pt));
  EXPECT_EQ(bytes_limit - bytes_used, fptu_space4data(pt));
  ro = fptu_take_noshrink(pt);
  ASSERT_STREQ(nullptr, fptu_check_ro(ro));
  EXPECT_EQ(fptu_eq, fptu_cmp_128(ro, 0, _128 + 43));
  EXPECT_EQ(0, memcmp(_128 + 43, fptu_get_128(ro, 0, nullptr), 128 / 8));
  EXPECT_EQ(0, fptu_junkspace(pt));

  // insert the first copy of field(0, _160)
  EXPECT_EQ(FPTU_OK, fptu_insert_160(pt, 0, _160));
  bytes_used += 160 / 8;
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(items_limit - 20, fptu_space4items(pt));
  EXPECT_EQ(bytes_limit - bytes_used, fptu_space4data(pt));
  ro = fptu_take_noshrink(pt);
  ASSERT_STREQ(nullptr, fptu_check_ro(ro));
  EXPECT_EQ(fptu_eq, fptu_cmp_160(ro, 0, _160));
  EXPECT_EQ(0, memcmp(_160, fptu_get_160(ro, 0, nullptr), 160 / 8));
  // insert the second copy of field(0, _160)
  EXPECT_EQ(FPTU_OK, fptu_insert_160(pt, 0, _160 + 43));
  bytes_used += 160 / 8;
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(items_limit - 21, fptu_space4items(pt));
  EXPECT_EQ(bytes_limit - bytes_used, fptu_space4data(pt));
  ro = fptu_take_noshrink(pt);
  ASSERT_STREQ(nullptr, fptu_check_ro(ro));
  EXPECT_EQ(fptu_eq, fptu_cmp_160(ro, 0, _160 + 43));
  EXPECT_EQ(0, memcmp(_160 + 43, fptu_get_160(ro, 0, nullptr), 160 / 8));
  EXPECT_EQ(0, fptu_junkspace(pt));

  // insert the first copy of field(0, _192)
  EXPECT_EQ(FPTU_OK, fptu_insert_192(pt, 0, _192));
  bytes_used += 192 / 8;
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(items_limit - 22, fptu_space4items(pt));
  EXPECT_EQ(bytes_limit - bytes_used, fptu_space4data(pt));
  ro = fptu_take_noshrink(pt);
  ASSERT_STREQ(nullptr, fptu_check_ro(ro));
  EXPECT_EQ(fptu_eq, fptu_cmp_192(ro, 0, _192));
  EXPECT_EQ(0, memcmp(_192, fptu_get_192(ro, 0, nullptr), 192 / 8));
  // insert the sexond copy of field(0, _192)
  EXPECT_EQ(FPTU_OK, fptu_insert_192(pt, 0, _192 + 43));
  bytes_used += 192 / 8;
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(items_limit - 23, fptu_space4items(pt));
  EXPECT_EQ(bytes_limit - bytes_used, fptu_space4data(pt));
  ro = fptu_take_noshrink(pt);
  ASSERT_STREQ(nullptr, fptu_check_ro(ro));
  EXPECT_EQ(fptu_eq, fptu_cmp_192(ro, 0, _192 + 43));
  EXPECT_EQ(0, memcmp(_192 + 43, fptu_get_192(ro, 0, nullptr), 192 / 8));
  EXPECT_EQ(0, fptu_junkspace(pt));

  // insert the first copy of field(0, _256)
  EXPECT_EQ(FPTU_OK, fptu_insert_256(pt, 0, _256));
  bytes_used += 256 / 8;
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(items_limit - 24, fptu_space4items(pt));
  EXPECT_EQ(bytes_limit - bytes_used, fptu_space4data(pt));
  ro = fptu_take_noshrink(pt);
  ASSERT_STREQ(nullptr, fptu_check_ro(ro));
  EXPECT_EQ(fptu_eq, fptu_cmp_256(ro, 0, _256));
  EXPECT_EQ(0, memcmp(_256, fptu_get_256(ro, 0, nullptr), 256 / 8));
  // insert the second copy of field(0, _256)
  EXPECT_EQ(FPTU_OK, fptu_insert_256(pt, 0, _256 + 43));
  bytes_used += 256 / 8;
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(items_limit - 25, fptu_space4items(pt));
  EXPECT_EQ(bytes_limit - bytes_used, fptu_space4data(pt));
  ro = fptu_take_noshrink(pt);
  ASSERT_STREQ(nullptr, fptu_check_ro(ro));
  EXPECT_EQ(fptu_eq, fptu_cmp_256(ro, 0, _256 + 43));
  EXPECT_EQ(0, memcmp(_256 + 43, fptu_get_256(ro, 0, nullptr), 256 / 8));
  EXPECT_EQ(0, fptu_junkspace(pt));

  // insert the first copy of field(0, c-string)
  EXPECT_EQ(FPTU_OK, fptu_insert_cstr(pt, 0, "abc"));
  bytes_used += 4;
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(items_limit - 26, fptu_space4items(pt));
  EXPECT_EQ(bytes_limit - bytes_used, fptu_space4data(pt));
  ro = fptu_take_noshrink(pt);
  ASSERT_STREQ(nullptr, fptu_check_ro(ro));
  EXPECT_STREQ("abc", fptu_get_cstr(ro, 0, nullptr));
  // insert the second copy of field(0, c-string)
  EXPECT_EQ(FPTU_OK, fptu_insert_cstr(pt, 0, "cstr"));
  bytes_used += 8;
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(items_limit - 27, fptu_space4items(pt));
  EXPECT_EQ(bytes_limit - bytes_used, fptu_space4data(pt));
  ro = fptu_take_noshrink(pt);
  ASSERT_STREQ(nullptr, fptu_check_ro(ro));
  EXPECT_STREQ("cstr", fptu_get_cstr(ro, 0, nullptr));
  EXPECT_EQ(0, fptu_junkspace(pt));

  // insert the first copy of field(0, opaque)
  EXPECT_EQ(FPTU_OK, fptu_insert_opaque(pt, 0, "data", 4));
  bytes_used += 4 + 4;
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(items_limit - 28, fptu_space4items(pt));
  EXPECT_EQ(bytes_limit - bytes_used, fptu_space4data(pt));
  ro = fptu_take_noshrink(pt);
  ASSERT_STREQ(nullptr, fptu_check_ro(ro));
  // insert the second copy of field(0, opaque)
  EXPECT_EQ(FPTU_OK, fptu_insert_opaque(pt, 0, "bananan", 8));
  bytes_used += 4 + 8;
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(items_limit - 29, fptu_space4items(pt));
  EXPECT_EQ(bytes_limit - bytes_used, fptu_space4data(pt));
  ro = fptu_take_noshrink(pt);
  ASSERT_STREQ(nullptr, fptu_check_ro(ro));

  //------------------------------------------------------------------
  // update present column, expect no error
  EXPECT_EQ(bytes_limit, bytes_used);
  EXPECT_EQ(0, fptu_space4items(pt));
  EXPECT_EQ(FPTU_OK, fptu_upsert_null(pt, 0));

  EXPECT_EQ(0, fptu_space4items(pt));
  EXPECT_EQ(bytes_limit - bytes_used, fptu_space4data(pt));
  EXPECT_EQ(0, fptu_junkspace(pt));
  EXPECT_EQ(FPTU_ENOSPACE, fptu_upsert_null(pt, 33));

  EXPECT_EQ(0, fptu_space4items(pt));
  EXPECT_EQ(0, fptu_space4data(pt));
  EXPECT_EQ(0, fptu_junkspace(pt));
  ASSERT_STREQ(nullptr, fptu_check(pt));

  ro = fptu_take_noshrink(pt);
  ASSERT_STREQ(nullptr, fptu_check_ro(ro));
  EXPECT_EQ((items_limit + 1) * 4 + bytes_limit, ro.total_bytes);

  // one more check secondary values
  EXPECT_EQ(0x8001 + 43, fptu_get_uint16(ro, 0, nullptr));
  EXPECT_EQ(1354824703 + 43, fptu_get_uint32(ro, 0, nullptr));
  EXPECT_EQ(-8782211 + 43, fptu_get_int32(ro, 0, nullptr));
  EXPECT_EQ(15047220096467327 + 43, fptu_get_uint64(ro, 0, nullptr));
  EXPECT_EQ(-60585001468255361 + 43, fptu_get_int64(ro, 0, nullptr));
  EXPECT_EQ(3.1415926535897932 + 43, fptu_get_fp64(ro, 0, nullptr));
  EXPECT_EQ((float)2.7182818284590452354 + 43, fptu_get_fp32(ro, 0, nullptr));
  EXPECT_STREQ("cstr", fptu_get_cstr(ro, 0, nullptr));
  EXPECT_EQ(fptu_eq, fptu_cmp_96(ro, 0, _96 + 43));
  EXPECT_EQ(fptu_eq, fptu_cmp_128(ro, 0, _128 + 43));
  EXPECT_EQ(fptu_eq, fptu_cmp_160(ro, 0, _160 + 43));
  EXPECT_EQ(fptu_eq, fptu_cmp_192(ro, 0, _192 + 43));
  EXPECT_EQ(fptu_eq, fptu_cmp_256(ro, 0, _256 + 43));

  EXPECT_EQ(0, fptu_space4items(pt));
  EXPECT_EQ(0, fptu_space4data(pt));
  EXPECT_EQ(0, fptu_junkspace(pt));
  ASSERT_STREQ(nullptr, fptu_check(pt));

  //------------------------------------------------------------------

  // update first copy of the field and check value
  unsigned junk = 0;
  EXPECT_EQ(FPTU_OK, fptu_update_uint16(pt, 0, 0x8001 - 42));
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(0, fptu_space4items(pt));
  EXPECT_EQ(0, fptu_space4data(pt));
  EXPECT_EQ(junk, fptu_junkspace(pt));
  EXPECT_EQ(0x8001 - 42, fptu_get_uint16(ro, 0, nullptr));

  // remove the first copy of field and check value of the next copy
  EXPECT_EQ(1, fptu_erase(pt, 0, fptu_uint16));
  junk += 4;
  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(0, fptu_space4items(pt));
  EXPECT_EQ(0, fptu_space4data(pt));
  EXPECT_EQ(junk, fptu_junkspace(pt));
  ro = fptu_take_noshrink(pt);
  ASSERT_STREQ(nullptr, fptu_check_ro(ro));
  EXPECT_EQ(0x8001, fptu_get_uint16(ro, 0, nullptr));

  // remove the second copy
  EXPECT_EQ(1, fptu_erase(pt, 0, fptu_uint16));
  junk += 4;
  EXPECT_EQ(0, fptu_space4items(pt));
  EXPECT_EQ(0, fptu_space4data(pt));
  EXPECT_EQ(junk, fptu_junkspace(pt));

  // try erase one more
  EXPECT_EQ(0, fptu_erase(pt, 0, fptu_uint16));
  EXPECT_EQ(FPTU_ENOFIELD, fptu_update_uint16(pt, 0, 0));
  EXPECT_EQ(0, fptu_space4items(pt));
  EXPECT_EQ(0, fptu_space4data(pt));
  EXPECT_EQ(junk, fptu_junkspace(pt));
  EXPECT_EQ(nullptr, fptu_lookup(pt, 0, fptu_uint16));

  ASSERT_STREQ(nullptr, fptu_check(pt));
  ro = fptu_take_noshrink(pt);
  ASSERT_STREQ(nullptr, fptu_check_ro(ro));
  EXPECT_EQ(nullptr, fptu_lookup_ro(ro, 0, fptu_uint16));

  //------------------------------------------------------------------

  // update secondary values and check ones
  EXPECT_EQ(FPTU_OK, fptu_update_uint32(pt, 0, 1354824703 - 42));
  EXPECT_EQ(FPTU_OK, fptu_update_int32(pt, 0, -8782211 - 42));
  EXPECT_EQ(FPTU_OK, fptu_update_uint64(pt, 0, 15047220096467327 - 42));
  EXPECT_EQ(FPTU_OK, fptu_update_int64(pt, 0, -64700360770547893));
  EXPECT_EQ(FPTU_OK, fptu_update_fp64(pt, 0, 3.14159265358979323846 - 42));
  EXPECT_EQ(FPTU_OK, fptu_update_fp32(pt, 0, 2.7182818284590452354 - 42));
  EXPECT_EQ(FPTU_OK, fptu_update_96(pt, 0, _96 - 42));
  EXPECT_EQ(FPTU_OK, fptu_update_128(pt, 0, _128 - 42));
  EXPECT_EQ(FPTU_OK, fptu_update_160(pt, 0, _160 - 42));
  EXPECT_EQ(FPTU_OK, fptu_update_192(pt, 0, _192 - 42));
  EXPECT_EQ(FPTU_OK, fptu_update_256(pt, 0, _256 - 42));
  EXPECT_EQ(FPTU_OK, fptu_update_cstr(pt, 0, "xyz_"));
  EXPECT_EQ(FPTU_OK, fptu_update_opaque(pt, 0, "1234567", 8));

  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(0, fptu_space4items(pt));
  EXPECT_EQ(0, fptu_space4data(pt));
  EXPECT_EQ(junk, fptu_junkspace(pt));
  ro = fptu_take_noshrink(pt);
  ASSERT_STREQ(nullptr, fptu_check_ro(ro));
  EXPECT_EQ(nullptr, fptu_lookup_ro(ro, 0, fptu_uint16));

  EXPECT_EQ(1354824703 - 42, fptu_get_uint32(ro, 0, nullptr));
  EXPECT_EQ(-8782211 - 42, fptu_get_int32(ro, 0, nullptr));
  EXPECT_EQ(15047220096467327 - 42, fptu_get_uint64(ro, 0, nullptr));
  EXPECT_EQ(-64700360770547893, fptu_get_int64(ro, 0, nullptr));
  EXPECT_EQ(3.1415926535897932 - 42, fptu_get_fp64(ro, 0, nullptr));
  EXPECT_EQ((float)2.7182818284590452354 - 42, fptu_get_fp32(ro, 0, nullptr));
  EXPECT_STREQ("xyz_", fptu_get_cstr(ro, 0, nullptr));
  iovec io = fptu_get_opaque(ro, 0, nullptr);
  EXPECT_EQ(8, io.iov_len);
  ASSERT_NE(nullptr, io.iov_base);
  EXPECT_STREQ("1234567", (const char *)io.iov_base);
  EXPECT_EQ(fptu_eq, fptu_cmp_96(ro, 0, _96 - 42));
  EXPECT_EQ(fptu_eq, fptu_cmp_128(ro, 0, _128 - 42));
  EXPECT_EQ(fptu_eq, fptu_cmp_160(ro, 0, _160 - 42));
  EXPECT_EQ(fptu_eq, fptu_cmp_192(ro, 0, _192 - 42));
  EXPECT_EQ(fptu_eq, fptu_cmp_256(ro, 0, _256 - 42));

  //------------------------------------------------------------------

  // remove secondary values and check
  EXPECT_EQ(1, fptu_erase(pt, 0, fptu_uint32));
  junk += 4 + 4;
  EXPECT_EQ(junk, fptu_junkspace(pt));

  EXPECT_EQ(1, fptu_erase(pt, 0, fptu_uint64));
  junk += 4 + 8;
  EXPECT_EQ(junk, fptu_junkspace(pt));

  EXPECT_EQ(1, fptu_erase(pt, 0, fptu_int64));
  junk += 4 + 8;
  EXPECT_EQ(junk, fptu_junkspace(pt));

  EXPECT_EQ(1, fptu_erase(pt, 0, fptu_int32));
  junk += 4 + 4;
  EXPECT_EQ(junk, fptu_junkspace(pt));

  EXPECT_EQ(1, fptu_erase(pt, 0, fptu_fp32));
  junk += 4 + 4;
  EXPECT_EQ(junk, fptu_junkspace(pt));

  EXPECT_EQ(1, fptu_erase(pt, 0, fptu_fp64));
  junk += 4 + 8;
  EXPECT_EQ(junk, fptu_junkspace(pt));

  EXPECT_EQ(1, fptu_erase(pt, 0, fptu_cstr));
  junk += 4 + 8;
  EXPECT_EQ(junk, fptu_junkspace(pt));

  EXPECT_EQ(1, fptu_erase(pt, 0, fptu_opaque));
  bytes_used -= 12;
  EXPECT_EQ(bytes_limit - bytes_used, fptu_space4data(pt));
  EXPECT_EQ(1, fptu_space4items(pt));
  EXPECT_EQ(junk, fptu_junkspace(pt));

  EXPECT_EQ(1, fptu_erase(pt, 0, fptu_96));
  junk += 4 + 12;
  EXPECT_EQ(junk, fptu_junkspace(pt));

  EXPECT_EQ(1, fptu_erase(pt, 0, fptu_128));
  junk += 4 + 16;
  EXPECT_EQ(junk, fptu_junkspace(pt));

  EXPECT_EQ(1, fptu_erase(pt, 0, fptu_160));
  junk += 4 + 20;
  EXPECT_EQ(junk, fptu_junkspace(pt));

  EXPECT_EQ(1, fptu_erase(pt, 0, fptu_192));
  junk += 4 + 24;
  EXPECT_EQ(junk, fptu_junkspace(pt));

  EXPECT_EQ(1, fptu_erase(pt, 0, fptu_256));
  junk += 4 + 32;
  EXPECT_EQ(junk, fptu_junkspace(pt));

  ASSERT_STREQ(nullptr, fptu_check(pt));
  EXPECT_EQ(1, fptu_space4items(pt));
  EXPECT_EQ(bytes_limit - bytes_used, fptu_space4data(pt));
  EXPECT_EQ(junk, fptu_junkspace(pt));
  ro = fptu_take_noshrink(pt);
  ASSERT_STREQ(nullptr, fptu_check_ro(ro));
  EXPECT_EQ(nullptr, fptu_lookup_ro(ro, 0, fptu_uint16));

  // secondary removed, expect first values
  EXPECT_EQ(1354824703, fptu_get_uint32(ro, 0, nullptr));
  EXPECT_EQ(-8782211, fptu_get_int32(ro, 0, nullptr));
  EXPECT_EQ(15047220096467327, fptu_get_uint64(ro, 0, nullptr));
  EXPECT_EQ(-60585001468255361, fptu_get_int64(ro, 0, nullptr));
  EXPECT_EQ(3.1415926535897932, fptu_get_fp64(ro, 0, nullptr));
  EXPECT_EQ((float)2.7182818284590452354, fptu_get_fp32(ro, 0, nullptr));
  EXPECT_STREQ("abc", fptu_get_cstr(ro, 0, nullptr));
  io = fptu_get_opaque(ro, 0, nullptr);
  EXPECT_EQ(4, io.iov_len);
  ASSERT_NE(nullptr, io.iov_base);
  EXPECT_EQ(0, strncmp("data", (const char *)io.iov_base, 4));
  EXPECT_EQ(fptu_eq, fptu_cmp_96(ro, 0, _96));
  EXPECT_EQ(fptu_eq, fptu_cmp_128(ro, 0, _128));
  EXPECT_EQ(fptu_eq, fptu_cmp_160(ro, 0, _160));
  EXPECT_EQ(fptu_eq, fptu_cmp_192(ro, 0, _192));
  EXPECT_EQ(fptu_eq, fptu_cmp_256(ro, 0, _256));

  //------------------------------------------------------------------
  // remove all first values

  EXPECT_EQ(14, fptu_erase(pt, 0, fptu_any));
  ASSERT_STREQ(nullptr, fptu_check(pt));
  ro = fptu_take_noshrink(pt);
  ASSERT_STREQ(nullptr, fptu_check_ro(ro));
  EXPECT_EQ(nullptr, fptu_lookup_ro(ro, 0, fptu_uint16));
  EXPECT_EQ(0, fptu_erase(pt, 0, fptu_null));

  // update_xyz() expect no-entry
  EXPECT_EQ(FPTU_ENOFIELD, fptu_update_uint16(pt, 0, 0));
  EXPECT_EQ(FPTU_ENOFIELD, fptu_update_uint32(pt, 0, 0));
  EXPECT_EQ(FPTU_ENOFIELD, fptu_update_int32(pt, 0, 0));
  EXPECT_EQ(FPTU_ENOFIELD, fptu_update_uint64(pt, 0, 0));
  EXPECT_EQ(FPTU_ENOFIELD, fptu_update_int64(pt, 0, 0));
  EXPECT_EQ(FPTU_ENOFIELD, fptu_update_fp64(pt, 0, 0));
  EXPECT_EQ(FPTU_ENOFIELD, fptu_update_fp32(pt, 0, 0));
  EXPECT_EQ(FPTU_ENOFIELD, fptu_update_96(pt, 0, "96"));
  EXPECT_EQ(FPTU_ENOFIELD, fptu_update_128(pt, 0, "128"));
  EXPECT_EQ(FPTU_ENOFIELD, fptu_update_160(pt, 0, "160"));
  EXPECT_EQ(FPTU_ENOFIELD, fptu_update_192(pt, 0, "192"));
  EXPECT_EQ(FPTU_ENOFIELD, fptu_update_256(pt, 0, "256"));
  EXPECT_EQ(FPTU_ENOFIELD, fptu_update_cstr(pt, 0, "cstr"));
  EXPECT_EQ(FPTU_ENOFIELD, fptu_update_opaque(pt, 0, "data", 4));
  EXPECT_EQ(0, fptu_erase(pt, 0, fptu_any));

  //------------------------------------------------------------------

  EXPECT_EQ(items_limit, fptu_space4items(pt));
  EXPECT_EQ(bytes_limit, fptu_space4data(pt));
  EXPECT_EQ(0, fptu_junkspace(pt));
  ASSERT_STREQ(nullptr, fptu_check(pt));
  free(pt);
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

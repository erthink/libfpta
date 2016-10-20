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

TEST(Upsert, InvalidColumn) {
    char space_exactly_noitems[sizeof(fpt_rw)];
    fpt_rw *pt = fpt_init(space_exactly_noitems, sizeof(space_exactly_noitems), 0);
    ASSERT_NE(nullptr, pt);
    ASSERT_STREQ(nullptr, fpt_check(pt));

    // column #1023 is a stub, expect EINVAL
    EXPECT_EQ(fpt_einval, fpt_upsert_null(pt, 1023));
    EXPECT_EQ(fpt_einval, fpt_upsert_uint16(pt, 1023, 0));
    EXPECT_EQ(fpt_einval, fpt_upsert_uint32(pt, 1023, 0));
    EXPECT_EQ(fpt_einval, fpt_upsert_int32(pt, 1023, 0));
    EXPECT_EQ(fpt_einval, fpt_upsert_uint64(pt, 1023, 0));
    EXPECT_EQ(fpt_einval, fpt_upsert_int64(pt, 1023, 0));
    EXPECT_EQ(fpt_einval, fpt_upsert_fp64(pt, 1023, 0));
    EXPECT_EQ(fpt_einval, fpt_upsert_fp32(pt, 1023, 0));

    EXPECT_EQ(fpt_einval, fpt_upsert_96(pt, 1023, "96"));
    EXPECT_EQ(fpt_einval, fpt_upsert_128(pt, 1023, "128"));
    EXPECT_EQ(fpt_einval, fpt_upsert_160(pt, 1023, "160"));
    EXPECT_EQ(fpt_einval, fpt_upsert_192(pt, 1023, "192"));
    EXPECT_EQ(fpt_einval, fpt_upsert_256(pt, 1023, "256"));
    EXPECT_EQ(fpt_einval, fpt_upsert_cstr(pt, 1023, "cstr"));
    EXPECT_EQ(fpt_einval, fpt_upsert_opaque(pt, 1023, "data", 4));

    EXPECT_EQ(0, fpt_space4items(pt));
    EXPECT_EQ(0, fpt_space4data(pt));
    EXPECT_EQ(0, fpt_junkspace(pt));
    EXPECT_STREQ(nullptr, fpt_check(pt));
}

TEST(Upsert, ZeroSpace) {
    char space_exactly_noitems[sizeof(fpt_rw)];
    fpt_rw *pt = fpt_init(space_exactly_noitems, sizeof(space_exactly_noitems), 0);
    ASSERT_NE(nullptr, pt);
    ASSERT_STREQ(nullptr, fpt_check(pt));
    EXPECT_EQ(0, fpt_space4items(pt));
    EXPECT_EQ(0, fpt_space4data(pt));
    EXPECT_EQ(0, fpt_junkspace(pt));

    // NOSPACE for a void value
    EXPECT_EQ(fpt_enospc, fpt_upsert_null(pt, fpt_max_cols));
    EXPECT_EQ(fpt_enospc, fpt_upsert_uint16(pt, 0, 0));
    EXPECT_EQ(fpt_enospc, fpt_upsert_uint32(pt, 1, 0));
    EXPECT_EQ(fpt_enospc, fpt_upsert_int32(pt, 42, 0));
    EXPECT_EQ(fpt_enospc, fpt_upsert_uint64(pt, 111, 0));
    EXPECT_EQ(fpt_enospc, fpt_upsert_int64(pt, fpt_max_cols / 3, 0));
    EXPECT_EQ(fpt_enospc, fpt_upsert_fp64(pt, fpt_max_cols - 3, 0));
    EXPECT_EQ(fpt_enospc, fpt_upsert_fp32(pt, fpt_max_cols - 4, 0));

    EXPECT_EQ(fpt_enospc, fpt_upsert_96(pt, fpt_max_cols / 2, "96"));
    EXPECT_EQ(fpt_enospc, fpt_upsert_128(pt, 257, "128"));
    EXPECT_EQ(fpt_enospc, fpt_upsert_160(pt, 7, "160"));
    EXPECT_EQ(fpt_enospc, fpt_upsert_192(pt, 8, "192"));
    EXPECT_EQ(fpt_enospc, fpt_upsert_256(pt, fpt_max_cols - 2, "256"));
    EXPECT_EQ(fpt_enospc, fpt_upsert_cstr(pt, fpt_max_cols - 1, "cstr"));
    EXPECT_EQ(fpt_enospc, fpt_upsert_opaque(pt, fpt_max_cols, "data", 4));

    EXPECT_STREQ(nullptr, fpt_check(pt));
    EXPECT_EQ(0, fpt_space4items(pt));
    EXPECT_EQ(0, fpt_space4data(pt));
    EXPECT_EQ(0, fpt_junkspace(pt));
}

static const uint8_t pattern[256] = {
    177,85,188,146,222,148,10,7,241,57,199,43,106,240,124,237,220,230,197,76,
    116,153,205,221,28,2,31,233,58,60,159,228,109,20,66,214,111,15,18,44,208,
    72,249,210,113,212,165,1,225,174,164,204,45,130,82,80,99,138,48,167,78,14,
    149,207,103,178,223,25,163,118,139,122,37,119,182,26,4,236,96,64,196,75,
    29,95,252,33,185,87,110,202,200,125,93,55,84,105,89,215,161,211,154,86,39,
    145,77,190,147,136,108,132,107,172,229,83,187,226,160,155,242,133,23,8,6,
    151,184,195,17,16,140,191,131,156,61,239,127,181,94,176,27,81,235,141,69,
    47,170,74,168,88,56,193,68,209,104,143,52,53,46,115,158,100,243,213,247,
    34,62,238,203,232,92,49,54,42,245,171,227,123,24,186,63,112,135,183,254,5,
    198,13,216,73,219,173,255,121,79,137,150,12,162,41,206,217,231,120,59,128,
    101,51,201,253,35,194,166,70,71,11,189,50,234,218,30,0,134,32,152,90,19,
    224,3,250,98,169,102,38,142,91,117,180,175,246,9,129,114,244,67,157,21,
    144,126,40,179,36,192,248,22,65,251,97
};

TEST(Upsert, Base) {
    fpt_rw* pt = fpt_alloc(15, 38*4);
    ASSERT_NE(nullptr, pt);
    ASSERT_STREQ(nullptr, fpt_check(pt));
    EXPECT_EQ(15, fpt_space4items(pt));
    EXPECT_EQ(38*4, fpt_space4data(pt));
    EXPECT_EQ(0, fpt_junkspace(pt));

    EXPECT_EQ(fpt_ok, fpt_upsert_null(pt, fpt_max_cols));
    ASSERT_STREQ(nullptr, fpt_check(pt));
    EXPECT_EQ(14, fpt_space4items(pt));
    EXPECT_EQ(38*4, fpt_space4data(pt));

    EXPECT_EQ(fpt_ok, fpt_upsert_uint16(pt, 0, 0x8001));
    ASSERT_STREQ(nullptr, fpt_check(pt));
    EXPECT_EQ(13, fpt_space4items(pt));
    EXPECT_EQ(38*4, fpt_space4data(pt));

    EXPECT_EQ(fpt_ok, fpt_upsert_uint32(pt, 1, 1354824703));
    ASSERT_STREQ(nullptr, fpt_check(pt));
    EXPECT_EQ(12, fpt_space4items(pt));
    EXPECT_EQ(38*4 - 4, fpt_space4data(pt));

    EXPECT_EQ(fpt_ok, fpt_upsert_int32(pt, 42, -8782211));
    ASSERT_STREQ(nullptr, fpt_check(pt));
    EXPECT_EQ(11, fpt_space4items(pt));
    EXPECT_EQ(38*4 - 8, fpt_space4data(pt));

    EXPECT_EQ(fpt_ok, fpt_upsert_uint64(pt, 111, 15047220096467327));
    ASSERT_STREQ(nullptr, fpt_check(pt));
    EXPECT_EQ(10, fpt_space4items(pt));
    EXPECT_EQ(38*4 - 16, fpt_space4data(pt));

    EXPECT_EQ(fpt_ok, fpt_upsert_int64(pt, fpt_max_cols / 3, -60585001468255361));
    ASSERT_STREQ(nullptr, fpt_check(pt));
    EXPECT_EQ(9, fpt_space4items(pt));
    EXPECT_EQ(38*4 - 24, fpt_space4data(pt));

    EXPECT_EQ(fpt_ok, fpt_upsert_fp64(pt, fpt_max_cols - 3, 3.14159265358979323846));
    ASSERT_STREQ(nullptr, fpt_check(pt));
    EXPECT_EQ(8, fpt_space4items(pt));
    EXPECT_EQ(38*4 - 32, fpt_space4data(pt));

    EXPECT_EQ(fpt_ok, fpt_upsert_fp32(pt, fpt_max_cols - 4, 2.7182818284590452354));
    ASSERT_STREQ(nullptr, fpt_check(pt));
    EXPECT_EQ(7, fpt_space4items(pt));
    EXPECT_EQ(38*4 - 36, fpt_space4data(pt));

    static const uint8_t* _96 = pattern;
    static const uint8_t* _128 = _96 + 12;
    static const uint8_t* _160 = _128 + 16;
    static const uint8_t* _192 = _160 + 20;
    static const uint8_t* _256 = _192 + 24;
    ASSERT_LT(32, pattern + sizeof(pattern) - _256);

    EXPECT_EQ(fpt_ok, fpt_upsert_96(pt, fpt_max_cols / 2, _96));
    ASSERT_STREQ(nullptr, fpt_check(pt));
    EXPECT_EQ(6, fpt_space4items(pt));
    EXPECT_EQ(38*4 - 48, fpt_space4data(pt));

    EXPECT_EQ(fpt_ok, fpt_upsert_128(pt, 257, _128));
    ASSERT_STREQ(nullptr, fpt_check(pt));
    EXPECT_EQ(5, fpt_space4items(pt));
    EXPECT_EQ(38*4 - 64, fpt_space4data(pt));

    EXPECT_EQ(fpt_ok, fpt_upsert_160(pt, 7, _160));
    ASSERT_STREQ(nullptr, fpt_check(pt));
    EXPECT_EQ(4, fpt_space4items(pt));
    EXPECT_EQ(38*4 - 84, fpt_space4data(pt));

    EXPECT_EQ(fpt_ok, fpt_upsert_192(pt, 8, _192));
    ASSERT_STREQ(nullptr, fpt_check(pt));
    EXPECT_EQ(3, fpt_space4items(pt));
    EXPECT_EQ(38*4 - 108, fpt_space4data(pt));

    EXPECT_EQ(fpt_ok, fpt_upsert_256(pt, fpt_max_cols - 2, _256));
    ASSERT_STREQ(nullptr, fpt_check(pt));
    EXPECT_EQ(2, fpt_space4items(pt));
    EXPECT_EQ(38*4 - 140, fpt_space4data(pt));

    EXPECT_EQ(fpt_ok, fpt_upsert_cstr(pt, fpt_max_cols - 1, "abc"));
    ASSERT_STREQ(nullptr, fpt_check(pt));
    EXPECT_EQ(1, fpt_space4items(pt));
    EXPECT_EQ(8, fpt_space4data(pt));

    EXPECT_EQ(fpt_ok, fpt_upsert_cstr(pt, 42, "cstr"));
    ASSERT_STREQ(nullptr, fpt_check(pt));
    EXPECT_EQ(0, fpt_space4items(pt));
    EXPECT_EQ(0, fpt_space4data(pt));
    EXPECT_EQ(0, fpt_junkspace(pt));

    // update present column, expect no error
    EXPECT_EQ(fpt_ok, fpt_upsert_null(pt, fpt_max_cols));

    EXPECT_EQ(0, fpt_space4items(pt));
    EXPECT_EQ(0, fpt_space4data(pt));
    EXPECT_EQ(0, fpt_junkspace(pt));
    EXPECT_EQ(fpt_enospc, fpt_upsert_null(pt, 33));

    EXPECT_EQ(0, fpt_space4items(pt));
    EXPECT_EQ(0, fpt_space4data(pt));
    EXPECT_EQ(0, fpt_junkspace(pt));
    ASSERT_STREQ(nullptr, fpt_check(pt));

    fpt_ro io = fpt_take_noshrink(pt);
    ASSERT_STREQ(nullptr, fpt_check_ro(io));
    EXPECT_EQ((1+15+38)*4, io.total_bytes);

    EXPECT_EQ(0x8001, fpt_get_uint16(io, 0, nullptr));
    EXPECT_EQ(1354824703, fpt_get_uint32(io, 1, nullptr));
    EXPECT_EQ(-8782211, fpt_get_int32(io, 42, nullptr));
    EXPECT_EQ(15047220096467327, fpt_get_uint64(io, 111, nullptr));
    EXPECT_EQ(-60585001468255361, fpt_get_int64(io, fpt_max_cols / 3, nullptr));
    EXPECT_EQ(3.1415926535897932, fpt_get_fp64(io, fpt_max_cols - 3, nullptr));
    EXPECT_EQ((float) 2.7182818284590452354, fpt_get_fp32(io, fpt_max_cols - 4, nullptr));
    EXPECT_STREQ("abc", fpt_get_cstr(io, fpt_max_cols - 1, nullptr));
    EXPECT_STREQ("cstr", fpt_get_cstr(io, 42, nullptr));
    EXPECT_EQ(fpt_eq, fpt_cmp_96(io, fpt_max_cols / 2, _96));
    EXPECT_EQ(fpt_eq, fpt_cmp_128(io, 257, _128));
    EXPECT_EQ(fpt_eq, fpt_cmp_160(io, 7, _160));
    EXPECT_EQ(fpt_eq, fpt_cmp_192(io, 8, _192));
    EXPECT_EQ(fpt_eq, fpt_cmp_256(io, fpt_max_cols - 2, _256));

    EXPECT_EQ(0, fpt_space4items(pt));
    EXPECT_EQ(0, fpt_space4data(pt));
    EXPECT_EQ(0, fpt_junkspace(pt));
    ASSERT_STREQ(nullptr, fpt_check(pt));
    free(pt);
}

struct iovec opaque_iov(unsigned col, unsigned n, unsigned salt) {
    struct iovec value;
    value.iov_base = (void*) (pattern + (n * salt) % 223);
    value.iov_len = 4 + ((n + col) & 3) * 4;
    return value;
}

TEST(Upsert, Overwrite) {
    fpt_rw* pt = fpt_alloc(3 + 3 * 2, (1+2 + (2+3+4)*2+5) * 4);
    ASSERT_NE(nullptr, pt);
    ASSERT_STREQ(nullptr, fpt_check(pt));
    EXPECT_EQ(9, fpt_space4items(pt));
    EXPECT_EQ(0, fpt_junkspace(pt));

    const unsigned m = 46091;
    unsigned n = 1;
    EXPECT_EQ(fpt_ok, fpt_upsert_opaque_iov(pt, 0, opaque_iov(0, n, 23)));
    EXPECT_EQ(fpt_ok, fpt_upsert_opaque_iov(pt, 1, opaque_iov(1, n, 37)));
    EXPECT_EQ(fpt_ok, fpt_upsert_opaque_iov(pt, 2, opaque_iov(2, n, 41)));
    EXPECT_EQ(fpt_ok, fpt_upsert_uint16(pt, 3, n * m));
    EXPECT_EQ(fpt_ok, fpt_upsert_uint32(pt, 4, n * m * m));
    EXPECT_EQ(fpt_ok, fpt_upsert_uint64(pt, 5, n * m * m * m));
    ASSERT_STREQ(nullptr, fpt_check(pt));
    EXPECT_EQ(3, fpt_space4items(pt));
    EXPECT_EQ(0, fpt_junkspace(pt));

    fpt_ro io = fpt_take_noshrink(pt);
    ASSERT_STREQ(nullptr, fpt_check_ro(io));
    EXPECT_EQ(fpt_eq, fpt_cmp_opaque_iov(io, 0, opaque_iov(0, n, 23)));
    EXPECT_EQ(fpt_eq, fpt_cmp_opaque_iov(io, 1, opaque_iov(1, n, 37)));
    EXPECT_EQ(fpt_eq, fpt_cmp_opaque_iov(io, 2, opaque_iov(2, n, 41)));
    EXPECT_EQ((uint16_t)(n * m), fpt_get_uint16(io, 3, nullptr));
    EXPECT_EQ((uint32_t)(n * m * m), fpt_get_uint32(io, 4, nullptr));
    EXPECT_EQ((uint64_t)(n * m * m * m), fpt_get_uint64(io, 5, nullptr));

    while(n < 1001) {
        unsigned p = n++;

        // overwrite field#3 and check all
        SCOPED_TRACE("touch field #3, uint16_t, n " + std::to_string(n));
        ASSERT_EQ(fpt_ok, fpt_upsert_uint16(pt, 3, n * m));
        ASSERT_STREQ(nullptr, fpt_check(pt));
        io = fpt_take_noshrink(pt);
        ASSERT_STREQ(nullptr, fpt_check_ro(io));
        EXPECT_EQ(fpt_eq, fpt_cmp_opaque_iov(io, 0, opaque_iov(0, p, 23)));
        EXPECT_EQ(fpt_eq, fpt_cmp_opaque_iov(io, 1, opaque_iov(1, p, 37)));
        EXPECT_EQ(fpt_eq, fpt_cmp_opaque_iov(io, 2, opaque_iov(2, p, 41)));
        EXPECT_EQ((uint16_t)(n * m), fpt_get_uint16(io, 3, nullptr));
        EXPECT_EQ((uint32_t)(p * m * m), fpt_get_uint32(io, 4, nullptr));
        EXPECT_EQ((uint64_t)(p * m * m * m), fpt_get_uint64(io, 5, nullptr));

        // overwrite field#5 and check all
        SCOPED_TRACE("touch field #5, uint64_t, n " + std::to_string(n));
        ASSERT_EQ(fpt_ok, fpt_upsert_uint64(pt, 5, n * m * m * m));
        ASSERT_STREQ(nullptr, fpt_check(pt));
        io = fpt_take_noshrink(pt);
        ASSERT_STREQ(nullptr, fpt_check_ro(io));
        EXPECT_EQ(fpt_eq, fpt_cmp_opaque_iov(io, 0, opaque_iov(0, p, 23)));
        EXPECT_EQ(fpt_eq, fpt_cmp_opaque_iov(io, 1, opaque_iov(1, p, 37)));
        EXPECT_EQ(fpt_eq, fpt_cmp_opaque_iov(io, 2, opaque_iov(2, p, 41)));
        EXPECT_EQ((uint16_t)(n * m), fpt_get_uint16(io, 3, nullptr));
        EXPECT_EQ((uint32_t)(p * m * m), fpt_get_uint32(io, 4, nullptr));
        EXPECT_EQ((uint64_t)(n * m * m * m), fpt_get_uint64(io, 5, nullptr));

        // overwrite field#0 and check all
        SCOPED_TRACE("touch field #0, len "
            + std::to_string(opaque_iov(0, p, 23).iov_len)
            + "=>" + std::to_string(opaque_iov(0, n, 23).iov_len)
            + ", n " + std::to_string(n)
            + ", space " + std::to_string(fpt_space4items(pt))
                + "/" + std::to_string(fpt_space4data(pt))
                + ", junk " + std::to_string(fpt_junkspace(pt)));
        ASSERT_EQ(fpt_ok, fpt_upsert_opaque_iov(pt, 0, opaque_iov(0, n, 23)));
        ASSERT_STREQ(nullptr, fpt_check(pt));
        io = fpt_take_noshrink(pt);
        ASSERT_STREQ(nullptr, fpt_check_ro(io));
        EXPECT_EQ(fpt_eq, fpt_cmp_opaque_iov(io, 0, opaque_iov(0, n, 23)));
        EXPECT_EQ(fpt_eq, fpt_cmp_opaque_iov(io, 1, opaque_iov(1, p, 37)));
        EXPECT_EQ(fpt_eq, fpt_cmp_opaque_iov(io, 2, opaque_iov(2, p, 41)));
        EXPECT_EQ((uint16_t)(n * m), fpt_get_uint16(io, 3, nullptr));
        EXPECT_EQ((uint32_t)(p * m * m), fpt_get_uint32(io, 4, nullptr));
        EXPECT_EQ((uint64_t)(n * m * m * m), fpt_get_uint64(io, 5, nullptr));

        // overwrite field#2 and check all
        SCOPED_TRACE("touch field #2, len "
            + std::to_string(opaque_iov(2, p, 41).iov_len)
            + "=>" + std::to_string(opaque_iov(2, n, 41).iov_len)
            + ", space " + std::to_string(fpt_space4items(pt))
                + "/" + std::to_string(fpt_space4data(pt))
                + ", junk " + std::to_string(fpt_junkspace(pt)));
        ASSERT_EQ(fpt_ok, fpt_upsert_opaque_iov(pt, 2, opaque_iov(2, n, 41)));
        ASSERT_STREQ(nullptr, fpt_check(pt));
        io = fpt_take_noshrink(pt);
        ASSERT_STREQ(nullptr, fpt_check_ro(io));
        EXPECT_EQ(fpt_eq, fpt_cmp_opaque_iov(io, 0, opaque_iov(0, n, 23)));
        EXPECT_EQ(fpt_eq, fpt_cmp_opaque_iov(io, 1, opaque_iov(1, p, 37)));
        EXPECT_EQ(fpt_eq, fpt_cmp_opaque_iov(io, 2, opaque_iov(2, n, 41)));
        EXPECT_EQ((uint16_t)(n * m), fpt_get_uint16(io, 3, nullptr));
        EXPECT_EQ((uint32_t)(p * m * m), fpt_get_uint32(io, 4, nullptr));
        EXPECT_EQ((uint64_t)(n * m * m * m), fpt_get_uint64(io, 5, nullptr));

        // overwrite field#4 and check all
        SCOPED_TRACE("touch field #4, uint32_t, n " + std::to_string(n));
        ASSERT_EQ(fpt_ok, fpt_upsert_uint32(pt, 4, n * m * m));
        ASSERT_STREQ(nullptr, fpt_check(pt));
        io = fpt_take_noshrink(pt);
        ASSERT_STREQ(nullptr, fpt_check_ro(io));
        EXPECT_EQ(fpt_eq, fpt_cmp_opaque_iov(io, 0, opaque_iov(0, n, 23)));
        EXPECT_EQ(fpt_eq, fpt_cmp_opaque_iov(io, 1, opaque_iov(1, p, 37)));
        EXPECT_EQ(fpt_eq, fpt_cmp_opaque_iov(io, 2, opaque_iov(2, n, 41)));
        EXPECT_EQ((uint16_t)(n * m), fpt_get_uint16(io, 3, nullptr));
        EXPECT_EQ((uint32_t)(n * m * m), fpt_get_uint32(io, 4, nullptr));
        EXPECT_EQ((uint64_t)(n * m * m * m), fpt_get_uint64(io, 5, nullptr));

        // overwrite field#1 and check all
        SCOPED_TRACE("touch field #1, len "
            + std::to_string(opaque_iov(1, p, 37).iov_len)
            + "=>" + std::to_string(opaque_iov(1, n, 37).iov_len)
            + ", space " + std::to_string(fpt_space4items(pt))
                + "/" + std::to_string(fpt_space4data(pt))
                + ", junk " + std::to_string(fpt_junkspace(pt)));
        ASSERT_EQ(fpt_ok, fpt_upsert_opaque_iov(pt, 1, opaque_iov(1, n, 37)));
        ASSERT_STREQ(nullptr, fpt_check(pt));
        io = fpt_take_noshrink(pt);
        ASSERT_STREQ(nullptr, fpt_check_ro(io));
        EXPECT_EQ(fpt_eq, fpt_cmp_opaque_iov(io, 0, opaque_iov(0, n, 23)));
        EXPECT_EQ(fpt_eq, fpt_cmp_opaque_iov(io, 1, opaque_iov(1, n, 37)));
        EXPECT_EQ(fpt_eq, fpt_cmp_opaque_iov(io, 2, opaque_iov(2, n, 41)));
        EXPECT_EQ((uint16_t)(n * m), fpt_get_uint16(io, 3, nullptr));
        EXPECT_EQ((uint32_t)(n * m * m), fpt_get_uint32(io, 4, nullptr));
        EXPECT_EQ((uint64_t)(n * m * m * m), fpt_get_uint64(io, 5, nullptr));
    }

    ASSERT_STREQ(nullptr, fpt_check(pt));
    free(pt);
}

int main(int argc, char** argv) {
    testing ::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

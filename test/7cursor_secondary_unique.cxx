/*
 * Copyright 2016-2017 libfpta authors: please see AUTHORS file.
 *
 * This file is part of libfpta, aka "Fast Positive Tables".
 *
 * libfpta is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * libfpta is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with libfpta.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "fpta_test.h"
#include "keygen.hpp"

/* Кол-во проверочных точек в диапазонах значений индексируемых типов.
 *
 * Значение не может быть больше чем 65536, так как это предел кол-ва
 * уникальных значений для fptu_uint16.
 *
 * Но для парного генератора не может быть больше 65536/NDUP,
 * так как для не-уникальных вторичных индексов нам требуются дубликаты,
 * что требует больше уникальных значений для первичного ключа.
 *
 * Использовать тут большие значения смысла нет. Время работы тестов
 * растет примерно линейно (чуть быстрее), тогда как вероятность
 * проявления каких-либо ошибок растет в лучшем случае как Log(NNN),
 * а скорее даже как SquareRoot(Log(NNN)).
 */
static constexpr unsigned NDUP = 5;
#if FPTA_CURSOR_UT_LONG
static constexpr unsigned NNN = 13103; // около часа в /dev/shm/
#else
static constexpr unsigned NNN = 41; // порядка 10-15 секунд в /dev/shm/
#endif

static const char testdb_name[] = TEST_DB_DIR "ut_cursor_secondary1.fpta";
static const char testdb_name_lck[] =
    TEST_DB_DIR "ut_cursor_secondary1.fpta" MDBX_LOCK_SUFFIX;

#include "cursor_secondary.hpp"

//----------------------------------------------------------------------------

#if GTEST_HAS_COMBINE

INSTANTIATE_TEST_CASE_P(
    Combine, CursorSecondary,
    ::testing::Combine(
        ::testing::Values(fpta_primary_unique_ordered_obverse,
                          fpta_primary_unique_ordered_reverse,
                          fpta_primary_withdups_ordered_obverse,
                          fpta_primary_withdups_ordered_reverse,
                          fpta_primary_unique_unordered,
                          fpta_primary_withdups_unordered),
        ::testing::Values(fptu_null, fptu_uint16, fptu_int32, fptu_uint32,
                          fptu_fp32, fptu_int64, fptu_uint64, fptu_fp64,
                          fptu_96, fptu_128, fptu_160, fptu_datetime, fptu_256,
                          fptu_cstr, fptu_opaque
                          /*, fptu_nested, fptu_farray */),
        ::testing::Values(fpta_secondary_unique_ordered_obverse,
                          fpta_secondary_unique_ordered_reverse,
                          fpta_secondary_unique_unordered),
        ::testing::Values(fptu_null, fptu_uint16, fptu_int32, fptu_uint32,
                          fptu_fp32, fptu_int64, fptu_uint64, fptu_fp64,
                          fptu_96, fptu_128, fptu_160, fptu_datetime, fptu_256,
                          fptu_cstr, fptu_opaque
                          /*, fptu_nested, fptu_farray */),
        ::testing::Values(fpta_unsorted, fpta_ascending, fpta_descending)));
#else

TEST(CursorSecondary, GoogleTestCombine_IS_NOT_Supported_OnThisPlatform) {}

#endif /* GTEST_HAS_COMBINE */

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

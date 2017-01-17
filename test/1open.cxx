/*
 * Copyright 2016 libfpta authors: please see AUTHORS file.
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

#include "fast_positive/tables_internal.h"
#include <gtest/gtest.h>

static const char testdb_name[] = "ut_create.fpta";
static const char testdb_name_lck[] = "ut_create.fpta-lock";

TEST(Open, Trivia) {
  /* Тривиальный тест открытия/создания БД во всех режимах durability.
   * Корректность самих режимов не проверяется. */
  ASSERT_TRUE(unlink(testdb_name) == 0 || errno == ENOENT);
  ASSERT_TRUE(unlink(testdb_name_lck) == 0 || errno == ENOENT);

  fpta_db *db = (fpta_db *)&db;
  EXPECT_EQ(ENOENT,
            fpta_db_open(testdb_name, fpta_readonly, 0644, 1, false, &db));
  EXPECT_EQ(nullptr, db);
  ASSERT_TRUE(unlink(testdb_name) != 0 && errno == ENOENT);
  ASSERT_TRUE(unlink(testdb_name_lck) != 0 && errno == ENOENT);

  EXPECT_EQ(FPTA_SUCCESS,
            fpta_db_open(testdb_name, fpta_sync, 0644, 1, false, &db));
  EXPECT_NE(nullptr, db);
  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));
  ASSERT_TRUE(unlink(testdb_name) == 0);
  ASSERT_TRUE(unlink(testdb_name_lck) == 0);

  EXPECT_EQ(FPTA_SUCCESS,
            fpta_db_open(testdb_name, fpta_sync, 0644, 1, false, &db));
  EXPECT_NE(nullptr, db);
  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));
  ASSERT_TRUE(unlink(testdb_name) == 0);
  ASSERT_TRUE(unlink(testdb_name_lck) == 0);

  EXPECT_EQ(FPTA_SUCCESS,
            fpta_db_open(testdb_name, fpta_lazy, 0644, 1, false, &db));
  EXPECT_NE(nullptr, db);
  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));
  ASSERT_TRUE(unlink(testdb_name) == 0);
  ASSERT_TRUE(unlink(testdb_name_lck) == 0);

  EXPECT_EQ(FPTA_SUCCESS,
            fpta_db_open(testdb_name, fpta_async, 0644, 1, false, &db));
  EXPECT_NE(nullptr, db);
  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));
  ASSERT_TRUE(unlink(testdb_name) == 0);
  ASSERT_TRUE(unlink(testdb_name_lck) == 0);
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

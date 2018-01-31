/*
 * Copyright 2016-2018 libfpta authors: please see AUTHORS file.
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
#include <string>
#include <thread>

static const char testdb_name[] = TEST_DB_DIR "ut_thread.fpta";
static const char testdb_name_lck[] =
    TEST_DB_DIR "ut_thread.fpta" MDBX_LOCK_SUFFIX;

using namespace std;

static std::string random_string(int len, int seed) {
  static std::string alphabet = "abcdefghijklmnopqrstuvwxyz0123456789";
  std::string result;
  srand((unsigned)seed);
  for (int i = 0; i < len; ++i)
    result.push_back(alphabet[rand() % alphabet.length()]);
  return result;
}

static void thread_proc(fpta_db *db, int thread_num) {
  SCOPED_TRACE("Thread " + std::to_string(thread_num) + " started");

  for (int i = 0; i < 500; ++i) {
    fpta_txn *txn = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_write, &txn));
    ASSERT_NE(nullptr, txn);

    fpta_name table, num, uuid, dst_ip, port, date;
    EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "table"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &num, "num"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &uuid, "uuidfield"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &dst_ip, "dst_ip"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &port, "port"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &date, "date"));

    fptu_rw *tuple = fptu_alloc(5, 1000);
    ASSERT_NE(nullptr, tuple);
    EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &table));

    EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &num));
    uint64_t result = 0;
    EXPECT_EQ(FPTA_OK, fpta_table_sequence(txn, &table, &result, 1));
    EXPECT_EQ(FPTA_OK,
              fpta_upsert_column(tuple, &num, fpta_value_uint(result)));

    EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &uuid));
    EXPECT_EQ(FPTA_OK,
              fpta_upsert_column(
                  tuple, &uuid,
                  fpta_value_cstr(
                      random_string(36, thread_num * 32768 + i).c_str())));

    EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &dst_ip));
    EXPECT_EQ(FPTA_OK,
              fpta_upsert_column(tuple, &dst_ip, fpta_value_cstr("127.0.0.1")));

    EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &port));
    EXPECT_EQ(FPTA_OK, fpta_upsert_column(tuple, &port, fpta_value_sint(100)));

    EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &date));
    EXPECT_EQ(FPTA_OK, fpta_upsert_column(
                           tuple, &date, fpta_value_datetime(fptu_now_fine())));

    EXPECT_EQ(FPTA_OK,
              fpta_probe_and_upsert_row(txn, &table, fptu_take(tuple)));

    fptu_clear(tuple);
    free(tuple);

    fpta_name_destroy(&table);
    fpta_name_destroy(&num);
    fpta_name_destroy(&uuid);
    fpta_name_destroy(&dst_ip);
    fpta_name_destroy(&port);
    fpta_name_destroy(&date);

    SCOPED_TRACE("Thread " + std::to_string(thread_num) + " insertion " +
                 std::to_string(i));
    EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
#if defined(_WIN32) || defined(_WIN64)
    SwitchToThread();
#else
    sched_yield();
#endif
  }
  SCOPED_TRACE("Thread " + std::to_string(thread_num) + " finished");
}

TEST(Threaded, SimpleConcurence) {
  fpta_db *db = nullptr;

  // чистим
  if (REMOVE_FILE(testdb_name) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }
  if (REMOVE_FILE(testdb_name_lck) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }

  EXPECT_EQ(FPTA_OK, fpta_db_open(testdb_name, fpta_weak, fpta_saferam, 0644, 1,
                                  true, &db));
  ASSERT_NE(db, (fpta_db *)nullptr);
  SCOPED_TRACE("Database opened");

  { // create table
    fpta_column_set def;
    fpta_column_set_init(&def);
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe("num", fptu_uint64,
                                   fpta_primary_unique_ordered_obverse, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe("uuidfield", fptu_cstr,
                                            fpta_noindex_nullable, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe("dst_ip", fptu_cstr,
                                            fpta_noindex_nullable, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe("port", fptu_int64,
                                            fpta_noindex_nullable, &def));
    fpta_column_describe("date", fptu_datetime, fpta_noindex_nullable, &def);

    fpta_txn *txn = nullptr;
    ASSERT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
    ASSERT_EQ(FPTA_OK, fpta_table_create(txn, "table", &def));
    ASSERT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
    SCOPED_TRACE("Table created");
  }

  ASSERT_EQ(FPTA_OK, fpta_db_close(db));
  db = nullptr;
  SCOPED_TRACE("Database closed");

  EXPECT_EQ(FPTA_OK, fpta_db_open(testdb_name, fpta_weak, fpta_saferam, 0644, 1,
                                  false, &db));
  ASSERT_NE(db, (fpta_db *)nullptr);
  SCOPED_TRACE("Database reopened");

  thread_proc(db, 42);

  const int threadNum = 2;
  vector<std::thread> threads;
  for (int16_t i = 1; i <= threadNum; ++i)
    threads.push_back(std::thread(thread_proc, db, i));

  for (auto &it : threads)
    it.join();

  SCOPED_TRACE("All threads are stopped");
  EXPECT_EQ(FPTA_OK, fpta_db_close(db));
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

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

#ifdef CI
#define REMOVE_DB_FILES true
#else
// пока не удялем файлы чтобы можно было посмотреть и натравить mdbx_chk
#define REMOVE_DB_FILES false
#endif

using namespace std;

static std::string random_string(int len, int seed) {
  static std::string alphabet = "abcdefghijklmnopqrstuvwxyz0123456789";
  std::string result;
  srand((unsigned)seed);
  for (int i = 0; i < len; ++i)
    result.push_back(alphabet[rand() % alphabet.length()]);
  return result;
}

//------------------------------------------------------------------------------

static void write_thread_proc(fpta_db *db, const int thread_num,
                              const int reps) {
  SCOPED_TRACE("Thread " + std::to_string(thread_num) + " started");

  for (int i = 0; i < reps; ++i) {
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

  write_thread_proc(db, 42, 50);

#ifdef CI
  const int reps = 250;
#else
  const int reps = 500;
#endif

  const int threadNum = 2;
  vector<std::thread> threads;
  for (int16_t i = 1; i <= threadNum; ++i)
    threads.push_back(std::thread(write_thread_proc, db, i, reps));

  for (auto &it : threads)
    it.join();

  SCOPED_TRACE("All threads are stopped");
  EXPECT_EQ(FPTA_OK, fpta_db_close(db));

  if (REMOVE_DB_FILES) {
    ASSERT_TRUE(REMOVE_FILE(testdb_name) == 0);
    ASSERT_TRUE(REMOVE_FILE(testdb_name_lck) == 0);
  }
}

//------------------------------------------------------------------------------

static void read_thread_proc(fpta_db *db, const int thread_num,
                             const int reps) {
  SCOPED_TRACE("Thread " + std::to_string(thread_num) + " started");

  fpta_name table, ip, port;
  EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "MyTable"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &ip, "Ip"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &port, "port"));

  fpta_filter filter, filter_a, filter_b;
  memset(&filter, 0, sizeof(fpta_filter));
  memset(&filter_a, 0, sizeof(fpta_filter));
  memset(&filter_b, 0, sizeof(fpta_filter));

  filter.type = fpta_node_and;
  filter.node_and.a = &filter_a;
  filter.node_and.b = &filter_b;

  filter_a.type = fpta_node_ne;
  filter_a.node_cmp.left_id = &ip;

  filter_b.type = fpta_node_ne;
  filter_b.node_cmp.left_id = &port;

  for (int i = 0; i < reps; ++i) {
    fpta_txn *txn = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_read, &txn));
    ASSERT_NE(nullptr, txn);

    EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &table));
    fpta_name column;
    EXPECT_EQ(FPTA_OK, fpta_table_column_get(&table, 0, &column));

    std::string str = random_string(15, i);
    filter_a.node_cmp.right_value = fpta_value_cstr(str.c_str());
    filter_b.node_cmp.right_value = fpta_value_sint(1000 + (rand() % 1000));

    fpta_cursor *cursor = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_cursor_open(txn, &column, fpta_value_begin(),
                                        fpta_value_end(), &filter,
                                        fpta_unsorted_dont_fetch, &cursor));
    ASSERT_NE(nullptr, cursor);
    EXPECT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_first));
    EXPECT_EQ(FPTA_OK, fpta_cursor_eof(cursor));
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor));

    fpta_name_destroy(&column);
    EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  }
  SCOPED_TRACE("Thread " + std::to_string(thread_num) + " finished");
}

TEST(Threaded, SimpleSelect) {
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
              fpta_column_describe("Ip", fptu_cstr,
                                   fpta_primary_unique_ordered_obverse, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe("port", fptu_int64,
                                            fpta_noindex_nullable, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe("word", fptu_cstr,
                                            fpta_noindex_nullable, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe(
                           "_last_changed", fptu_datetime,
                           fpta_secondary_withdups_ordered_obverse, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe(
                           "_id", fptu_uint64,
                           fpta_secondary_unique_ordered_obverse, &def));

    fpta_txn *txn = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
    ASSERT_NE(nullptr, txn);

    EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "MyTable", &def));
    EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
    SCOPED_TRACE("Table created");
  }
  ASSERT_EQ(FPTA_OK, fpta_db_close(db));
  db = nullptr;
  SCOPED_TRACE("Database closed");

  EXPECT_EQ(FPTA_OK, fpta_db_open(testdb_name, fpta_weak, fpta_saferam, 0644, 1,
                                  false, &db));
  ASSERT_NE(db, (fpta_db *)nullptr);
  SCOPED_TRACE("Database reopened");

  fpta_txn *txn = nullptr;
  fpta_transaction_begin(db, fpta_write, &txn);
  ASSERT_NE(nullptr, txn);

  fpta_name table, ip, port, word, date, id;
  EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "MyTable"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &ip, "Ip"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &port, "port"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &word, "word"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &date, "_last_changed"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &id, "_id"));

  fptu_rw *tuple = fptu_alloc(5, 1000);
  ASSERT_NE(nullptr, tuple);
  EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &table));

  EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &id));
  uint64_t result = 0;
  EXPECT_EQ(FPTA_OK, fpta_table_sequence(txn, &table, &result, 1));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(tuple, &id, fpta_value_uint(result)));

  EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &ip));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(tuple, &ip, fpta_value_cstr("1.1.1.1")));

  EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &word));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(tuple, &word, fpta_value_cstr("hello")));

  EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &port));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(tuple, &port, fpta_value_sint(111)));

  EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &date));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(tuple, &date,
                                        fpta_value_datetime(fptu_now_fine())));

  EXPECT_EQ(FPTA_OK, fpta_probe_and_upsert_row(txn, &table, fptu_take(tuple)));

  EXPECT_EQ(FPTA_OK, fptu_clear(tuple));
  free(tuple);

  fpta_name_destroy(&table);
  fpta_name_destroy(&id);
  fpta_name_destroy(&word);
  fpta_name_destroy(&ip);
  fpta_name_destroy(&port);
  fpta_name_destroy(&date);

  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  SCOPED_TRACE("Record written");

#ifdef CI
  const int reps = 1000;
#else
  const int reps = 100000;
#endif

  const int threadNum = 16;
  vector<std::thread> threads;
  for (int i = 0; i < threadNum; ++i)
    threads.push_back(std::thread(read_thread_proc, db, i, reps));

  for (auto &thread : threads)
    thread.join();

  SCOPED_TRACE("All threads are stopped");
  EXPECT_EQ(FPTA_OK, fpta_db_close(db));

  if (REMOVE_DB_FILES) {
    ASSERT_TRUE(REMOVE_FILE(testdb_name) == 0);
    ASSERT_TRUE(REMOVE_FILE(testdb_name_lck) == 0);
  }
}

//------------------------------------------------------------------------------

static int visitor(const fptu_ro *row, void *context, void *arg) {
  fpta_value val;
  fpta_name *name = (fpta_name *)arg;
  int rc = fpta_get_column(*row, name, &val);
  if (rc != FPTA_OK)
    return rc;

  if (val.type != fpta_signed_int)
    return (int)FPTA_DEADBEEF;
  int64_t *max_val = (int64_t *)context;
  if (val.sint > *max_val)
    *max_val = val.sint;

  return FPTA_OK;
}

static void visitor_thread_proc(fpta_db *db, const int thread_num,
                                const int reps, int *counter) {
  SCOPED_TRACE("Thread " + std::to_string(thread_num) + " started");
  *counter = 0;

  fpta_name table, key, host, date, id;
  EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "Counting"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &key, "key"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &host, "host"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &date, "_last_changed"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &id, "_id"));

  fpta_filter filter;
  memset(&filter, 0, sizeof(fpta_filter));
  filter.type = fpta_node_gt;
  filter.node_cmp.left_id = &key;
  filter.node_cmp.right_value = fpta_value_sint(0);

  for (int i = 0; i < reps; ++i) {

    // start read-transaction and get max_value
    int64_t max_value = 0;
    {
      fpta_txn *txn = nullptr;
      EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_read, &txn));
      ASSERT_NE(nullptr, txn);

      EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &key));

      fpta_name column;
      EXPECT_EQ(FPTA_OK, fpta_table_column_get(&table, 0, &column));
      EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &column));
      size_t count = 0;

      int err = fpta_apply_visitor(txn, &column, fpta_value_begin(),
                                   fpta_value_end(), &filter, fpta_unsorted, 0,
                                   10000, nullptr, nullptr, &count, &visitor,
                                   &max_value /* context */, &key /* arg */);
      if (err != FPTA_OK) {
        EXPECT_EQ(FPTA_NODATA, err);
      }

      fpta_name_destroy(&column);
      EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
    }

    // start write-transaction and insert max_value + 1
    {
      fpta_txn *txn = nullptr;
      EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_write, &txn));
      ASSERT_NE(nullptr, txn);

      EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &table));

      fptu_rw *tuple = fptu_alloc(4, 1000);
      ASSERT_NE(nullptr, tuple);

      EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &id));
      uint64_t result = 0;
      EXPECT_EQ(FPTA_OK, fpta_table_sequence(txn, &table, &result, 1));
      EXPECT_EQ(FPTA_OK,
                fpta_upsert_column(tuple, &id, fpta_value_uint(result)));

      EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &host));
      std::string str = random_string(15, i);
      EXPECT_EQ(FPTA_OK,
                fpta_upsert_column(tuple, &host, fpta_value_cstr(str.c_str())));

      EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &date));
      EXPECT_EQ(FPTA_OK,
                fpta_upsert_column(tuple, &date,
                                   fpta_value_datetime(fptu_now_fine())));

      EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &key));
      EXPECT_EQ(FPTA_OK, fpta_upsert_column(tuple, &key,
                                            fpta_value_sint(max_value + 1)));

      int err = fpta_probe_and_upsert_row(txn, &table, fptu_take(tuple));
      fptu_clear(tuple);
      free(tuple);

      if (err != FPTA_OK) {
        // отменяем если была ошибка
        ASSERT_EQ(FPTA_OK, fpta_transaction_end(txn, true));
      } else {
        // коммитим и ожидаем ошибку переполнения здесь
        err = fpta_transaction_end(txn, false);
        if (err != FPTA_OK) {
          ASSERT_EQ(FPTA_DB_FULL, err);
        }
      }
    }
  }

  fpta_name_destroy(&table);
  fpta_name_destroy(&id);
  fpta_name_destroy(&host);
  fpta_name_destroy(&key);
  fpta_name_destroy(&date);
  SCOPED_TRACE("Thread " + std::to_string(thread_num) + " finished");
}

TEST(Threaded, SimpleVisitor) {
  if (REMOVE_FILE(testdb_name) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }
  if (REMOVE_FILE(testdb_name_lck) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }

  fpta_db *db = nullptr;
  fpta_db_open(testdb_name, fpta_weak, fpta_saferam, 0644, 1, true, &db);
  ASSERT_NE(db, (fpta_db *)nullptr);
  SCOPED_TRACE("Database opened");

  { // create table
    fpta_column_set def;
    fpta_column_set_init(&def);
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe("key", fptu_int64,
                                   fpta_primary_unique_ordered_obverse, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe(
                           "host", fptu_cstr,
                           fpta_secondary_withdups_ordered_obverse, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe(
                           "_last_changed", fptu_datetime,
                           fpta_secondary_withdups_ordered_obverse, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe(
                           "_id", fptu_uint64,
                           fpta_secondary_unique_ordered_obverse, &def));

    fpta_txn *txn = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
    ASSERT_NE(nullptr, txn);
    EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "Counting", &def));
    EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
    SCOPED_TRACE("Table created");
  }
  ASSERT_EQ(FPTA_OK, fpta_db_close(db));
  db = nullptr;
  SCOPED_TRACE("Database closed");

  EXPECT_EQ(FPTA_OK, fpta_db_open(testdb_name, fpta_weak, fpta_saferam, 0644, 1,
                                  false, &db));
  ASSERT_NE(db, (fpta_db *)nullptr);
  SCOPED_TRACE("Database reopened");

#ifdef CI
  const int reps = 1000;
#else
  const int reps = 10000;
#endif

  const int threadNum = 16;
  int counters[threadNum] = {0};
  vector<std::thread> threads;
  for (int i = 0; i < threadNum; ++i)
    threads.push_back(
        std::thread(visitor_thread_proc, db, i, reps, &counters[i]));

  int i = 0;
  for (auto &thread : threads) {
    SCOPED_TRACE("Thread " + std::to_string(i) + ": counter = " +
                 std::to_string(counters[i]));
    thread.join();
  }

  SCOPED_TRACE("All threads are stopped");
  EXPECT_EQ(FPTA_OK, fpta_db_close(db));

  if (REMOVE_DB_FILES) {
    ASSERT_TRUE(REMOVE_FILE(testdb_name) == 0);
    ASSERT_TRUE(REMOVE_FILE(testdb_name_lck) == 0);
  }
}

//------------------------------------------------------------------------------

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

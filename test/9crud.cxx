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

static const char testdb_name[] = TEST_DB_DIR "ut_crud.fpta";
static const char testdb_name_lck[] =
    TEST_DB_DIR "ut_crud.fpta" MDBX_LOCK_SUFFIX;

class CrudSimple
    : public ::testing::TestWithParam<
#if GTEST_USE_OWN_TR1_TUPLE || GTEST_HAS_TR1_TUPLE
          std::tr1::tuple<bool, unsigned, unsigned, unsigned, unsigned>>
#else
          std::tuple<bool, unsigned, unsigned, unsigned, unsigned>>
#endif
{
public:
  bool secondary;
  unsigned order_key;
  unsigned order_val;
  unsigned nitems;
  unsigned shift;

  scoped_db_guard db_quard;
  scoped_txn_guard txn_guard;
  scoped_cursor_guard cursor_guard;

  fpta_name table, col_pk, col_se, col_val;

  virtual void SetUp() {
#if GTEST_USE_OWN_TR1_TUPLE || GTEST_HAS_TR1_TUPLE
    secondary = std::tr1::get<0>(GetParam());
    nitems = std::tr1::get<1>(GetParam());
    shift = std::tr1::get<2>(GetParam());
    order_key = std::tr1::get<3>(GetParam());
    order_val = std::tr1::get<4>(GetParam());
#else
    secondary = std::get<0>(GetParam());
    nitems = std::get<1>(GetParam());
    shift = std::get<2>(GetParam());
    order_key = std::get<3>(GetParam());
    order_val = std::get<4>(GetParam());
#endif

    // инициализируем идентификаторы таблицы и её колонок
    EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "table"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_pk, "pk_str_uniq"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_se, "se_opaque_dups"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_val, "col_uint"));

    // чистим
    if (REMOVE_FILE(testdb_name) != 0)
      ASSERT_EQ(ENOENT, errno);
    if (REMOVE_FILE(testdb_name_lck) != 0)
      ASSERT_EQ(ENOENT, errno);

    // открываем/создаем базульку в 1 мегабайт
    fpta_db *db = nullptr;
    EXPECT_EQ(FPTA_SUCCESS,
              fpta_db_open(testdb_name, fpta_async, 0644, 1, true, &db));
    ASSERT_NE(nullptr, db);
    db_quard.reset(db);

    // описываем простейшую таблицу с тремя колонками,
    // одним Primary и одним Secondary
    fpta_column_set def;
    fpta_column_set_init(&def);

    EXPECT_EQ(FPTA_OK, fpta_column_describe("pk_str_uniq", fptu_cstr,
                                            secondary ? fpta_primary
                                                      : fpta_primary_withdups,
                                            &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe("se_opaque_dups", fptu_opaque,
                                            secondary ? fpta_secondary_withdups
                                                      : fpta_index_none,
                                            &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe("col_uint", fptu_uint64,
                                            fpta_index_none, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def));

    // запускам транзакцию и создаем таблицу
    fpta_txn *txn = (fpta_txn *)&txn;
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
    ASSERT_NE(nullptr, txn);
    txn_guard.reset(txn);
    EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "table", &def));
    ASSERT_EQ(FPTA_OK, fpta_transaction_end(txn_guard.release(), false));
    txn = nullptr;

    // разрушаем описание таблицы
    EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def));
    EXPECT_NE(FPTA_OK, fpta_column_set_validate(&def));

    //------------------------------------------------------------------------

    // начинаем транзакцию записи
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_write, &txn));
    ASSERT_NE(nullptr, txn);
    txn_guard.reset(txn);

    // связываем идентификаторы с ранее созданной схемой
    EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &col_pk));
    EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_se));
    EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_val));
  }

  virtual void TearDown() {
    // разрушаем привязанные идентификаторы
    fpta_name_destroy(&table);
    fpta_name_destroy(&col_pk);
    fpta_name_destroy(&col_se);
    fpta_name_destroy(&col_val);

    // закрываем курсор и завершаем транзакцию
    if (cursor_guard)
      EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
    if (txn_guard)
      ASSERT_EQ(FPTA_OK, fpta_transaction_end(txn_guard.release(), true));
    if (db_quard) {
      // закрываем и удаляем базу
      ASSERT_EQ(FPTA_SUCCESS, fpta_db_close(db_quard.release()));
      ASSERT_TRUE(REMOVE_FILE(testdb_name) == 0);
      ASSERT_TRUE(REMOVE_FILE(testdb_name_lck) == 0);
    }
  }
};

TEST_P(CrudSimple, Nulls) {
  /* Проверка корректности обработки ключей и значений нулевой длины,
   * в том числе дубликатов.
   *
   * Было замечено, что опорный движок теряет для ключа-дубликата
   * значение нулевой длины, если это значение было добавлено первым.
   *
   * Например, пусть у нас есть две key-value записи "foo"="bar" и "foo"=NIL.
   * Тогда, если пара "foo"=NIL будет добавлена первой (NIL будет первым
   * добавляемым значением для ключа "foo"), то последующее добавление
   * "foo"="bar" (добавление второго значение для ключа "foo") приведет
   * к потере первой записи "foo"=NIL, как-будто её не добавляли.
   *
   * Этот тест был добавлен для выяснения всех деталей/обстоятельств
   * замеченной проблемы и последующего её устранения.
   *
   * Сценарий:
   *  1. Создаем базу с одной таблицей, в которой три колонки
   *     и два индекса (primary и secondary).
   *  2. Добавляем данные:
   *     - прогоняем тест перебирая все разумные комбинации (порядок
   *       добавления, количество записей, порядок ключей, через первичный
   *       или вторичный индексы).
   *     - добавляем несколько записей, в том числе ключи и значения нулевой
   *       длины, в том числе дубликаты (несколько значений данных для одного
   *       ключа).
   *     - при добавлении формируем проверочную карту, в которой подсчитываем
   *       ожидаемое количество дубликатов соответствующего ключа.
   *  3. Проверяем состояние таблицы:
   *     - в отдельной читающей транзакции открываем курсор по проверяемому
   *       индексу.
   *     - сверяем общее количество строк с ожидаемым.
   *     - итеративно считываем все строки и для каждой сверяем
   *       количество дубликатов с построенной картой.
   *  4. Завершаем операции и освобождаем ресурсы.
   */

  fpta_value empty_string = fpta_value_cstr("");
  ASSERT_EQ(0, empty_string.binary_length);
  fpta_value empty_binary = fpta_value_binary(nullptr, 0);
  ASSERT_EQ(0, empty_binary.binary_length);

  fptu_rw *row = fptu_alloc(3, 42);
  ASSERT_NE(nullptr, row);
  ASSERT_STREQ(nullptr, fptu_check(row));
  fpta_txn *txn = txn_guard.get();
  std::unordered_map<unsigned, unsigned> checker;

  SCOPED_TRACE(std::string(secondary ? "index secondary" : "index primary") +
               ", order_key " + std::to_string(order_key) + ", order_value " +
               std::to_string(order_val) + ", n-items " +
               std::to_string(nitems) + ", shift " + std::to_string(shift));

  std::string changelog;
  for (unsigned i = 0; i < nitems; ++i) {
    const unsigned n = (i + shift) % 9;
    ASSERT_EQ(FPTU_OK, fptu_clear(row));
    ASSERT_STREQ(nullptr, fptu_check(row));

    int key_case, val_case;
    if (!secondary) {
      /* только первичный индекс с возможностью вставлять дубликаты,
       * вставляем несколько пустых ключей и для каждого из них
       * по одному пустому значению в различном порядке */
      key_case = (order_key == n / 3) ? -1 : n / 3;
      val_case = (order_val == n % 3) ? -1 : n;
    } else {
      /* уникальный первичный, плюс вторичный индекс с возможностью вставлять
       * дубликаты, вставляем один пустой ключ в PK и три пустых значения. */
      key_case = (order_key == n) ? -1 : n;
      val_case = (order_val == n % 3) ? -1 : n % 3;
    }

    changelog += fptu::format("\t[%i] %c -> %c\n", i,
                              (key_case < 0) ? '*' : 'A' + key_case,
                              (val_case < 0) ? '*' : 'a' + val_case);

    int count_by = secondary ? val_case : key_case;
    checker[count_by]++;

    SCOPED_TRACE("key-case " + std::to_string(key_case) + ", val-case " +
                 std::to_string(val_case) + ", count_by " +
                 std::to_string(count_by) + " (" +
                 std::to_string(checker[count_by]) + ")");

    ASSERT_EQ(FPTA_OK,
              fpta_upsert_column(row, &col_val, fpta_value_uint(count_by)));

    if (key_case == -1)
      ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_pk, empty_string));
    else
      ASSERT_EQ(FPTA_OK,
                fpta_upsert_column(row, &col_pk,
                                   fpta_value_str(std::to_string(key_case))));

    if (val_case == -1)
      ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_se, empty_binary));
    else {
      std::string value = std::to_string(val_case);
      ASSERT_EQ(FPTA_OK, fpta_upsert_column(
                             row, &col_se,
                             fpta_value_binary(value.data(), value.length())));
    }

    ASSERT_STREQ(nullptr, fptu_check(row));
    ASSERT_EQ(FPTA_OK, fpta_insert_row(txn, &table, fptu_take(row)));
  }

  // завершаем транзакцию
  ASSERT_EQ(FPTA_OK, fpta_transaction_end(txn_guard.release(), false));
  txn = nullptr;

  // разрушаем кортеж
  ASSERT_STREQ(nullptr, fptu_check(row));
  free(row);

  //------------------------------------------------------------------------

  // начинаем транзакцию чтения
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db_quard.get(), fpta_read, &txn));
  ASSERT_NE(nullptr, txn);
  txn_guard.reset(txn);

  SCOPED_TRACE("the changelog is...\n" + changelog);

  fpta_cursor *cursor;
  EXPECT_EQ(FPTA_OK, fpta_cursor_open(txn, secondary ? &col_se : &col_pk,
                                      fpta_value_begin(), fpta_value_end(),
                                      nullptr, fpta_unsorted, &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);

  size_t count;
  ASSERT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(nitems, count);

  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_first));
  for (;;) {
    fptu_ro tuple;
    EXPECT_EQ(FPTA_OK, fpta_cursor_get(cursor_guard.get(), &tuple));
    ASSERT_STREQ(nullptr, fptu_check_ro(tuple));

    int error;
    auto count_by = fptu_get_uint(tuple, col_val.column.num, &error);
    ASSERT_EQ(FPTU_OK, error);

    size_t dups = 100500;
    ASSERT_EQ(FPTA_OK, fpta_cursor_dups(cursor_guard.get(), &dups));
    EXPECT_EQ(checker[count_by], dups);

    error = fpta_cursor_move(cursor, fpta_next);
    if (error == FPTA_NODATA)
      break;
    ASSERT_EQ(FPTA_SUCCESS, error);
  }
}

#if GTEST_HAS_COMBINE
INSTANTIATE_TEST_CASE_P(
    Combine, CrudSimple,
    ::testing::Combine(::testing::Values(true, false),
                       ::testing::Values(1, 2, 3, 4, 5, 6, 7, 8, 9),
                       ::testing::Values(0, 1, 2, 3, 4, 5, 6, 7, 8),
                       ::testing::Values(0, 1, 2), ::testing::Values(0, 1, 2)));
#else
TEST(CrudSimple, GoogleTestCombine_IS_NOT_Supported_OnThisPlatform) {}
#endif /* GTEST_HAS_COMBINE */

//----------------------------------------------------------------------------

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

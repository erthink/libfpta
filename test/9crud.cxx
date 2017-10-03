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

class CrudSimple : public ::testing::TestWithParam<
#if GTEST_USE_OWN_TR1_TUPLE || GTEST_HAS_TR1_TUPLE
                       std::tr1::tuple<bool, int, int, int, int>>
#else
                       std::tuple<bool, int, int, int, int>>
#endif
{
public:
  bool secondary;
  int order_key;
  int order_val;
  int nitems;
  int shift;

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
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_val, "col_int"));

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

    EXPECT_EQ(FPTA_OK, fpta_column_describe(
                           "pk_str_uniq", fptu_cstr,
                           secondary ? fpta_primary_unique_ordered_obverse
                                     : fpta_primary_withdups_ordered_obverse,
                           &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe(
                           "se_opaque_dups", fptu_opaque,
                           secondary ? fpta_secondary_withdups_ordered_obverse
                                     : fpta_index_none,
                           &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe("col_int", fptu_int64,
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
  ASSERT_EQ(0u, empty_string.binary_length);
  fpta_value empty_binary = fpta_value_binary(nullptr, 0);
  ASSERT_EQ(0u, empty_binary.binary_length);

  fptu_rw *row = fptu_alloc(3, 42);
  ASSERT_NE(nullptr, row);
  ASSERT_STREQ(nullptr, fptu_check(row));
  fpta_txn *txn = txn_guard.get();
  std::unordered_map<int, int> checker;

  SCOPED_TRACE(std::string(secondary ? "index secondary" : "index primary") +
               ", order_key " + std::to_string(order_key) + ", order_value " +
               std::to_string(order_val) + ", n-items " +
               std::to_string(nitems) + ", shift " + std::to_string(shift));

  std::string changelog;
  for (int i = 0; i < nitems; ++i) {
    const int n = (i + shift) % 9;
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
              fpta_upsert_column(row, &col_val, fpta_value_sint(count_by)));

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
    auto count_by = (int)fptu_get_sint(tuple, col_val.column.num, &error);
    ASSERT_EQ(FPTU_OK, error);

    size_t dups = 100500;
    ASSERT_EQ(FPTA_OK, fpta_cursor_dups(cursor_guard.get(), &dups));
    EXPECT_EQ((size_t)checker[count_by], dups);

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

TEST(Nullable, AsyncSchemaChange) {
  /* Проверка поведения при асинхронном изменении признака nullable
   * для части колонок.
   *
   * Сценарий:
   *  1. Создаем базу с одной таблицей и двумя индексированные колонками.
   *     Исходно обе колонки НЕ-nullable.
   *
   *  2. Делаем несколько итераций последующих шагов. При этом в конце
   *     каждой итерации пересоздаем таблицу, меняя только признак nullable
   *     для одной из колонок.
   *
   *  2. Вставляем данные из контекста "коррелятора" для проверки
   *     что с таблицей все хорошо.
   *
   *  3. Параллельно открываем базу в контексте "командера" и изменяем
   *     схему таблицы.
   *
   *  4. Еще раз вставляем данные из контекста "коррелятора". При этом пробуем
   *     вставить строки/коретежи, как со значениями для всех колонок,
   *     так и все варианты отсутствия колонок.
   *
   *  5. Переходим к следующей итерации или освобождаем ресурсы.
   */

  // создаем исходную базу
  fpta_db *db_commander = nullptr;
  uint64_t db_initial_version;
  {
    // чистим
    if (REMOVE_FILE(testdb_name) != 0)
      ASSERT_EQ(ENOENT, errno);
    if (REMOVE_FILE(testdb_name_lck) != 0)
      ASSERT_EQ(ENOENT, errno);

    EXPECT_EQ(FPTA_OK, fpta_db_open(testdb_name, fpta_async, 0644, 1, true,
                                    &db_commander));
    ASSERT_NE(db_commander, (fpta_db *)nullptr);

    // описываем простейшую таблицу с двумя колонками
    fpta_column_set def;
    fpta_column_set_init(&def);
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe("pk", fptu_int64,
                                   fpta_primary_unique_ordered_obverse, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe(
                           "se", fptu_int64,
                           fpta_secondary_withdups_ordered_obverse, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def));

    // запускам транзакцию и создаем таблицу с обозначенным набором колонок
    fpta_txn *txn = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db_commander, fpta_schema, &txn));
    ASSERT_NE((fpta_txn *)nullptr, txn);
    ASSERT_EQ(FPTA_OK,
              fpta_transaction_versions(txn, &db_initial_version, nullptr));
    ASSERT_EQ(FPTA_OK, fpta_table_create(txn, "table", &def));
    ASSERT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  }

  // формируем четые строки-кортежа с разным заполнением значениями
  scoped_ptrw_guard pt_guard[4];
  fptu_rw *tuple_empty = fptu_alloc(2, 16);
  ASSERT_NE(tuple_empty, (fptu_rw *)nullptr);
  pt_guard[0].reset(tuple_empty);
  fptu_rw *tuple_pk_only = fptu_alloc(2, 16);
  ASSERT_NE(tuple_pk_only, (fptu_rw *)nullptr);
  pt_guard[1].reset(tuple_pk_only);
  fptu_rw *tuple_se_only = fptu_alloc(2, 16);
  ASSERT_NE(tuple_se_only, (fptu_rw *)nullptr);
  pt_guard[2].reset(tuple_se_only);
  fptu_rw *tuple_both = fptu_alloc(2, 16);
  ASSERT_NE(tuple_both, (fptu_rw *)nullptr);
  pt_guard[3].reset(tuple_both);

  // итоговые значения тегов-идентификаторов заведом известны,
  // поэтому заполняем кортежи без использования идентификаторов колонок,
  // но после проверим их (tuple_empty оставляем пустым).
  EXPECT_EQ(FPTU_OK, fptu_upsert_int64(tuple_pk_only, 0, 1));
  EXPECT_EQ(FPTU_OK, fptu_upsert_int64(tuple_se_only, 1, 2));
  EXPECT_EQ(FPTU_OK, fptu_upsert_int64(tuple_both, 0, 3));
  EXPECT_EQ(FPTU_OK, fptu_upsert_int64(tuple_both, 1, 4));

  // идентификаторы колонок для коммандера, они будут связаны
  // со средой/базой открытой из коммандера,
  // и НЕ должы использоваться в среде коррелятора.
  fpta_name cm_table, cm_col_pk, cm_col_se;
  EXPECT_EQ(FPTA_OK, fpta_table_init(&cm_table, "table"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&cm_table, &cm_col_pk, "pk"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&cm_table, &cm_col_se, "se"));

  // идентификаторы колонок для коррелятора, они будут связаны
  // со средой/базой открытой из коррелятора,
  // и НЕ должы использоваться в среде коммандера.
  fpta_name cr_table, cr_col_pk, cr_col_se;
  EXPECT_EQ(FPTA_OK, fpta_table_init(&cr_table, "table"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&cr_table, &cr_col_pk, "pk"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&cr_table, &cr_col_se, "se"));

  // открываем базу в "корреляторе"
  fpta_db *db_correlator = nullptr;
  EXPECT_EQ(FPTA_OK, fpta_db_open(testdb_name, fpta_async, 0644, 1, false,
                                  &db_correlator));

  // выполняем первое пробное обновление в корреляторе
  // обе колонки требуют значений
  {
    fpta_txn *txn_correlator = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db_correlator, fpta_write,
                                              &txn_correlator));

    // сверяем идентификаторы колонок
    EXPECT_EQ(FPTA_OK,
              fpta_name_refresh_couple(txn_correlator, &cr_table, &cr_col_pk));
    EXPECT_EQ(FPTA_OK,
              fpta_name_refresh_couple(txn_correlator, &cr_table, &cr_col_se));
    ASSERT_EQ(0, cr_col_pk.column.num);
    ASSERT_EQ(1, cr_col_se.column.num);
    EXPECT_EQ(db_initial_version + 0, cr_table.version);
    EXPECT_EQ(db_initial_version + 0, cr_col_pk.version);
    EXPECT_EQ(db_initial_version + 0, cr_col_se.version);

    EXPECT_EQ(FPTA_COLUMN_MISSING, fpta_insert_row(txn_correlator, &cr_table,
                                                   fptu_take(tuple_empty)));
    EXPECT_EQ(FPTA_COLUMN_MISSING,
              fpta_probe_and_insert_row(txn_correlator, &cr_table,
                                        fptu_take(tuple_pk_only)));
    EXPECT_EQ(FPTA_COLUMN_MISSING, fpta_insert_row(txn_correlator, &cr_table,
                                                   fptu_take(tuple_se_only)));
    EXPECT_EQ(FPTA_OK, fpta_insert_row(txn_correlator, &cr_table,
                                       fptu_take(tuple_both)));

    EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn_correlator, false));
  }

  // выполняем контрольное чтение и изменяем схему в "коммандоре"
  // первая колонка становится nullable
  {
    fpta_txn *txn_commander = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db_commander, fpta_schema,
                                              &txn_commander));
    ASSERT_NE((fpta_txn *)nullptr, txn_commander);

    // контрольное чтение
    fptu_ro row;
    fpta_value key = fpta_value_sint(3);
    EXPECT_EQ(FPTA_OK, fpta_get(txn_commander, &cm_col_pk, &key, &row));
    EXPECT_TRUE(fpta_is_same(row.sys, fptu_take(tuple_both).sys));

    // сверяем идентификаторы и версию схемы
    ASSERT_EQ(0, cm_col_pk.column.num);
    EXPECT_EQ(db_initial_version + 0, cm_table.version);
    EXPECT_EQ(db_initial_version + 0, cm_col_pk.version);
    // вторая колонка не использовалась и поэтому требует ручного обновления
    EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn_commander, &cm_col_se));
    EXPECT_EQ(db_initial_version + 0, cm_col_se.version);
    ASSERT_EQ(1, cm_col_se.column.num);

    // удаляем существующую таблицу
    EXPECT_EQ(FPTA_OK, fpta_table_drop(txn_commander, "table"));

    // описываем новую структуру таблицы
    fpta_column_set def;
    fpta_column_set_init(&def);
    EXPECT_EQ(FPTA_OK, fpta_column_describe(
                           "pk", fptu_int64,
                           fpta_primary_unique_ordered_obverse_nullable, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe(
                           "se", fptu_int64,
                           fpta_secondary_unique_ordered_obverse, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def));

    // создаем новую таблицу
    EXPECT_EQ(FPTA_OK, fpta_table_create(txn_commander, "table", &def));
    EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn_commander, false));
  }

  // выполняем второе контрольное обновление данных
  // после первого изменения схемы
  // сейчас первая колонка nullable, а вторая нет
  {
    fpta_txn *txn_correlator = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db_correlator, fpta_write,
                                              &txn_correlator));

    // делаем пробные вставки
    // теперь первая (pk) колонка nullable, но вторая (se) еще нет
    EXPECT_EQ(FPTA_COLUMN_MISSING,
              fpta_probe_and_insert_row(txn_correlator, &cr_table,
                                        fptu_take(tuple_empty)));
    EXPECT_EQ(FPTA_COLUMN_MISSING,
              fpta_probe_and_insert_row(txn_correlator, &cr_table,
                                        fptu_take(tuple_pk_only)));
    EXPECT_EQ(FPTA_OK, fpta_insert_row(txn_correlator, &cr_table,
                                       fptu_take(tuple_se_only)));
    EXPECT_EQ(FPTA_OK, fpta_insert_row(txn_correlator, &cr_table,
                                       fptu_take(tuple_both)));

    // сверяем идентификатор таблицы, он должен был обновиться автоматически,
    // а идентификаторы колонок - нет
    EXPECT_EQ(db_initial_version + 2, cr_table.version);
    EXPECT_EQ(db_initial_version + 0, cr_col_pk.version);
    EXPECT_EQ(db_initial_version + 0, cr_col_se.version);
    // обновляем и сверяем идентификаторы колонок
    EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn_correlator, &cr_col_pk));
    EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn_correlator, &cr_col_se));
    EXPECT_EQ(db_initial_version + 2, cr_table.version);
    EXPECT_EQ(db_initial_version + 2, cr_col_pk.version);
    EXPECT_EQ(db_initial_version + 2, cr_col_se.version);
    EXPECT_EQ(0, cr_col_pk.column.num);
    EXPECT_EQ(1, cr_col_se.column.num);

    EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn_correlator, false));
  }

  // выполняем третье контрольное чтение
  // и второй раз изменяем схему в "коммандоре"
  // вторая колонка также становится nullable
  {
    fpta_txn *txn_commander = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db_commander, fpta_schema,
                                              &txn_commander));
    ASSERT_NE((fpta_txn *)nullptr, txn_commander);

    // контрольное чтение
    fptu_ro row;
    fpta_value key = fpta_value_sint(4);
    EXPECT_EQ(FPTA_OK, fpta_get(txn_commander, &cm_col_se, &key, &row));
    EXPECT_TRUE(fpta_is_same(row.sys, fptu_take(tuple_both).sys));
    key = fpta_value_sint(2);
    EXPECT_EQ(FPTA_OK, fpta_get(txn_commander, &cm_col_se, &key, &row));
    EXPECT_TRUE(fpta_is_same(row.sys, fptu_take(tuple_se_only).sys));

    // сверяем идентификаторы и версию схемы
    ASSERT_EQ(0, cm_col_pk.column.num);
    EXPECT_EQ(db_initial_version + 2, cm_table.version);
    EXPECT_EQ(db_initial_version + 2, cm_col_se.version);
    // первая колонка не использовалась и поэтому требует ручного обновления
    EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn_commander, &cm_col_pk));
    EXPECT_EQ(db_initial_version + 2, cm_col_pk.version);
    ASSERT_EQ(1, cm_col_se.column.num);

    // удаляем существующую таблицу
    EXPECT_EQ(FPTA_OK, fpta_table_drop(txn_commander, "table"));

    // описываем новую структуру таблицы
    fpta_column_set def;
    fpta_column_set_init(&def);
    EXPECT_EQ(FPTA_OK, fpta_column_describe(
                           "pk", fptu_int64,
                           fpta_primary_unique_ordered_obverse_nullable, &def));
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe(
                  "se", fptu_int64,
                  fpta_secondary_unique_ordered_obverse_nullable, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def));

    // создаем новую таблицу
    EXPECT_EQ(FPTA_OK, fpta_table_create(txn_commander, "table", &def));
    EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn_commander, false));
  }

  // выполняем третье контрольное обновление данных
  // после второго изменения схемы
  // сейчас обе колонки nullable
  {
    fpta_txn *txn_correlator = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db_correlator, fpta_write,
                                              &txn_correlator));

    // делаем пробные вставки, теперь обе колонки nullable
    EXPECT_EQ(FPTA_OK, fpta_insert_row(txn_correlator, &cr_table,
                                       fptu_take(tuple_pk_only)));
    EXPECT_EQ(FPTA_OK, fpta_probe_and_insert_row(txn_correlator, &cr_table,
                                                 fptu_take(tuple_se_only)));
    EXPECT_EQ(FPTA_OK, fpta_insert_row(txn_correlator, &cr_table,
                                       fptu_take(tuple_both)));
    // попытку вставки двух пустых значений делаем последней,
    // иначе сможем вставить другие комбинации с NIL.
    EXPECT_EQ(FPTA_KEYEXIST, fpta_insert_row(txn_correlator, &cr_table,
                                             fptu_take(tuple_empty)));

    // сверяем идентификатор таблицы, он должен был обновиться автоматически,
    // а идентификаторы колонок - нет
    EXPECT_EQ(db_initial_version + 4, cr_table.version);
    EXPECT_EQ(db_initial_version + 2, cr_col_pk.version);
    EXPECT_EQ(db_initial_version + 2, cr_col_se.version);
    // обновляем и сверяем идентификаторы колонок
    EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn_correlator, &cr_col_pk));
    EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn_correlator, &cr_col_se));
    EXPECT_EQ(db_initial_version + 4, cr_table.version);
    EXPECT_EQ(db_initial_version + 4, cr_col_pk.version);
    EXPECT_EQ(db_initial_version + 4, cr_col_se.version);
    EXPECT_EQ(0, cr_col_pk.column.num);
    EXPECT_EQ(1, cr_col_se.column.num);

    EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn_correlator, false));
  }

  // выполняем четверное контрольное чтение
  // и в третий раз изменяем схему в "коммандоре"
  // первая колонка становится не-nullable, вторая остается nullable
  {
    fpta_txn *txn_commander = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db_commander, fpta_schema,
                                              &txn_commander));
    ASSERT_NE((fpta_txn *)nullptr, txn_commander);

    // контрольное чтение
    fptu_ro row;
    fpta_value key = fpta_value_sint(4);
    EXPECT_EQ(FPTA_OK, fpta_get(txn_commander, &cm_col_se, &key, &row));
    EXPECT_TRUE(fpta_is_same(row.sys, fptu_take(tuple_both).sys));
    key = fpta_value_sint(2);
    EXPECT_EQ(FPTA_OK, fpta_get(txn_commander, &cm_col_se, &key, &row));
    EXPECT_TRUE(fpta_is_same(row.sys, fptu_take(tuple_se_only).sys));
    key = fpta_value_sint(1);
    EXPECT_EQ(FPTA_OK, fpta_get(txn_commander, &cm_col_pk, &key, &row));
    EXPECT_TRUE(fpta_is_same(row.sys, fptu_take(tuple_pk_only).sys));

    // сверяем идентификаторы и версию схемы
    ASSERT_EQ(0, cm_col_pk.column.num);
    EXPECT_EQ(db_initial_version + 4, cm_table.version);
    EXPECT_EQ(db_initial_version + 4, cm_col_pk.version);
    EXPECT_EQ(db_initial_version + 4, cm_col_se.version);
    ASSERT_EQ(1, cm_col_se.column.num);

    // удаляем существующую таблицу
    EXPECT_EQ(FPTA_OK, fpta_table_drop(txn_commander, "table"));

    // описываем новую структуру таблицы
    fpta_column_set def;
    fpta_column_set_init(&def);
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe("pk", fptu_int64,
                                   fpta_primary_unique_ordered_obverse, &def));
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe(
                  "se", fptu_int64,
                  fpta_secondary_unique_ordered_obverse_nullable, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def));

    // создаем новую таблицу
    EXPECT_EQ(FPTA_OK, fpta_table_create(txn_commander, "table", &def));
    EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn_commander, false));
  }

  // выполняем контрольное обновление данных после третьего изменения схемы
  // сейчас первая колонка снова не-nullable, а вторая еще nullable
  {
    fpta_txn *txn_correlator = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db_correlator, fpta_write,
                                              &txn_correlator));

    // делаем пробные вставки
    // теперь первая (pk) колонка не-nullable, но вторая (se) еще nullable
    EXPECT_EQ(FPTA_COLUMN_MISSING,
              fpta_probe_and_insert_row(txn_correlator, &cr_table,
                                        fptu_take(tuple_empty)));
    EXPECT_EQ(FPTA_OK, fpta_insert_row(txn_correlator, &cr_table,
                                       fptu_take(tuple_pk_only)));
    EXPECT_EQ(FPTA_COLUMN_MISSING,
              fpta_probe_and_insert_row(txn_correlator, &cr_table,
                                        fptu_take(tuple_se_only)));
    EXPECT_EQ(FPTA_OK, fpta_insert_row(txn_correlator, &cr_table,
                                       fptu_take(tuple_both)));

    // сверяем идентификатор таблицы, он должен был обновиться автоматически,
    // а идентификаторы колонок - нет
    EXPECT_EQ(db_initial_version + 6, cr_table.version);
    EXPECT_EQ(db_initial_version + 4, cr_col_pk.version);
    EXPECT_EQ(db_initial_version + 4, cr_col_se.version);
    // обновляем и сверяем идентификаторы колонок
    EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn_correlator, &cr_col_pk));
    EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn_correlator, &cr_col_se));
    EXPECT_EQ(db_initial_version + 6, cr_table.version);
    EXPECT_EQ(db_initial_version + 6, cr_col_pk.version);
    EXPECT_EQ(db_initial_version + 6, cr_col_se.version);
    EXPECT_EQ(0, cr_col_pk.column.num);
    EXPECT_EQ(1, cr_col_se.column.num);

    EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn_correlator, false));
  }

  // выполняем пятое контрольное чтение
  // и в четверный (последний) раз изменяем схему в "коммандоре"
  // вторая колонка становится не-nullable
  {
    fpta_txn *txn_commander = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db_commander, fpta_schema,
                                              &txn_commander));
    ASSERT_NE((fpta_txn *)nullptr, txn_commander);

    // контрольное чтение
    fptu_ro row;
    fpta_value key = fpta_value_sint(4);
    EXPECT_EQ(FPTA_OK, fpta_get(txn_commander, &cm_col_se, &key, &row));
    EXPECT_TRUE(fpta_is_same(row.sys, fptu_take(tuple_both).sys));
    key = fpta_value_sint(1);
    EXPECT_EQ(FPTA_OK, fpta_get(txn_commander, &cm_col_pk, &key, &row));
    EXPECT_TRUE(fpta_is_same(row.sys, fptu_take(tuple_pk_only).sys));

    // сверяем идентификаторы и версию схемы
    ASSERT_EQ(0, cm_col_pk.column.num);
    EXPECT_EQ(db_initial_version + 6, cm_table.version);
    EXPECT_EQ(db_initial_version + 6, cm_col_pk.version);
    EXPECT_EQ(db_initial_version + 6, cm_col_se.version);
    ASSERT_EQ(1, cm_col_se.column.num);

    // удаляем существующую таблицу
    EXPECT_EQ(FPTA_OK, fpta_table_drop(txn_commander, "table"));

    // описываем новую структуру таблицы
    fpta_column_set def;
    fpta_column_set_init(&def);
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe("pk", fptu_int64,
                                   fpta_primary_unique_ordered_obverse, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe(
                           "se", fptu_int64,
                           fpta_secondary_unique_ordered_obverse, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def));

    // создаем новую таблицу
    EXPECT_EQ(FPTA_OK, fpta_table_create(txn_commander, "table", &def));
    EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn_commander, false));
  }

  // выполняем пятое (последнее) пробное обновление в корреляторе
  // сейчас сноыав обе колонки требуют значений
  {
    fpta_txn *txn_correlator = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db_correlator, fpta_write,
                                              &txn_correlator));

    EXPECT_EQ(FPTA_COLUMN_MISSING, fpta_insert_row(txn_correlator, &cr_table,
                                                   fptu_take(tuple_empty)));
    EXPECT_EQ(FPTA_COLUMN_MISSING,
              fpta_probe_and_insert_row(txn_correlator, &cr_table,
                                        fptu_take(tuple_pk_only)));
    EXPECT_EQ(FPTA_COLUMN_MISSING, fpta_insert_row(txn_correlator, &cr_table,
                                                   fptu_take(tuple_se_only)));
    EXPECT_EQ(FPTA_OK, fpta_insert_row(txn_correlator, &cr_table,
                                       fptu_take(tuple_both)));

    // сверяем идентификаторы колонок
    ASSERT_EQ(0, cr_col_pk.column.num);
    ASSERT_EQ(1, cr_col_se.column.num);
    EXPECT_EQ(db_initial_version + 8, cr_table.version);
    // идентификаторы колонок не использовались с прошлой транзакции
    EXPECT_EQ(db_initial_version + 6, cr_col_pk.version);
    EXPECT_EQ(db_initial_version + 6, cr_col_se.version);
    EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn_correlator, &cr_col_pk));
    EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn_correlator, &cr_col_se));
    EXPECT_EQ(db_initial_version + 8, cr_col_pk.version);
    EXPECT_EQ(db_initial_version + 8, cr_col_se.version);

    EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn_correlator, false));
  }

  // разрушаем идентифкаторы коррелятора
  fpta_name_destroy(&cr_col_pk);
  fpta_name_destroy(&cr_col_se);
  fpta_name_destroy(&cr_table);
  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db_correlator));

  // выполняем контрольное чтение и изменяем схему в "коммандоре"
  // первая колонка становится nullable
  {
    fpta_txn *txn_commander = nullptr;
    EXPECT_EQ(FPTA_OK,
              fpta_transaction_begin(db_commander, fpta_read, &txn_commander));
    ASSERT_NE((fpta_txn *)nullptr, txn_commander);

    // контрольное чтение
    fptu_ro row;
    fpta_value key = fpta_value_sint(4);
    EXPECT_EQ(FPTA_OK, fpta_get(txn_commander, &cm_col_se, &key, &row));
    EXPECT_TRUE(fpta_is_same(row.sys, fptu_take(tuple_both).sys));
    key = fpta_value_sint(3);
    EXPECT_EQ(FPTA_OK, fpta_get(txn_commander, &cm_col_pk, &key, &row));
    EXPECT_TRUE(fpta_is_same(row.sys, fptu_take(tuple_both).sys));

    // сверяем идентификаторы и версию схемы
    ASSERT_EQ(0, cm_col_pk.column.num);
    EXPECT_EQ(db_initial_version + 8, cm_table.version);
    EXPECT_EQ(db_initial_version + 8, cm_col_pk.version);
    EXPECT_EQ(db_initial_version + 8, cm_col_se.version);
    ASSERT_EQ(1, cm_col_se.column.num);

    EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn_commander, false));
  }

  // разрушаем идентифкаторы коммандера
  fpta_name_destroy(&cm_col_pk);
  fpta_name_destroy(&cm_col_se);
  fpta_name_destroy(&cm_table);
  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db_commander));
}

//----------------------------------------------------------------------------

TEST(Nullable, SchemaReloadAfterAbort) {
  /* FIXME: Описание сценария теста */

  // чистим
  if (REMOVE_FILE(testdb_name) != 0)
    ASSERT_EQ(ENOENT, errno);
  if (REMOVE_FILE(testdb_name_lck) != 0)
    ASSERT_EQ(ENOENT, errno);

  fpta_db *db_correlator = nullptr;
  EXPECT_EQ(FPTA_OK, fpta_db_open(testdb_name, fpta_async, 0644, 1, false,
                                  &db_correlator));
  ASSERT_NE(db_correlator, (fpta_db *)nullptr);

  fpta_db *db_commander = nullptr;
  EXPECT_EQ(FPTA_OK, fpta_db_open(testdb_name, fpta_async, 0644, 1, true,
                                  &db_commander));
  ASSERT_NE(db_commander, (fpta_db *)nullptr);

  { // create table in commander
    fpta_column_set def;
    fpta_column_set_init(&def);
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe("host", fptu_cstr,
                                   fpta_primary_unique_ordered_obverse, &def));
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe("port", fptu_int64, fpta_index_none, &def));
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe("user", fptu_cstr, fpta_index_none, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe("date", fptu_datetime,
                                            fpta_index_none, &def));
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe("_id", fptu_int64,
                                   fpta_secondary_unique_unordered, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe(
                           "_last_changed", fptu_datetime,
                           fpta_secondary_withdups_ordered_obverse, &def));

    EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def));

    fpta_txn *txn = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db_commander, fpta_schema, &txn));
    ASSERT_NE((fpta_txn *)nullptr, txn);
    EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "table", &def));
    EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  }
  EXPECT_EQ(FPTA_OK, fpta_db_close(db_commander));
  db_commander = nullptr;

  { // try to fill table in correlator
    fpta_name table, host, port, user, date, id, lc;
    EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "table"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &host, "host"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &port, "port"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &user, "user"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &date, "date"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &id, "_id"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &lc, "_last_changed"));

    fpta_txn *txn = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db_correlator, fpta_write, &txn));
    ASSERT_NE((fpta_txn *)nullptr, txn);

    fpta_name_refresh(txn, &table);
    fptu_ro record;
    memset(&record, 0, sizeof(fptu_ro));

    fpta_value value = fpta_value_cstr("127.0.0.1");

    // no need to refresh column name because it will be refreshed inside
    // fpta_get
    EXPECT_EQ(FPTA_NOTFOUND, fpta_get(txn, &host, &value, &record));
    size_t row_count = 0;
    EXPECT_EQ(FPTA_OK, fpta_table_info(txn, &table, &row_count, nullptr));
    EXPECT_EQ(size_t(0), row_count);

    fptu_rw *tuple = fptu_alloc(6, 1024);
    ASSERT_NE(tuple, (fptu_rw *)nullptr);

    EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &host));
    EXPECT_EQ(FPTA_OK,
              fpta_upsert_column(tuple, &host, fpta_value_cstr("127.0.0.1")));
    // EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &port));
    // EXPECT_EQ(FPTA_OK, fpta_upsert_column(tuple, &port,
    // fpta_value_sint(100)));
    // EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &user));
    // EXPECT_EQ(FPTA_OK, fpta_upsert_column(tuple, &user,
    // fpta_value_cstr("user")));
    // EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &date));
    // EXPECT_EQ(FPTA_OK, fpta_upsert_column(tuple, &date,
    // fpta_value_datetime(fptu_now_fine())));

    uint64_t seq = 0;
    EXPECT_EQ(FPTA_OK, fpta_table_sequence(txn, &table, &seq, 1));
    EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &id));
    EXPECT_EQ(FPTA_OK, fpta_upsert_column(tuple, &id, fpta_value_uint(seq)));

    EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &lc));
    EXPECT_EQ(FPTA_OK, fpta_upsert_column(
                           tuple, &lc, fpta_value_datetime(fptu_now_fine())));

    EXPECT_EQ(FPTA_COLUMN_MISSING,
              fpta_probe_and_upsert_row(txn, &table, fptu_take(tuple)));

    fptu_clear(tuple);
    free(tuple);

    EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, true));

    fpta_name_destroy(&table);
    fpta_name_destroy(&host);
    fpta_name_destroy(&port);
    fpta_name_destroy(&user);
    fpta_name_destroy(&date);
    fpta_name_destroy(&id);
    fpta_name_destroy(&lc);
  }

  EXPECT_EQ(FPTA_OK, fpta_db_open(testdb_name, fpta_async, 0644, 1, true,
                                  &db_commander));
  ASSERT_NE(db_commander, (fpta_db *)nullptr);
  { // drop and recreate table in commander
    fpta_txn *txn = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db_commander, fpta_schema, &txn));
    ASSERT_NE((fpta_txn *)nullptr, txn);
    EXPECT_EQ(FPTA_OK, fpta_table_drop(txn, "table"));
    EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));

    fpta_column_set def;
    fpta_column_set_init(&def);
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe("host", fptu_cstr,
                                   fpta_primary_unique_ordered_obverse, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe("port", fptu_int64,
                                            fpta_index_fnullable, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe("user", fptu_cstr,
                                            fpta_index_fnullable, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe("date", fptu_datetime,
                                            fpta_index_fnullable, &def));
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe("_id", fptu_int64,
                                   fpta_secondary_unique_unordered, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe(
                           "_last_changed", fptu_datetime,
                           fpta_secondary_withdups_ordered_obverse, &def));

    EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def));

    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db_commander, fpta_schema, &txn));
    ASSERT_NE((fpta_txn *)nullptr, txn);
    EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "table", &def));
    EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  }
  EXPECT_EQ(FPTA_OK, fpta_db_close(db_commander));
  db_commander = nullptr;

  { // try to fill table in correlator
    fpta_name table, host, port, user, date, id, lc;
    EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "table"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &host, "host"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &port, "port"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &user, "user"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &date, "date"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &id, "_id"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &lc, "_last_changed"));

    fpta_txn *txn = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db_correlator, fpta_write, &txn));
    ASSERT_NE((fpta_txn *)nullptr, txn);

    EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &table));
    fptu_ro record;
    memset(&record, 0, sizeof(fptu_ro));

    fpta_value value = fpta_value_cstr("127.0.0.1");

    // no need to refresh column name because it will be refreshed inside
    // fpta_get
    EXPECT_EQ(FPTA_NOTFOUND, fpta_get(txn, &host, &value, &record));
    size_t row_count = 0;
    EXPECT_EQ(FPTA_OK, fpta_table_info(txn, &table, &row_count, nullptr));
    EXPECT_EQ(size_t(0), row_count);

    fptu_rw *tuple = fptu_alloc(6, 1024);
    ASSERT_NE(tuple, (fptu_rw *)nullptr);

    EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &host));
    EXPECT_EQ(FPTA_OK,
              fpta_upsert_column(tuple, &host, fpta_value_cstr("127.0.0.1")));
    // EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &port));
    // EXPECT_EQ(FPTA_OK, fpta_upsert_column(tuple, &port,
    // fpta_value_sint(100)));
    // EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &user));
    // EXPECT_EQ(FPTA_OK, fpta_upsert_column(tuple, &user,
    // fpta_value_cstr("user")));
    // EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &date));
    // EXPECT_EQ(FPTA_OK, fpta_upsert_column(tuple, &date,
    // fpta_value_datetime(fptu_now_fine())));

    uint64_t seq = 0;
    EXPECT_EQ(FPTA_OK, fpta_table_sequence(txn, &table, &seq, 1));
    EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &id));
    EXPECT_EQ(FPTA_OK, fpta_upsert_column(tuple, &id, fpta_value_uint(seq)));

    EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &lc));
    EXPECT_EQ(FPTA_OK, fpta_upsert_column(
                           tuple, &lc, fpta_value_datetime(fptu_now_fine())));

    EXPECT_EQ(FPTA_OK,
              fpta_probe_and_upsert_row(txn, &table, fptu_take(tuple)));

    fptu_clear(tuple);
    free(tuple);

    EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, true));

    fpta_name_destroy(&table);
    fpta_name_destroy(&host);
    fpta_name_destroy(&port);
    fpta_name_destroy(&user);
    fpta_name_destroy(&date);
    fpta_name_destroy(&id);
    fpta_name_destroy(&lc);
  }

  EXPECT_EQ(FPTA_OK, fpta_db_close(db_correlator));
}

//----------------------------------------------------------------------------

static std::string random_string(int len) {
  static std::string alphabet = "abcdefghijklmnopqrstuvwxyz0123456789";
  std::string result;
  for (int i = 0; i < len; ++i)
    result.push_back(alphabet[rand() % alphabet.length()]);
  return result;
}

TEST(CRUD, DISABLED_ExtraOps) {
  /* FIXME: Описание сценария теста */

  // чистим
  if (REMOVE_FILE(testdb_name) != 0)
    ASSERT_EQ(ENOENT, errno);
  if (REMOVE_FILE(testdb_name_lck) != 0)
    ASSERT_EQ(ENOENT, errno);

  fpta_db *db_correlator = nullptr;
  EXPECT_EQ(FPTA_OK, fpta_db_open(testdb_name, fpta_async, 0644, 20, true,
                                  &db_correlator));
  ASSERT_NE(db_correlator, (fpta_db *)nullptr);

  { // create table
    fpta_column_set def;
    fpta_column_set_init(&def);
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe("uuidfield", fptu_cstr,
                                   fpta_primary_unique_ordered_reverse, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe("dst_ip", fptu_cstr,
                                            fpta_noindex_nullable, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe("port", fptu_int64,
                                            fpta_noindex_nullable, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe("date", fptu_datetime,
                                            fpta_noindex_nullable, &def));
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe("_id", fptu_int64,
                                   fpta_secondary_unique_unordered, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe(
                           "_last_changed", fptu_datetime,
                           fpta_secondary_withdups_ordered_obverse, &def));

    EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def));

    fpta_txn *txn = nullptr;
    EXPECT_EQ(FPTA_OK,
              fpta_transaction_begin(db_correlator, fpta_schema, &txn));
    ASSERT_NE((fpta_txn *)nullptr, txn);
    EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "table", &def));
    EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  }
  EXPECT_EQ(FPTA_OK, fpta_db_close(db_correlator));

  EXPECT_EQ(FPTA_OK, fpta_db_open(testdb_name, fpta_async, 0644, 30, false,
                                  &db_correlator));
  int i = 0;
  for (; i < 1500000; ++i) { // try to fill table
    fpta_txn *txn = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db_correlator, fpta_write, &txn));
    ASSERT_NE((fpta_txn *)nullptr, txn);

    fpta_name table, uuid, dst_ip, port, date, id, lc;
    EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "table"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &uuid, "uuidfield"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &dst_ip, "dst_ip"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &port, "port"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &date, "date"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &id, "_id"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &lc, "_last_changed"));

    fpta_name_refresh(txn, &table);
    fptu_ro record;
    memset(&record, 0, sizeof(fptu_ro));

    const auto string_holder = random_string(36);
    fpta_value value = fpta_value_str(string_holder);
    EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &uuid));
    int rc = fpta_get(txn, &uuid, &value, &record);
    EXPECT_TRUE(rc == FPTA_OK || rc == FPTA_NOTFOUND);

    size_t row_count = 0;
    EXPECT_EQ(FPTA_OK, fpta_table_info(txn, &table, &row_count, nullptr));
    if (row_count > 50000) {
      EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &lc));
      fpta_cursor *cursor = nullptr;

      ASSERT_EQ(FPTA_OK,
                fpta_cursor_open(txn, &lc, fpta_value_begin(), fpta_value_end(),
                                 nullptr, fpta_ascending_dont_fetch, &cursor));
      EXPECT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_first));

      size_t deleted = 0;
      for (size_t j = 0; j < 20000; ++j) {
        EXPECT_EQ(FPTA_OK, fpta_cursor_delete(cursor));
        ++deleted;
      }
      ASSERT_EQ(FPTA_OK, fpta_cursor_close(cursor));
    }

    fptu_rw *tuple = fptu_alloc(5, 110);
    ASSERT_NE(tuple, (fptu_rw *)nullptr);

    EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &uuid));
    EXPECT_EQ(FPTA_OK, fpta_upsert_column(tuple, &uuid, value));
    // EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &port));
    // EXPECT_EQ(FPTA_OK, fpta_upsert_column(tuple, &port,
    // fpta_value_sint(100)));
    EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &dst_ip));
    std::string ip = random_string(7 + (rand() % 9));
    EXPECT_EQ(FPTA_OK,
              fpta_upsert_column(tuple, &dst_ip, fpta_value_cstr(ip.c_str())));
    EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &date));
    EXPECT_EQ(FPTA_OK, fpta_upsert_column(
                           tuple, &date, fpta_value_datetime(fptu_now_fine())));

    uint64_t seq = 0;
    EXPECT_EQ(FPTA_OK, fpta_table_sequence(txn, &table, &seq, 1));
    EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &id));
    EXPECT_EQ(FPTA_OK, fpta_upsert_column(tuple, &id, fpta_value_uint(seq)));

    EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &lc));
    EXPECT_EQ(FPTA_OK, fpta_upsert_column(
                           tuple, &lc, fpta_value_datetime(fptu_now_fine())));

    EXPECT_EQ(FPTA_OK,
              fpta_probe_and_upsert_row(txn, &table, fptu_take(tuple)));

    fptu_clear(tuple);
    free(tuple);

    fpta_name_destroy(&table);
    fpta_name_destroy(&uuid);
    fpta_name_destroy(&port);
    fpta_name_destroy(&dst_ip);
    fpta_name_destroy(&date);
    fpta_name_destroy(&id);
    fpta_name_destroy(&lc);

    EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  }
  EXPECT_EQ(FPTA_OK, fpta_db_close(db_correlator));
}

//----------------------------------------------------------------------------

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

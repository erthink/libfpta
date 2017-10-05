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
 * Использовать тут большие значения смысла нет. Время работы тестов
 * растет примерно линейно (чуть быстрее), тогда как вероятность
 * проявления каких-либо ошибок растет в лучшем случае как Log(NNN),
 * а скорее даже как SquareRoot(Log(NNN)).
 */
#ifdef FPTA_INDEX_UT_LONG
static constexpr int NNN = 65521; // около 1-2 минуты в /dev/shm/
#else
static constexpr int NNN = 509; // менее секунды в /dev/shm/
#endif

static const char testdb_name[] = TEST_DB_DIR "ut_index_primary.fpta";
static const char testdb_name_lck[] =
    TEST_DB_DIR "ut_index_primary.fpta" MDBX_LOCK_SUFFIX;

template <fptu_type type, fpta_index_type index> void TestPrimary() {
  /* Тест первичных (primary) индексов.
   *
   * Сценарий (общий для всех типов полей и всех видов первичных индексов):
   *  1. Создается тестовая база с одной таблицей, в которой четыре колонки:
   *     - PK (primary key) с типом, для которого производится тестирование
   *       работы индекса.
   *     - Колонка "order", в которую записывается контрольный (ожидаемый)
   *       порядковый номер следования строки, при сортировке по PK и
   *       проверяемому виду индекса.
   *     - Колонка "dup_id", которая используется для идентификации дубликатов
   *       индексов допускающих не-уникальные ключи.
   *     - Колонка "t1ha", в которую записывается "контрольная сумма" от
   *       ожидаемого порядка строки, типа PK и вида индекса. Принципиальной
   *       необходимости в этой колонке нет, она используется как
   *       "утяжелитель", а также для дополнительного контроля.
   *
   *  2. Тест также используется для недопустимых комбинаций индексируемого
   *     типа и видов индекса. В этом случае проверяется что формирование
   *     описания колонок таблицы завершается с соответствующими ошибками.
   *
   *  3. Таблица заполняется строками, значение PK в которых генерируется
   *     соответствующим генератором значений:
   *     - Сами генераторы проверяются в одном из тестов 0corny.
   *     - Для индексов без уникальности для каждого ключа вставляется две
   *       строки с разным dup_id.
   *     - При заполнении таблицы производятся попытки обновить
   *       несуществующую запись, вставить дубликат по ключу или полный
   *       дубликат записи, выполнить upsert при не уникальном PK.
   *
   *  4. В отдельной транзакции открывается курсор, через который проверяется:
   *      - Итоговое количество строк.
   *      - Перемещение на последнюю и на первую строку.
   *      - Итерирование строк с контролем их порядок, в том числе порядок
   *        следования дубликатов.
   *
   *  5. Завершаются операции и освобождаются ресурсы.
   */
  CHECK_RUNTIME_LIMIT_OR_SKIP();
  const bool valid = is_valid4primary(type, index);
  scoped_db_guard db_quard;
  scoped_txn_guard txn_guard;

  // нужно простое число, иначе сломается переупорядочивание
  ASSERT_TRUE(isPrime(NNN));
  // иначе не сможем проверить fptu_uint16
  ASSERT_GE(65535, NNN);

  SCOPED_TRACE("type " + std::to_string(type) + ", index " +
               std::to_string(index) +
               (valid ? ", (valid case)" : ", (invalid case)"));
  // создаем четыре колонки: pk, order, t1ha и dup_id
  fpta_column_set def;
  fpta_column_set_init(&def);

  const std::string pk_col_name = "pk_" + std::to_string(type);
  if (valid) {
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe(pk_col_name.c_str(), type, index, &def));
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe("order", fptu_int32, fpta_index_none, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe("dup_id", fptu_uint16,
                                            fpta_noindex_nullable, &def));
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe("t1ha", fptu_uint64, fpta_index_none, &def));
    ASSERT_EQ(FPTA_OK, fpta_column_set_validate(&def));
  } else {
    EXPECT_NE(FPTA_OK,
              fpta_column_describe(pk_col_name.c_str(), type, index, &def));
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe("order", fptu_int32, fpta_index_none, &def));
    ASSERT_NE(FPTA_OK, fpta_column_set_validate(&def));

    // разрушаем описание таблицы
    EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def));
    EXPECT_NE(FPTA_OK, fpta_column_set_validate(&def));
    return;
  }

  // чистим
  if (REMOVE_FILE(testdb_name) != 0)
    ASSERT_EQ(ENOENT, errno);
  if (REMOVE_FILE(testdb_name_lck) != 0)
    ASSERT_EQ(ENOENT, errno);

#ifdef FPTA_INDEX_UT_LONG
  // пытаемся обойтись меньшей базой, но для строк потребуется больше места
  unsigned megabytes = 16;
  if (type > fptu_128)
    megabytes = 20;
  if (type > fptu_256)
    megabytes = 32;
#else
  const unsigned megabytes = 1;
#endif

  fpta_db *db = nullptr;
  EXPECT_EQ(FPTA_SUCCESS,
            fpta_db_open(testdb_name, fpta_async, 0644, megabytes, true, &db));
  ASSERT_NE(nullptr, db);
  db_quard.reset(db);

  // создаем таблицу
  fpta_txn *txn = nullptr;
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
  ASSERT_NE(nullptr, txn);
  txn_guard.reset(txn);
  ASSERT_EQ(FPTA_OK, fpta_table_create(txn, "table", &def));
  ASSERT_EQ(FPTA_OK, fpta_transaction_end(txn_guard.release(), false));
  txn = nullptr;

  // разрушаем описание таблицы
  EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def));
  EXPECT_NE(FPTA_OK, fpta_column_set_validate(&def));

  // инициализируем идентификаторы колонок
  fpta_name table, col_pk, col_order, col_dup_id, col_t1ha;
  EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "table"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_pk, pk_col_name.c_str()));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_order, "order"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_dup_id, "dup_id"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_t1ha, "t1ha"));

  //------------------------------------------------------------------------

  // начинаем транзакцию записи
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_write, &txn));
  ASSERT_NE(nullptr, txn);
  txn_guard.reset(txn);

  // связываем идентификаторы с ранее созданной схемой
  ASSERT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &col_pk));
  ASSERT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_order));
  ASSERT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_dup_id));
  ASSERT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_t1ha));

  fptu_rw *row = fptu_alloc(4, fpta_max_keylen * 2 + 4 + 4);
  ASSERT_NE(nullptr, row);
  ASSERT_STREQ(nullptr, fptu_check(row));

  any_keygen keygen(type, index);
  unsigned n = 0;
  for (int order = 0; order < NNN; ++order) {
    SCOPED_TRACE("order " + std::to_string(order));
    fpta_value value_pk = keygen.make(order, NNN);
    if (value_pk.type == fpta_end)
      break;
    if (value_pk.type == fpta_begin)
      continue;

    // теперь формируем кортеж
    ASSERT_EQ(FPTU_OK, fptu_clear(row));
    ASSERT_STREQ(nullptr, fptu_check(row));
    ASSERT_EQ(FPTA_OK,
              fpta_upsert_column(row, &col_order, fpta_value_sint(order)));
    ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_pk, value_pk));
    // t1ha как "checksum" для order
    ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_t1ha,
                                          order_checksum(order, type, index)));

    // пытаемся обновить несуществующую запись
    ASSERT_EQ(FPTA_NOTFOUND,
              fpta_update_row(txn, &table, fptu_take_noshrink(row)));

    if (fpta_index_is_unique(index)) {
      // вставляем
      ASSERT_EQ(FPTA_OK, fpta_insert_row(txn, &table, fptu_take_noshrink(row)));
      n++;
      // проверяем что полный дубликат не вставляется
      ASSERT_EQ(FPTA_KEYEXIST,
                fpta_insert_row(txn, &table, fptu_take_noshrink(row)));

      // обновляем dup_id и проверям что дубликат по ключу не проходит
      ASSERT_EQ(FPTA_OK,
                fpta_upsert_column(row, &col_dup_id, fpta_value_uint(1)));
      ASSERT_EQ(FPTA_KEYEXIST,
                fpta_insert_row(txn, &table, fptu_take_noshrink(row)));

      // проверяем что upsert и update работают,
      // сначала upsert с текущим dup_id = 1
      ASSERT_EQ(FPTA_OK, fpta_upsert_row(txn, &table, fptu_take_noshrink(row)));
      // теперь update c dup_id = 42, должен остаться только этот
      // вариант
      ASSERT_EQ(FPTA_OK,
                fpta_upsert_column(row, &col_dup_id, fpta_value_uint(42)));
      ASSERT_EQ(FPTA_OK, fpta_update_row(txn, &table, fptu_take_noshrink(row)));
    } else {
      // вставляем c dup_id = 0
      ASSERT_EQ(FPTA_OK,
                fpta_upsert_column(row, &col_dup_id, fpta_value_uint(0)));
      ASSERT_EQ(FPTA_OK, fpta_insert_row(txn, &table, fptu_take_noshrink(row)));
      n++;
      // проверяем что полный дубликат не вставляется
      ASSERT_EQ(FPTA_KEYEXIST,
                fpta_insert_row(txn, &table, fptu_take_noshrink(row)));

      // обновляем dup_id и вставляем дубль по ключу
      ASSERT_EQ(FPTA_OK,
                fpta_upsert_column(row, &col_dup_id, fpta_value_uint(1)));
      ASSERT_EQ(FPTA_OK, fpta_insert_row(txn, &table, fptu_take_noshrink(row)));
      n++;

      // проверяем что upsert отказывается обновлять строку,
      // когда есть дубликаты по ключу.
      // сначала удаляем dup_id, чтобы строка не была полным дублем,
      // т.е. чтобы получить отказ именно из-за дубликата ключа,
      // а не из-за защиты от полных дубликатов.
      ASSERT_EQ(1, fptu_erase(row, col_dup_id.column.num, fptu_any));
      ASSERT_EQ(FPTA_KEYEXIST, fpta_upsert_row(txn, &table, fptu_take(row)));
    }
  }

  // завершаем транзакцию
  ASSERT_EQ(FPTA_OK, fpta_transaction_end(txn_guard.release(), false));
  txn = nullptr;

  // разрушаем кортеж
  ASSERT_STREQ(nullptr, fptu_check(row));
  free(row);

  //------------------------------------------------------------------------

  /* Для полноты тесты переоткрываем базу. В этом нет явной необходимости,
   * но только так можно проверить работу некоторых механизмов.
   *
   * В частности:
   *  - внутри движка создание таблицы одновременно приводит к открытию её
   *    dbi-хендла, с размещением его во внутренних структурах.
   *  - причем этот хендл будет жив до закрытии всей базы, либо до удаления
   *    таблицы.
   *  - поэтому для проверки кода открывающего существующую таблицы
   *    необходимо закрыть и повторно открыть всю базу.
   */
  // закрываем базу
  ASSERT_EQ(FPTA_SUCCESS, fpta_db_close(db_quard.release()));
  db = nullptr;
  // открываем заново
  EXPECT_EQ(FPTA_SUCCESS,
            fpta_db_open(testdb_name, fpta_async, 0644, megabytes, false, &db));
  ASSERT_NE(nullptr, db);
  db_quard.reset(db);

  // сбрасываем привязку идентификаторов
  EXPECT_EQ(FPTA_SUCCESS, fpta_name_reset(&table));
  EXPECT_EQ(FPTA_SUCCESS, fpta_name_reset(&col_pk));
  EXPECT_EQ(FPTA_SUCCESS, fpta_name_reset(&col_order));
  EXPECT_EQ(FPTA_SUCCESS, fpta_name_reset(&col_dup_id));
  EXPECT_EQ(FPTA_SUCCESS, fpta_name_reset(&col_t1ha));

  // начинаем транзакцию чтения
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_read, &txn));
  ASSERT_NE(nullptr, txn);
  txn_guard.reset(txn);

  scoped_cursor_guard cursor_guard;
  fpta_cursor *cursor;
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn, &col_pk, fpta_value_begin(), fpta_value_end(),
                             nullptr, fpta_index_is_ordered(index)
                                          ? fpta_ascending_dont_fetch
                                          : fpta_unsorted_dont_fetch,
                             &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);

  // проверяем кол-во записей.
  size_t count;
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(n, count);

  // переходим к первой записи
  if (n) {
    EXPECT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_first));
  } else {
    EXPECT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_first));
  }

  int order = 0;
  for (unsigned i = 0; i < n;) {
    SCOPED_TRACE(std::to_string(i) + " of " + std::to_string(n) + ", order " +
                 std::to_string(order));
    fptu_ro tuple;
    EXPECT_EQ(FPTA_OK, fpta_cursor_get(cursor, &tuple));
    ASSERT_STREQ(nullptr, fptu_check_ro(tuple));

    int error;
    fpta_value key;
    ASSERT_EQ(FPTU_OK, fpta_cursor_key(cursor, &key));
    SCOPED_TRACE("key: " + std::to_string(key.type) + ", length " +
                 std::to_string(key.binary_length));

    auto tuple_order = (int)fptu_get_sint(tuple, col_order.column.num, &error);
    ASSERT_EQ(FPTU_OK, error);
    if (fpta_index_is_ordered(index))
      ASSERT_EQ(order, tuple_order);

    auto tuple_checksum = fptu_get_uint(tuple, col_t1ha.column.num, &error);
    ASSERT_EQ(FPTU_OK, error);
    auto checksum = order_checksum(tuple_order, type, index).uint;
    ASSERT_EQ(checksum, tuple_checksum);

    auto tuple_dup_id = fptu_get_uint(tuple, col_dup_id.column.num, &error);
    ASSERT_EQ(FPTU_OK, error);
    size_t dups = 100500;
    ASSERT_EQ(FPTA_OK, fpta_cursor_dups(cursor_guard.get(), &dups));
    if (fpta_index_is_unique(index)) {
      ASSERT_EQ(42u, tuple_dup_id);
      ASSERT_EQ(1u, dups);
    } else {
      /* Наличие дубликатов означает что для одного значения ключа
       * в базе есть несколько значений. Причем эти значения хранятся
       * в отдельном отсортированном под-дереве.
       *
       * В случае с primary-индексом, значениями являются
       * непосредственно
       * строки (кортежи), а для сравнения значений используется
       * memcmp().
       *
       * Сформированные в этом тесте строки с одинаковым значеним pk,
       * отличаются только полем dup_id, причем используются только два
       * значения 0 и 1.
       * Соответственно, строка с dup_id = 0 будет всегда идти первой.
       */
      ASSERT_EQ(i & 1, tuple_dup_id);
      ASSERT_EQ(2u, dups);
    }

    if (++i < n)
      ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
    else
      EXPECT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_next));

    if (fpta_index_is_unique(index) || (i & 1) == 0)
      ++order;
  }

  EXPECT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));

  // закрываем курсор и завершаем транзакцию
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;
  ASSERT_EQ(FPTA_OK, fpta_transaction_end(txn_guard.release(), true));
  txn = nullptr;

  //------------------------------------------------------------------------

  // разрушаем привязанные идентификаторы
  fpta_name_destroy(&table);
  fpta_name_destroy(&col_pk);
  fpta_name_destroy(&col_order);
  fpta_name_destroy(&col_dup_id);
  fpta_name_destroy(&col_t1ha);

  // закрываем и удаляем базу
  ASSERT_EQ(FPTA_SUCCESS, fpta_db_close(db_quard.release()));
  ASSERT_TRUE(REMOVE_FILE(testdb_name) == 0);
  ASSERT_TRUE(REMOVE_FILE(testdb_name_lck) == 0);
}

//----------------------------------------------------------------------------

template <typename TypeParam> class PrimaryIndex : public ::testing::Test {};
TYPED_TEST_CASE_P(PrimaryIndex);

template <fptu_type _type> struct glue {
  static constexpr fptu_type type = _type;
};

typedef ::testing::Types<glue<fptu_null>, glue<fptu_uint16>, glue<fptu_int32>,
                         glue<fptu_uint32>, glue<fptu_fp32>, glue<fptu_int64>,
                         glue<fptu_uint64>, glue<fptu_fp64>, glue<fptu_96>,
                         glue<fptu_128>, glue<fptu_160>, glue<fptu_datetime>,
                         glue<fptu_256>, glue<fptu_cstr>, glue<fptu_opaque>,
                         /* glue<fptu_nested>, */ glue<fptu_farray>>
    ColumnTypes;

TYPED_TEST_CASE(PrimaryIndex, ColumnTypes);

TYPED_TEST(PrimaryIndex, obverse_unique) {
  TestPrimary<TypeParam::type, fpta_primary_unique_ordered_obverse>();
}

TYPED_TEST(PrimaryIndex, unordered_unique) {
  TestPrimary<TypeParam::type, fpta_primary_unique_unordered>();
}

TYPED_TEST(PrimaryIndex, reverse_unique) {
  TestPrimary<TypeParam::type, fpta_primary_unique_ordered_reverse>();
}

TYPED_TEST(PrimaryIndex, obverse_withdups) {
  TestPrimary<TypeParam::type, fpta_primary_withdups_ordered_obverse>();
}

TYPED_TEST(PrimaryIndex, unordered_withdups) {
  TestPrimary<TypeParam::type, fpta_primary_withdups_unordered>();
}

TYPED_TEST(PrimaryIndex, reverse_withdups) {
  TestPrimary<TypeParam::type, fpta_primary_withdups_ordered_reverse>();
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

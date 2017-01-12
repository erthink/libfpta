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
#include <memory>

#include "keygen.hpp"

/* кол-во проверочных точек в диапазонах значений индексируемых типов */
#ifdef FPTA_INDEX_UT_LONG
static constexpr unsigned NNN = 32749;
#else
static constexpr unsigned NNN = 509; // менее секунды в /dev/shm/
#endif

#define TEST_DB_DIR "/dev/shm/"

struct db_deleter : public std::unary_function<void, fpta_db *> {
  void operator()(fpta_db *db) const {
    if (db)
      EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));
  }
};

struct txn_deleter : public std::unary_function<void, fpta_txn *> {
  void operator()(fpta_txn *txn) const {
    if (txn)
      ASSERT_EQ(FPTA_OK, fpta_transaction_end(txn, true));
  }
};

struct cursor_deleter : public std::unary_function<void, fpta_cursor *> {
  void operator()(fpta_cursor *cursor) const {
    if (cursor)
      ASSERT_EQ(FPTA_OK, fpta_cursor_close(cursor));
  }
};

typedef std::unique_ptr<fpta_db, db_deleter> scoped_db_guard;
typedef std::unique_ptr<fpta_txn, txn_deleter> scoped_txn_guard;
typedef std::unique_ptr<fpta_cursor, cursor_deleter> scoped_cursor_guard;

static const char testdb_name[] = TEST_DB_DIR "ut_index_secondary.fpta";
static const char testdb_name_lck[] =
    TEST_DB_DIR "ut_index_secondary.fpta-lock";

TEST(SecondaryIndex, keygen) {
  scalar_range_stepper<float, 42>::test();
  scalar_range_stepper<float, 43>::test();
  scalar_range_stepper<double, NNN>::test();
  scalar_range_stepper<double, NNN * 2>::test();
  scalar_range_stepper<uint16_t, 43>::test();
  scalar_range_stepper<uint32_t, 43>::test();
  scalar_range_stepper<int32_t, 43>::test();
  scalar_range_stepper<int64_t, 43>::test();

  string_keygen_test<false>(1, 3);
  string_keygen_test<true>(1, 3);
  string_keygen_test<false>(1, fpta_max_keylen);
  string_keygen_test<true>(1, fpta_max_keylen);
  string_keygen_test<false>(8, 8);
  string_keygen_test<true>(8, 8);

  fixbin_stepper<11, 43>::test();
  varbin_stepper<fptu_cstr, 421>::test();
  varbin_stepper<fptu_opaque, 421>::test();
}

TEST(SecondaryIndex, Invalid) {
  // TODO
  //    static const fpta_index_type index_cases[] = {
  //        /* clang-format off */
  //        fpta_primary_unique, fpta_primary_unique_unordered,
  //        fpta_primary_unique_reversed
  //        /* clang-format on */
  //    };
}

//----------------------------------------------------------------------------

/* */
template <fpta_index_type pk_index, fptu_type pk_type,
          fpta_index_type se_index, fptu_type se_type, unsigned N>
struct coupled_keygen {
  const int order_from = 0;
  const int order_to = N - 1;

  fpta_value make_primary(int order) {
    if (fpta_index_is_unique(se_index))
      return keygen<pk_index, pk_type, N>::make(order);

    if (order % 3)
      return keygen<pk_index, pk_type, N * 2>::make(order * 2);
    return keygen<pk_index, pk_type, N * 2>::make(order * 2 + 1);
  }

  fpta_value make_primary_4dup(int order) {
    if (fpta_index_is_unique(se_index))
      fpta_value_null();

    if (order % 3)
      return keygen<pk_index, pk_type, N * 2>::make(order * 2 + 1);
    return keygen<pk_index, pk_type, N * 2>::make(order * 2);
  }

  fpta_value make_secondary(int order) {
    return keygen<se_index, se_type, N>::make(order);
  }
};

//----------------------------------------------------------------------------

template <fptu_type pk_type, fpta_index_type pk_index, fptu_type se_type,
          fpta_index_type se_index>
void TestSecondary() {
  const bool valid_pk = is_valid4primary(pk_type, pk_index);
  const bool valid_se =
      is_valid4secondary(pk_type, pk_index, se_type, se_index);
  scoped_db_guard db_quard;
  scoped_txn_guard txn_guard;

  SCOPED_TRACE(
      "pk_type " + std::to_string(pk_type) + ", pk_index " +
      std::to_string(pk_index) + ", se_type " + std::to_string(se_type) +
      ", se_index " + std::to_string(se_index) +

      (valid_se && valid_pk ? ", (valid case)" : ", (invalid case)"));
  // создаем пять колонок: primary_key, secondary_key, order, t1ha и dup_id
  fpta_column_set def;
  fpta_column_set_init(&def);

  const std::string pk_col_name = "pk_" + std::to_string(pk_type);
  const std::string se_col_name = "se_" + std::to_string(se_type);
  if (!valid_pk) {
    EXPECT_EQ(FPTA_EINVAL, fpta_column_describe(pk_col_name.c_str(), pk_type,
                                                pk_index, &def));
    return;
  }
  EXPECT_EQ(FPTA_OK, fpta_column_describe(pk_col_name.c_str(), pk_type,
                                          pk_index, &def));
  if (!valid_se) {
    EXPECT_EQ(FPTA_EINVAL, fpta_column_describe(se_col_name.c_str(), se_type,
                                                se_index, &def));
    return;
  }
  EXPECT_EQ(FPTA_OK, fpta_column_describe(se_col_name.c_str(), se_type,
                                          se_index, &def));

  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("order", fptu_int32, fpta_index_none, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe("dup_id", fptu_uint16,
                                          fpta_index_none, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("t1ha", fptu_uint64, fpta_index_none, &def));
  ASSERT_EQ(FPTA_OK, fpta_column_set_validate(&def));

  // чистим
  ASSERT_TRUE(unlink(testdb_name) == 0 || errno == ENOENT);
  ASSERT_TRUE(unlink(testdb_name_lck) == 0 || errno == ENOENT);

#ifdef FPTA_INDEX_UT_LONG
  // пытаемся обойтись меньшей базой, но для строк потребуется больше места
  unsigned megabytes = 32;
  if (type > fptu_128)
    megabytes = 40;
  if (type > fptu_256)
    megabytes = 56;
#else
  const unsigned megabytes = 1;
#endif

  fpta_db *db = nullptr;
  EXPECT_EQ(FPTA_SUCCESS, fpta_db_open(testdb_name, fpta_async, 0644,
                                       megabytes, false, &db));
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

  // инициализируем идентификаторы колонок
  fpta_name table, col_pk, col_se, col_order, col_dup_id, col_t1ha;
  EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "table"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_pk, pk_col_name.c_str()));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_se, se_col_name.c_str()));
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
  ASSERT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_se));
  ASSERT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_order));
  ASSERT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_dup_id));
  ASSERT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_t1ha));

  fptu_rw *row = fptu_alloc(6, fpta_max_keylen * 42);
  ASSERT_NE(nullptr, row);
  ASSERT_STREQ(nullptr, fptu_check(row));

  coupled_keygen<pk_index, pk_type, se_index, se_type, NNN> pg;
  unsigned n = 0;
  for (int order = pg.order_from; order <= pg.order_to; ++order) {
    SCOPED_TRACE("order " + std::to_string(order));

    // теперь формируем кортеж
    ASSERT_EQ(FPTU_OK, fptu_clear(row));
    ASSERT_STREQ(nullptr, fptu_check(row));
    fpta_value value_pk = pg.make_primary(order);
    ASSERT_EQ(FPTA_OK,
              fpta_upsert_column(row, &col_order, fpta_value_sint(order)));
    ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_pk, value_pk));
    // тут важно помнить, что генераторы ключей для не-числовых типов
    // используют статический буфер, поэтому генерация значения для
    // secondary может повредить значение primary.
    // поэтому primary поле нужно добавить в кортеж до генерации
    // secondary.
    fpta_value value_se = pg.make_secondary(order);
    ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_se, value_se));
    // t1ha как "checksum" для order
    ASSERT_EQ(FPTA_OK,
              fpta_upsert_column(row, &col_t1ha,
                                 order_checksum(order, se_type, se_index)));

    // пытаемся обновить несуществующую запись
    ASSERT_EQ(MDB_NOTFOUND,
              fpta_update_row(txn, &table, fptu_take_noshrink(row)));

    if (fpta_index_is_unique(se_index)) {
      // вставляем
      ASSERT_EQ(FPTA_OK,
                fpta_insert_row(txn, &table, fptu_take_noshrink(row)));
      n++;
      // проверяем что полный дубликат не вставляется
      ASSERT_EQ(MDB_KEYEXIST,
                fpta_insert_row(txn, &table, fptu_take_noshrink(row)));

      // обновляем dup_id и проверям что дубликат по ключу не проходит
      ASSERT_EQ(FPTA_OK,
                fpta_upsert_column(row, &col_dup_id, fpta_value_uint(1)));
      ASSERT_EQ(MDB_KEYEXIST,
                fpta_insert_row(txn, &table, fptu_take_noshrink(row)));

      // проверяем что upsert и update работают,
      // сначала upsert с текущим dup_id = 1
      ASSERT_EQ(FPTA_OK,
                fpta_upsert_row(txn, &table, fptu_take_noshrink(row)));
      // теперь update c dup_id = 42, должен остаться только этот
      // вариант
      ASSERT_EQ(FPTA_OK,
                fpta_upsert_column(row, &col_dup_id, fpta_value_uint(42)));
      ASSERT_EQ(FPTA_OK,
                fpta_update_row(txn, &table, fptu_take_noshrink(row)));
    } else {
      // вставляем c dup_id = 0
      ASSERT_EQ(FPTA_OK,
                fpta_upsert_column(row, &col_dup_id, fpta_value_uint(0)));
      ASSERT_EQ(FPTA_OK,
                fpta_insert_row(txn, &table, fptu_take_noshrink(row)));
      n++;
      // проверяем что полный дубликат не вставляется
      ASSERT_EQ(MDB_KEYEXIST,
                fpta_insert_row(txn, &table, fptu_take_noshrink(row)));

      // обновляем dup_id и вставляем дубль по ключу
      // без обновления primary, такой дубликат также
      // НЕ должен вставится
      ASSERT_EQ(FPTA_OK,
                fpta_upsert_column(row, &col_dup_id, fpta_value_uint(1)));
      ASSERT_EQ(MDB_KEYEXIST, fpta_insert_row(txn, &table, fptu_take(row)));

      // теперь обновляем primary key и вставляем дубль по вторичному
      // ключу
      value_pk = pg.make_primary_4dup(order);
      ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_pk, value_pk));
      ASSERT_EQ(FPTA_OK, fpta_insert_row(txn, &table, fptu_take(row)));
      n++;
    }
  }

  // завершаем транзакцию
  ASSERT_EQ(FPTA_OK, fpta_transaction_end(txn_guard.release(), false));
  txn = nullptr;

  // разрушаем кортеж
  ASSERT_STREQ(nullptr, fptu_check(row));
  free(row);

  //------------------------------------------------------------------------

  // начинаем транзакцию чтения
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_read, &txn));
  ASSERT_NE(nullptr, txn);
  txn_guard.reset(txn);

  scoped_cursor_guard cursor_guard;
  fpta_cursor *cursor;
  EXPECT_EQ(FPTA_OK, fpta_cursor_open(txn, &col_se, fpta_value_begin(),
                                      fpta_value_end(), nullptr,
                                      fpta_index_is_ordered(se_index)
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

  int order = pg.order_from;
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

    auto tuple_order = fptu_get_sint(tuple, col_order.column.num, &error);
    ASSERT_EQ(FPTU_OK, error);
    if (fpta_index_is_ordered(se_index))
      ASSERT_EQ(order, tuple_order);

    auto tuple_checksum = fptu_get_uint(tuple, col_t1ha.column.num, &error);
    ASSERT_EQ(FPTU_OK, error);
    auto checksum = order_checksum(tuple_order, se_type, se_index).uint;
    ASSERT_EQ(checksum, tuple_checksum);

    auto tuple_dup_id = fptu_get_uint(tuple, col_dup_id.column.num, &error);
    ASSERT_EQ(FPTU_OK, error);
    if (fpta_index_is_unique(se_index))
      ASSERT_EQ(42, tuple_dup_id);
    else {
      /* Наличие дубликатов означает что для одного значения ключа
       * в базе есть несколько значений. Причем эти значения хранятся
       * в отдельном отсортированном под-дереве.
       *
       * В случае с secondary-индексом, значениями является primary key,
       * а для сравнения используется соответствующий компаратор.
       *
       * Сформированные в этом тесте строки-дубликаты по вторичному
       * ключу всегда отличаются двумя полями: pk и dup_id.
       * Однако, порядок следования строк определяется значением pk,
       * который генерируется с чередованием больше/меньше,
       * с тем чтобы можно было проверить корректность реализации.
       * Соответственно, этот порядок должен соблюдаться, если только
       * первичный индекс не unordered.
       */
      if (!fpta_index_is_ordered(pk_index))
        ASSERT_GT(2, tuple_dup_id);
      else if (tuple_order % 3)
        ASSERT_EQ(i & 1, tuple_dup_id);
      else
        ASSERT_EQ((i ^ 1) & 1, tuple_dup_id);
    }

    if (++i < n)
      ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
    else
      EXPECT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_next));

    if (fpta_index_is_unique(se_index) || (i & 1) == 0)
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
  fpta_name_destroy(&col_se);
  fpta_name_destroy(&col_order);
  fpta_name_destroy(&col_dup_id);
  fpta_name_destroy(&col_t1ha);

  // закрываем и удаляем базу
  ASSERT_EQ(FPTA_SUCCESS, fpta_db_close(db_quard.release()));
  ASSERT_TRUE(unlink(testdb_name) == 0);
  ASSERT_TRUE(unlink(testdb_name_lck) == 0);
}

template <typename TypeParam>
class SecondaryIndex : public ::testing::Test {};
TYPED_TEST_CASE_P(SecondaryIndex);

template <fptu_type _type> struct glue {
  static constexpr fptu_type type = _type;
};

typedef ::testing::Types<glue<fptu_null>, glue<fptu_uint16>, glue<fptu_int32>,
                         glue<fptu_uint32>, glue<fptu_fp32>, glue<fptu_int64>,
                         glue<fptu_uint64>, glue<fptu_fp64>, glue<fptu_96>,
                         glue<fptu_128>, glue<fptu_160>, glue<fptu_192>,
                         glue<fptu_256>, glue<fptu_cstr>, glue<fptu_opaque>,
                         /* glue<fptu_nested>, */ glue<fptu_farray>>
    ColumnTypes;

TYPED_TEST_CASE(SecondaryIndex, ColumnTypes);

#define SI_TEST_CASE(se_index, pk_type, pk_index)                            \
  TYPED_TEST(SecondaryIndex, se_index##__PK_##pk_type##_##pk_index) {        \
    TestSecondary<fptu_##pk_type, fpta_primary_##pk_index, TypeParam::type,  \
                  fpta_secondary_##se_index>();                              \
  }

#define SI_TEST_CASES__ITERATE_SI(pk_type, pk_index)                         \
  SI_TEST_CASE(unique_obverse, pk_type, pk_index)                            \
  SI_TEST_CASE(unique_unordered, pk_type, pk_index)                          \
  SI_TEST_CASE(unique_reversed, pk_type, pk_index)                           \
  SI_TEST_CASE(withdups_obverse, pk_type, pk_index)                          \
  SI_TEST_CASE(withdups_unordered, pk_type, pk_index)                        \
  SI_TEST_CASE(withdups_reversed, pk_type, pk_index)

#define SI_TEST_CASES__ITERATE_PI(pk_type)                                   \
  SI_TEST_CASES__ITERATE_SI(pk_type, unique_obverse)                         \
  SI_TEST_CASES__ITERATE_SI(pk_type, unique_unordered)                       \
  SI_TEST_CASES__ITERATE_SI(pk_type, unique_reversed)                        \
  SI_TEST_CASES__ITERATE_SI(pk_type, withdups_obverse)                       \
  SI_TEST_CASES__ITERATE_SI(pk_type, withdups_unordered)                     \
  SI_TEST_CASES__ITERATE_SI(pk_type, withdups_reversed)

// SI_TEST_CASES__ITERATE_PI(null)
// SI_TEST_CASES__ITERATE_PI(uint16)
// SI_TEST_CASES__ITERATE_PI(int32)
// SI_TEST_CASES__ITERATE_PI(uint32)
// SI_TEST_CASES__ITERATE_PI(int64)
// SI_TEST_CASES__ITERATE_PI(uint64)
// SI_TEST_CASES__ITERATE_PI(fp32)
// SI_TEST_CASES__ITERATE_PI(fp64)
// SI_TEST_CASES__ITERATE_PI(96)
// SI_TEST_CASES__ITERATE_PI(128)
// SI_TEST_CASES__ITERATE_PI(160)
// SI_TEST_CASES__ITERATE_PI(192)
// SI_TEST_CASES__ITERATE_PI(256)
SI_TEST_CASES__ITERATE_PI(cstr)
// SI_TEST_CASES__ITERATE_PI(opaque)
// // SI_TEST_CASES__ITERATE_PI(nested)
// SI_TEST_CASES__ITERATE_PI(farray)

//----------------------------------------------------------------------------

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

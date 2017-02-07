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

#include "fast_positive/tables_internal.h"
#include <gtest/gtest.h>
#include <tuple>
#include <unordered_map>

#include "keygen.hpp"
#include "tools.hpp"

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
#if FPTA_CURSOR_UT_LONG
static constexpr unsigned NNN = 65521; // около 1-2 минуты в /dev/shm/
#else
static constexpr unsigned NNN = 509; // менее секунды в /dev/shm/
#endif

#define TEST_DB_DIR "/dev/shm/"

static const char testdb_name[] = TEST_DB_DIR "ut_cursor_primary.fpta";
static const char testdb_name_lck[] =
    TEST_DB_DIR "ut_cursor_primary.fpta-lock";

class CursorPrimary
    : public ::testing::TestWithParam<
#if GTEST_USE_OWN_TR1_TUPLE || GTEST_HAS_TR1_TUPLE
          std::tr1::tuple<fptu_type, fpta_index_type, fpta_cursor_options>>
#else
          std::tuple<fptu_type, fpta_index_type, fpta_cursor_options>>
#endif
{
public:
  fptu_type type;
  fpta_index_type index;
  fpta_cursor_options ordering;
  bool valid_index_ops;
  bool valid_cursor_ops;

  scoped_db_guard db_quard;
  scoped_txn_guard txn_guard;
  scoped_cursor_guard cursor_guard;

  std::string pk_col_name;
  fpta_name table, col_pk, col_order, col_dup_id, col_t1ha;
  static constexpr int n_dups = 5;
  unsigned n;
  std::unordered_map<int, int> reorder;

  void CheckPosition(int linear, int dup) {
    if (linear < 0)
      linear += reorder.size();
    if (dup < 0)
      dup += n_dups;

    SCOPED_TRACE("linear-order " + std::to_string(linear) + " [0..." +
                 std::to_string(reorder.size() - 1) + "], dup " +
                 std::to_string(dup));

    ASSERT_EQ(1, reorder.count(linear));
    const auto expected_order = reorder[linear];

    SCOPED_TRACE("logical-order " + std::to_string(expected_order) + " [" +
                 std::to_string(0) + "..." + std::to_string(NNN) + ")");

    ASSERT_EQ(0, fpta_cursor_eof(cursor_guard.get()));

    fptu_ro tuple;
    EXPECT_EQ(FPTA_OK, fpta_cursor_get(cursor_guard.get(), &tuple));
    ASSERT_STREQ(nullptr, fptu_check_ro(tuple));

    int error;
    fpta_value key;
    ASSERT_EQ(FPTU_OK, fpta_cursor_key(cursor_guard.get(), &key));
    SCOPED_TRACE("key: " + std::to_string(key.type) + ", length " +
                 std::to_string(key.binary_length));

    auto tuple_order = fptu_get_sint(tuple, col_order.column.num, &error);
    ASSERT_EQ(FPTU_OK, error);
    ASSERT_EQ(expected_order, tuple_order);

    auto tuple_checksum = fptu_get_uint(tuple, col_t1ha.column.num, &error);
    ASSERT_EQ(FPTU_OK, error);
    auto checksum = order_checksum(tuple_order, type, index).uint;
    ASSERT_EQ(checksum, tuple_checksum);

    auto tuple_dup_id = fptu_get_uint(tuple, col_dup_id.column.num, &error);
    ASSERT_EQ(FPTU_OK, error);
    size_t dups = 100500;
    ASSERT_EQ(FPTA_OK, fpta_cursor_dups(cursor_guard.get(), &dups));
    if (fpta_index_is_unique(index)) {
      ASSERT_EQ(42, tuple_dup_id);
      ASSERT_EQ(1, dups);
    } else {
      /* Следует пояснить (в том числе напомнить себе), почему порядок
       * следования строк-дубликатов (с одинаковым значением PK) здесь
       * совпадает с dup_id:
       *  - При формировании строк-дубликатов, их кортежи полностью совпадают,
       *    за исключением поля dup_id. При этом dup_id отличается только
       *    одним байтом.
       *  - В случае primary-индекса данные соответствующие одному значению
       *    ключа сортируются при помощи компаратора fptu_cmp_tuples().
       *    Однако, даже при сравнении посредством memcmp(), различие в
       *    только в одном байте гарантирует нужный порядок. */
      int expected_dup_id =
          (ordering & fpta_descending) ? n_dups - (dup + 1) : dup;
      ASSERT_EQ(expected_dup_id, tuple_dup_id);
      ASSERT_EQ(n_dups, dups);
    }
  }

  virtual void Fill() {
    fptu_rw *row = fptu_alloc(4, fpta_max_keylen * 2 + 4 + 4);
    ASSERT_NE(nullptr, row);
    ASSERT_STREQ(nullptr, fptu_check(row));
    fpta_txn *const txn = txn_guard.get();

    any_keygen keygen(type, index);
    n = 0;
    for (unsigned order = 0; order < NNN; ++order) {
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
      ASSERT_EQ(FPTA_OK,
                fpta_upsert_column(row, &col_t1ha,
                                   order_checksum(order, type, index)));

      if (fpta_index_is_unique(index)) {
        // вставляем одну запись с dup_id = 42
        ASSERT_EQ(FPTA_OK,
                  fpta_upsert_column(row, &col_dup_id, fpta_value_uint(42)));
        ASSERT_EQ(FPTA_OK,
                  fpta_insert_row(txn, &table, fptu_take_noshrink(row)));
        ++n;
      } else {
        for (unsigned dup_id = 0; dup_id < n_dups; ++dup_id) {
          ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_dup_id,
                                                fpta_value_uint(dup_id)));
          ASSERT_EQ(FPTA_OK,
                    fpta_insert_row(txn, &table, fptu_take_noshrink(row)));
          ++n;
        }
      }
    }

    // разрушаем кортеж
    ASSERT_STREQ(nullptr, fptu_check(row));
    free(row);
  }

  virtual void SetUp() {
    // нужно простое число, иначе сломается переупорядочивание
    ASSERT_TRUE(isPrime(NNN));
    // иначе не сможем проверить fptu_uint16
    ASSERT_GE(UINT16_MAX, NNN);
#if GTEST_USE_OWN_TR1_TUPLE || GTEST_HAS_TR1_TUPLE
    type = std::tr1::get<0>(GetParam());
    index = std::tr1::get<1>(GetParam());
    ordering = std::tr1::get<2>(GetParam());
#else
    type = std::get<0>(GetParam());
    index = std::get<1>(GetParam());
    ordering = std::get<2>(GetParam());
#endif
    valid_index_ops = is_valid4primary(type, index);
    valid_cursor_ops = is_valid4cursor(index, ordering);

    SCOPED_TRACE("type " + std::to_string(type) + ", index " +
                 std::to_string(index) +
                 (valid_index_ops ? ", (valid index case)"
                                  : ", (invalid index case)"));

    SCOPED_TRACE("ordering " + std::to_string(ordering) + ", index " +
                 std::to_string(index) +
                 (valid_cursor_ops ? ", (valid cursor case)"
                                   : ", (invalid cursor case)"));

    // инициализируем идентификаторы колонок
    ASSERT_EQ(FPTA_OK, fpta_table_init(&table, "table"));
    pk_col_name = "pk_" + std::to_string(type);
    ASSERT_EQ(FPTA_OK,
              fpta_column_init(&table, &col_pk, pk_col_name.c_str()));
    ASSERT_EQ(FPTA_OK, fpta_column_init(&table, &col_order, "order"));
    ASSERT_EQ(FPTA_OK, fpta_column_init(&table, &col_dup_id, "dup_id"));
    ASSERT_EQ(FPTA_OK, fpta_column_init(&table, &col_t1ha, "t1ha"));

    // создаем четыре колонки: pk, order, t1ha и dup_id
    fpta_column_set def;
    fpta_column_set_init(&def);

    if (!valid_index_ops) {
      EXPECT_EQ(FPTA_EINVAL,
                fpta_column_describe(pk_col_name.c_str(), type, index, &def));
      EXPECT_EQ(FPTA_OK, fpta_column_describe("order", fptu_int32,
                                              fpta_index_none, &def));
      ASSERT_NE(FPTA_OK, fpta_column_set_validate(&def));
      return;
    }

    EXPECT_EQ(FPTA_OK,
              fpta_column_describe(pk_col_name.c_str(), type, index, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe("order", fptu_int32,
                                            fpta_index_none, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe("dup_id", fptu_uint16,
                                            fpta_index_none, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe("t1ha", fptu_uint64,
                                            fpta_index_none, &def));
    ASSERT_EQ(FPTA_OK, fpta_column_set_validate(&def));

    // чистим
    ASSERT_TRUE(unlink(testdb_name) == 0 || errno == ENOENT);
    ASSERT_TRUE(unlink(testdb_name_lck) == 0 || errno == ENOENT);

#ifdef FPTA_CURSOR_UT_LONG
    // пытаемся обойтись меньшей базой,
    // но для строк потребуется больше места
    unsigned megabytes = 32;
    if (type > fptu_96)
      megabytes = 56;
    if (type > fptu_256)
      megabytes = 64;
#else
    const unsigned megabytes = 1;
#endif

    fpta_db *db = nullptr;
    EXPECT_EQ(FPTA_SUCCESS, fpta_db_open(testdb_name, fpta_async, 0644,
                                         megabytes, true, &db));
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
    EXPECT_EQ(FPTA_SUCCESS, fpta_db_open(testdb_name, fpta_async, 0644,
                                         megabytes, false, &db));
    ASSERT_NE(nullptr, db);
    db_quard.reset(db);

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

    ASSERT_NO_FATAL_FAILURE(Fill());

    // завершаем транзакцию
    ASSERT_EQ(FPTA_OK, fpta_transaction_end(txn_guard.release(), false));
    txn = nullptr;

    //------------------------------------------------------------------------

    // начинаем транзакцию чтения
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_read, &txn));
    ASSERT_NE(nullptr, txn);
    txn_guard.reset(txn);

    fpta_cursor *cursor;
    if (valid_cursor_ops) {
      EXPECT_EQ(FPTA_OK, fpta_cursor_open(txn, &col_pk, fpta_value_begin(),
                                          fpta_value_end(), nullptr, ordering,
                                          &cursor));
      ASSERT_NE(nullptr, cursor);
      cursor_guard.reset(cursor);
    } else {
      EXPECT_EQ(FPTA_NO_INDEX,
                fpta_cursor_open(txn, &col_pk, fpta_value_begin(),
                                 fpta_value_end(), nullptr, ordering,
                                 &cursor));
      cursor_guard.reset(cursor);
      ASSERT_EQ(nullptr, cursor);
      return;
    }

    // формируем линейную карту, чтобы проще проверять переходы
    reorder.clear();
    reorder.reserve(NNN);
    for (int linear = 0; fpta_cursor_eof(cursor) == FPTA_OK; ++linear) {
      fptu_ro tuple;
      EXPECT_EQ(FPTA_OK, fpta_cursor_get(cursor_guard.get(), &tuple));
      ASSERT_STREQ(nullptr, fptu_check_ro(tuple));

      int error;
      auto tuple_order = fptu_get_sint(tuple, col_order.column.num, &error);
      ASSERT_EQ(FPTU_OK, error);
      auto tuple_checksum = fptu_get_uint(tuple, col_t1ha.column.num, &error);
      ASSERT_EQ(FPTU_OK, error);

      auto checksum = order_checksum(tuple_order, type, index).uint;
      ASSERT_EQ(checksum, tuple_checksum);

      reorder[linear] = tuple_order;

      error = fpta_cursor_move(cursor, fpta_key_next);
      if (error == FPTA_NODATA)
        break;
      ASSERT_EQ(FPTA_SUCCESS, error);
    }

    ASSERT_EQ(NNN, reorder.size());

    if (fpta_cursor_is_ordered(ordering)) {
      std::map<int, int> probe;
      for (auto pair : reorder)
        probe[pair.first] = pair.second;
      ASSERT_EQ(probe.size(), reorder.size());
      ASSERT_TRUE(
          is_properly_ordered(probe, fpta_cursor_is_descending(ordering)));
    }
  }

  virtual void TearDown() {
    // разрушаем привязанные идентификаторы
    fpta_name_destroy(&table);
    fpta_name_destroy(&col_pk);
    fpta_name_destroy(&col_order);
    fpta_name_destroy(&col_dup_id);
    fpta_name_destroy(&col_t1ha);

    // закрываем курсор и завершаем транзакцию
    if (cursor_guard)
      EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
    if (txn_guard)
      ASSERT_EQ(FPTA_OK, fpta_transaction_end(txn_guard.release(), true));
    if (db_quard) {
      // закрываем и удаляем базу
      ASSERT_EQ(FPTA_SUCCESS, fpta_db_close(db_quard.release()));
      ASSERT_TRUE(unlink(testdb_name) == 0);
      ASSERT_TRUE(unlink(testdb_name_lck) == 0);
    }
  }
};

constexpr int CursorPrimary::n_dups;

TEST_P(CursorPrimary, basicMoves) {
  /* Проверка базовых перемещений курсора по первичному (primary) индексу.
   *
   * Сценарий (общий для всех типов полей и всех видов первичных индексов):
   *  1. Создается тестовая база с одной таблицей, в которой четыре колонки:
   *      - "col_pk" (primary key) с типом, для которого производится
   *        тестирование работы индекса.
   *      - Колонка "order", в которую записывается контрольный (ожидаемый)
   *        порядковый номер следования строки, при сортировке по col_pk и
   *        проверяемому виду индекса.
   *      - Колонка "dup_id", которая используется для идентификации
   *        дубликатов индексов допускающих не-уникальные ключи.
   *      - Колонка "t1ha", в которую записывается "контрольная сумма" от
   *        ожидаемого порядка строки, типа PK и вида индекса.
   *        Принципиальной необходимости в этой колонке нет, она используется
   *        как "утяжелитель", а также для дополнительного контроля.
   *
   *  2. Для валидных комбинаций вида индекса и типа данных col_pk таблица
   *     заполняется строками, значение col_pk в которых генерируется
   *     соответствующим генератором значений:
   *      - Сами генераторы проверяются в одном из тестов 0corny.
   *      - Для индексов без уникальности для каждого ключа вставляется
   *        5 строк с разным dup_id.
   *
   *  3. Перебираются все комбинации индексов, типов колонок и видов курсора.
   *     Для НЕ валидных комбинаций контролируются коды ошибок.
   *
   *  4. Для валидных комбинаций индекса и типа курсора, после заполнения
   *     в отдельной транзакции формируется "карта" верификации перемещений:
   *      - "карта" строится как неупорядоченное отображение линейных номеров
   *        строк в порядке просмотра через курсор, на ожидаемые (контрольные)
   *        значения колонки "order".
   *      - при построении "карты" все строки читаются последовательно через
   *        проверяемый курсор.
   *      - для построенной "карты" проверяется размер (что прочитали все
   *        строки и только по одному разу) и соответствия порядка строк
   *        типу курсора (возрастание/убывание).
   *
   *  5. После формирования "карты" верификации перемещений выполняется ряд
   *     базовых перемещений курсора:
   *      - переход к первой/последней строке.
   *      - попытки перейти за последнюю и за первую строки.
   *      - переход в начало с отступлением к концу.
   *      - переход к концу с отступление к началу.
   *      - при каждом перемещении проверяется корректность кода ошибки,
   *        соответствие текущей строки ожидаемому порядку, включая
   *        содержимое строки и номера дубликата.
   *
   *  6. Завершаются операции и освобождаются ресурсы.
   */
  if (!valid_index_ops || !valid_cursor_ops)
    return;

  SCOPED_TRACE("type " + std::to_string(type) + ", index " +
               std::to_string(index) +
               (valid_index_ops ? ", (valid case)" : ", (invalid case)"));

  SCOPED_TRACE("ordering " + std::to_string(ordering) + ", index " +
               std::to_string(index) +
               (valid_cursor_ops ? ", (valid cursor case)"
                                 : ", (invalid cursor case)"));

  ASSERT_GT(n, 5);
  fpta_cursor *const cursor = cursor_guard.get();
  ASSERT_NE(nullptr, cursor);

  // переходим туда-сюда и к первой строке
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_first));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 0));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_last));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, -1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_first));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 0));

  // пробуем уйти дальше последней строки
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_last));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, -1));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_next));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
#if FPTA_ENABLE_RETURN_INTO_RANGE
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_key_next));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_dup_next));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_dup_last));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_dup_first));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
#else
  ASSERT_EQ(FPTA_ECURSOR, fpta_cursor_move(cursor, fpta_key_next));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
  ASSERT_EQ(FPTA_ECURSOR, fpta_cursor_move(cursor, fpta_dup_next));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
  ASSERT_EQ(FPTA_ECURSOR, fpta_cursor_move(cursor, fpta_dup_last));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
  ASSERT_EQ(FPTA_ECURSOR, fpta_cursor_move(cursor, fpta_dup_first));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
#endif

  // пробуем выйти за первую строку
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_first));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 0));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
#if FPTA_ENABLE_RETURN_INTO_RANGE
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_key_prev));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_dup_prev));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_dup_last));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_dup_first));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
#else
  ASSERT_EQ(FPTA_ECURSOR, fpta_cursor_move(cursor, fpta_key_prev));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
  ASSERT_EQ(FPTA_ECURSOR, fpta_cursor_move(cursor, fpta_dup_prev));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
  ASSERT_EQ(FPTA_ECURSOR, fpta_cursor_move(cursor, fpta_dup_last));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
  ASSERT_EQ(FPTA_ECURSOR, fpta_cursor_move(cursor, fpta_dup_first));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
#endif

  // идем в конец и проверяем назад/вперед
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_last));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, -1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_key_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-2, -1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_key_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 0));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_key_next));
#if FPTA_ENABLE_RETURN_INTO_RANGE
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
#else
  ASSERT_EQ(FPTA_ECURSOR, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_last));
#endif
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, -1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_key_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-2, -1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_key_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-3, -1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_key_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-2, 0));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_key_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 0));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_key_next));

  // идем в начало и проверяем назад/вперед
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_first));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 0));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_key_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(1, 0));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_key_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, -1));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_key_prev));
#if FPTA_ENABLE_RETURN_INTO_RANGE
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
#else
  ASSERT_EQ(FPTA_ECURSOR, fpta_cursor_move(cursor, fpta_next));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_first));
#endif
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 0));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_key_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(1, 0));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_key_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(2, 0));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_key_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(1, -1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_key_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, -1));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_key_prev));
}

//----------------------------------------------------------------------------

class CursorPrimaryDups : public CursorPrimary {};

TEST_P(CursorPrimaryDups, dupMoves) {
  /* Проверка перемещений курсора по дубликатами в первичном (primary)
   * индексе.
   *
   * Сценарий (общий для всех типов полей и всех видов первичных индексов):
   *  1. Создается тестовая база с одной таблицей, в которой четыре колонки:
   *      - "col_pk" (primary key) с типом, для которого производится
   *        тестирование работы индекса.
   *      - Колонка "order", в которую записывается контрольный (ожидаемый)
   *        порядковый номер следования строки, при сортировке по col_pk и
   *        проверяемому виду индекса.
   *      - Колонка "dup_id", которая используется для нумерации дубликатов.
   *      - Колонка "t1ha", в которую записывается "контрольная сумма" от
   *        ожидаемого порядка строки, типа PK и вида индекса. Принципиальной
   *        необходимости в этой колонке нет, она используется как
   *        "утяжелитель", а также для дополнительного контроля.
   *
   *  2. Для валидных комбинаций вида индекса и типа данных col_pk таблица
   *     заполняется строками, значение PK в которых генерируется
   *     соответствующим генератором значений:
   *      - Сами генераторы проверяются в одном из тестов 0corny.
   *      - Для каждого значения ключа вставляется 5 строк с разным dup_id.
   *      - FIXME: Дополнительно, для тестирования межстраничных переходов,
   *        генерируется длинная серия повторов, которая более чем в три раза
   *        превышает размер страницы БД.
   *
   *  3. Перебираются все комбинации индексов, типов колонок и видов курсора.
   *     Для НЕ валидных комбинаций контролируются коды ошибок.
   *
   *  4. Для валидных комбинаций индекса и типа курсора, после заполнения
   *     в отдельной транзакции формируется "карта" верификации перемещений:
   *      - "карта" строится как неупорядоченное отображение линейных номеров
   *        строк в порядке просмотра через курсор, на ожидаемые (контрольные)
   *        значения колонки "order".
   *      - при построении "карты" все строки читаются последовательно через
   *        проверяемый курсор.
   *      - для построенной "карты" проверяется размер (что прочитали все
   *        строки и только по одному разу) и соответствия порядка строк
   *        типу курсора (возрастание/убывание).
   *
   *  5. После формирования "карты" верификации перемещений выполняется ряд
   *     базовых перемещений курсора по дубликатам:
   *      - переход к первой/последней строке,
   *        к первому и последнему дубликату.
   *      - попытки перейти за последнюю и за первую строки,
   *        за первый/последний дубликат.
   *      - переход в начало с отступлением к концу.
   *      - переход к концу с отступление к началу.
   *      - при каждом перемещении проверяется корректность кода ошибки,
   *        соответствие текущей строки ожидаемому порядку, включая
   *        содержимое строки и номера дубликата.
   *
   *  6. Завершаются операции и освобождаются ресурсы.
   */
  if (!valid_index_ops || !valid_cursor_ops)
    return;

  SCOPED_TRACE("type " + std::to_string(type) + ", index " +
               std::to_string(index) +
               (valid_index_ops ? ", (valid case)" : ", (invalid case)"));

  SCOPED_TRACE("ordering " + std::to_string(ordering) + ", index " +
               std::to_string(index) +
               (valid_cursor_ops ? ", (valid cursor case)"
                                 : ", (invalid cursor case)"));

  ASSERT_GT(n, 5);
  fpta_cursor *const cursor = cursor_guard.get();
  ASSERT_NE(nullptr, cursor);

  /* переходим туда-сюда и к первой строке, такие переходы уже проверялись
   * в предыдущем тесте, здесь же для проверки жизнеспособности курсора. */
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_first));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 0));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_last));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, -1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_first));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 0));

  // к последнему, затем к первому дубликату первой строки
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_dup_last));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, -1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_dup_first));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 0));

  // вперед по дубликатам первой строки
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_dup_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_dup_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 2));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_dup_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 3));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_dup_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 4));
  // пробуем выйти за последний дубликат
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_dup_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 4));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_dup_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 4));

  // назад по дубликатам первой строки
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_dup_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 3));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_dup_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 2));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_dup_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_dup_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 0));
  // пробуем выйти за первый дубликат
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_dup_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 0));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_dup_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 0));

  // вперед в обход дубликатов, ко второй строке, затем к третьей и четвертой
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_key_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(1, 0));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_key_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(2, 0));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_key_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(3, 0));

  // назад в обход дубликатов, до первой строки
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_key_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(2, -1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_key_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(1, -1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_key_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, -1));
  // пробуем выйти за первую строку
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_key_prev));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
#if FPTA_ENABLE_RETURN_INTO_RANGE
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_key_prev));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
#else
  ASSERT_EQ(FPTA_ECURSOR, fpta_cursor_move(cursor, fpta_key_prev));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
#endif

// последовательно вперед от начала по каждому дубликату
#if FPTA_ENABLE_RETURN_INTO_RANGE
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
#else
  ASSERT_EQ(FPTA_ECURSOR, fpta_cursor_move(cursor, fpta_next));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
  ASSERT_EQ(FPTA_ECURSOR, fpta_cursor_move(cursor, fpta_first));
#endif
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 0));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 2));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 3));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 4));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(1, 0));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(1, 1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(1, 2));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(1, 3));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(1, 4));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(2, 0));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(2, 1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(2, 2));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(2, 3));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(2, 4));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(3, 0));

  // последовательно назад к началу по каждому дубликату
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(2, 4));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(2, 3));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(2, 2));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(2, 1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(2, 0));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(1, 4));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(1, 3));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(1, 2));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(1, 1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(1, 0));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 4));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 3));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 2));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 0));
  // пробуем выйти за первую строку
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
#if FPTA_ENABLE_RETURN_INTO_RANGE
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
#else
  ASSERT_EQ(FPTA_ECURSOR, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
#endif

  //--------------------------------------------------------------------------

  // к последней строке
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_last));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, -1));
  // к первому, затем к последнему дубликату последней строки
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_dup_first));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 0));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_dup_last));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, -1));

  // назад по дубликатам последней строки
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_dup_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 3));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_dup_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 2));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_dup_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_dup_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 0));
  // пробуем выйти за первый дубликат
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_dup_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 0));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_dup_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 0));

  // вперед по дубликатам первой строки
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_dup_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_dup_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 2));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_dup_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 3));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_dup_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 4));
  // пробуем выйти за последний дубликат
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_dup_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, -1));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_dup_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, -1));

  // назад в обход дубликатов, к предпоследней строке,
  // затем к пред-предпоследней...
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_key_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-2, -1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_key_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-3, -1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_key_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-4, -1));

  // вперед в обход дубликатов, до последней строки
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_key_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-3, 0));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_key_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-2, 0));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_key_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 0));
  // пробуем выйти за первую строку
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_key_next));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
#if FPTA_ENABLE_RETURN_INTO_RANGE
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_key_next));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
#else
  ASSERT_EQ(FPTA_ECURSOR, fpta_cursor_move(cursor, fpta_key_next));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
#endif

// последовательно назад от конца по каждому дубликату
#if FPTA_ENABLE_RETURN_INTO_RANGE
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
#else
  ASSERT_EQ(FPTA_ECURSOR, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
  ASSERT_EQ(FPTA_ECURSOR, fpta_cursor_move(cursor, fpta_last));
#endif
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 4));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 3));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 2));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 0));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-2, 4));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-2, 3));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-2, 2));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-2, 1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-2, 0));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-3, 4));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-3, 3));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-3, 2));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-3, 1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-3, 0));

  // последовательно вперед до конца по каждому дубликату
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-3, 1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-3, 2));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-3, 3));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-3, 4));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-2, 0));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-2, 1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-2, 2));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-2, 3));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-2, 4));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 0));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 2));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 3));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 4));
  // пробуем выйти за последнюю строку
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_next));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
#if FPTA_ENABLE_RETURN_INTO_RANGE
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_next));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
#else
  ASSERT_EQ(FPTA_ECURSOR, fpta_cursor_move(cursor, fpta_next));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_last));
#endif
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 4));
}

#if GTEST_HAS_COMBINE

INSTANTIATE_TEST_CASE_P(
    Combine, CursorPrimary,
    ::testing::Combine(
        ::testing::Values(fptu_null, fptu_uint16, fptu_int32, fptu_uint32,
                          fptu_fp32, fptu_int64, fptu_uint64, fptu_fp64,
                          fptu_96, fptu_128, fptu_160, fptu_datetime,
                          fptu_256, fptu_cstr, fptu_opaque
                          /*, fptu_nested, fptu_farray */),
        ::testing::Values(fpta_primary_unique, fpta_primary_unique_reversed,
                          fpta_primary_withdups,
                          fpta_primary_withdups_reversed,
                          fpta_primary_unique_unordered,
                          fpta_primary_withdups_unordered),
        ::testing::Values(fpta_unsorted, fpta_ascending, fpta_descending)));

INSTANTIATE_TEST_CASE_P(
    Combine, CursorPrimaryDups,
    ::testing::Combine(
        ::testing::Values(fptu_null, fptu_uint16, fptu_int32, fptu_uint32,
                          fptu_fp32, fptu_int64, fptu_uint64, fptu_fp64,
                          fptu_96, fptu_128, fptu_160, fptu_datetime,
                          fptu_256, fptu_cstr, fptu_opaque
                          /*, fptu_nested, fptu_farray */),
        ::testing::Values(fpta_primary_withdups,
                          fpta_primary_withdups_reversed,
                          fpta_primary_withdups_unordered),
        ::testing::Values(fpta_unsorted, fpta_ascending, fpta_descending)));

#else

TEST(CursorPrimary, GoogleTestCombine_IS_NOT_Supported_OnThisPlatform) {}
TEST(CursorPrimaryDups, GoogleTestCombine_IS_NOT_Supported_OnThisPlatform) {}

#endif /* GTEST_HAS_COMBINE */

//----------------------------------------------------------------------------

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

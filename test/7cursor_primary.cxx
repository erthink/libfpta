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
#ifdef FPTA_CURSOR_UT_LONG
static constexpr int NNN = 65521; // около 1-2 минуты в /dev/shm/
#else
static constexpr int NNN = 509; // менее секунды в /dev/shm/
#endif

static const char testdb_name[] = TEST_DB_DIR "ut_cursor_primary.fpta";
static const char testdb_name_lck[] =
    TEST_DB_DIR "ut_cursor_primary.fpta" MDBX_LOCK_SUFFIX;

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
  int n_records;
  std::unordered_map<int, int> reorder;

  static int mesh(int n) { return (int)((163 + (unsigned)n * 42101) % NNN); }

  void CheckPosition(int linear, int dup_id, int expected_n_dups = 0,
                     bool check_dup_id = true) {
    if (linear < 0) {
      /* для удобства и выразительности теста linear = -1 здесь
       * означает последнюю запись, -2 = предпоследнюю и т.д. */
      linear += (int)reorder.size();
    }

    if (dup_id < 0) {
      /* для удобства и выразительности теста dup_id = -1 здесь
       * означает последний дубликат, -2 = предпоследний и т.д. */
      dup_id += n_dups;
    }

    if (expected_n_dups == 0) {
      /* для краткости теста expected_n_dups = 0 здесь означает значение
       * по-умолчанию, когда строки-дубликаты не удаляются в ходе теста */
      expected_n_dups = fpta_index_is_unique(index) ? 1 : n_dups;
    }

    SCOPED_TRACE("linear-order " + std::to_string(linear) + " [0..." +
                 std::to_string(reorder.size() - 1) + "], linear-dup " +
                 (check_dup_id ? std::to_string(dup_id) : "any"));

    ASSERT_EQ(1u, reorder.count(linear));
    const auto expected_order = reorder.at(linear);

    /* Следует пояснить (в том числе напомнить себе), почему порядок
     * следования строк-дубликатов (с одинаковым значением PK) здесь
     * совпадает с dup_id:
     *  - При формировании строк-дубликатов, их кортежи полностью совпадают,
     *    за исключением поля dup_id. При этом dup_id отличается только
     *    одним байтом.
     *  - В случае primary-индекса данные, соответствующие одному значению
     *    ключа, сортируются при помощи компаратора fptu_cmp_tuples(), что
     *    формирует порядок по возрастанию dup_id. Однако, даже при
     *    сравнении посредством memcmp(), различие только в одном байте
     *    также сформирует порядок по возрастанию dup_id.
     *
     * Таким образом, физический порядок строк-дубликатов всегда по
     * возрастанию dup_id (причем нет видимых причин это как-либо менять).
     *
     * --------------------------------------------------------------------
     *
     * Далее, для курсора с обратным порядком сортировки (descending)
     * видимый порядок строк будет изменен на обратный, включая порядок
     * строк-дубликатов. Предполагается что такая симметричность будет
     * более ожидаема и удобна, нежели если порядок дубликатов
     * сохранится (не будет обратным).
     *
     * Соответственно ниже для descending-курсора выполняется "переворот"
     * контрольного номера дубликата. */
    const int expected_dup_id =
        fpta_index_is_unique(index) ? 42 : fpta_cursor_is_descending(ordering)
                                               ? n_dups - (dup_id + 1)
                                               : dup_id;

    SCOPED_TRACE("logical-order " + std::to_string(expected_order) + " [" +
                 std::to_string(0) + "..." + std::to_string(NNN) +
                 "), logical-dup " +
                 (check_dup_id ? std::to_string(expected_dup_id) : "any"));

    ASSERT_EQ(FPTA_OK, fpta_cursor_eof(cursor_guard.get()));

    fptu_ro tuple;
    ASSERT_EQ(FPTA_OK, fpta_cursor_get(cursor_guard.get(), &tuple));
    ASSERT_STREQ(nullptr, fptu_check_ro(tuple));

    int error;
    fpta_value key;
    ASSERT_EQ(FPTA_OK, fpta_cursor_key(cursor_guard.get(), &key));
    SCOPED_TRACE("key: " + std::to_string(key.type) + ", length " +
                 std::to_string(key.binary_length));

    auto tuple_order = (int)fptu_get_sint(tuple, col_order.column.num, &error);
    ASSERT_EQ(FPTU_OK, error);
    ASSERT_EQ(expected_order, tuple_order);

    auto tuple_checksum = fptu_get_uint(tuple, col_t1ha.column.num, &error);
    ASSERT_EQ(FPTU_OK, error);
    auto checksum = order_checksum(tuple_order, type, index).uint;
    ASSERT_EQ(checksum, tuple_checksum);

    auto tuple_dup_id =
        (int)fptu_get_uint(tuple, col_dup_id.column.num, &error);
    ASSERT_EQ(FPTU_OK, error);
    if (check_dup_id || fpta_index_is_unique(index))
      EXPECT_EQ(expected_dup_id, tuple_dup_id);

    size_t dups = 100500;
    ASSERT_EQ(FPTA_OK, fpta_cursor_dups(cursor_guard.get(), &dups));
    ASSERT_EQ(expected_n_dups, dups);
  }

  virtual void Fill() {
    fptu_rw *row = fptu_alloc(4, fpta_max_keylen * 2 + 4 + 4);
    ASSERT_NE(nullptr, row);
    ASSERT_STREQ(nullptr, fptu_check(row));
    fpta_txn *const txn = txn_guard.get();

    any_keygen keygen(type, index);
    n_records = 0;
    for (int linear = 0; linear < NNN; ++linear) {
      int order = mesh(linear);
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
        ++n_records;
      } else {
        for (int dup_id = 0; dup_id < n_dups; ++dup_id) {
          ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_dup_id,
                                                fpta_value_sint(dup_id)));
          ASSERT_EQ(FPTA_OK,
                    fpta_insert_row(txn, &table, fptu_take_noshrink(row)));
          ++n_records;
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
    ASSERT_GE(65535u, NNN);
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

    SCOPED_TRACE(
        "type " + std::to_string(type) + ", index " + std::to_string(index) +
        (valid_index_ops ? ", (valid index case)" : ", (invalid index case)"));

    SCOPED_TRACE("ordering " + std::to_string(ordering) + ", index " +
                 std::to_string(index) +
                 (valid_cursor_ops ? ", (valid cursor case)"
                                   : ", (invalid cursor case)"));

    // инициализируем идентификаторы колонок
    ASSERT_EQ(FPTA_OK, fpta_table_init(&table, "table"));
    pk_col_name = "pk_" + std::to_string(type);
    ASSERT_EQ(FPTA_OK, fpta_column_init(&table, &col_pk, pk_col_name.c_str()));
    ASSERT_EQ(FPTA_OK, fpta_column_init(&table, &col_order, "order"));
    ASSERT_EQ(FPTA_OK, fpta_column_init(&table, &col_dup_id, "dup_id"));
    ASSERT_EQ(FPTA_OK, fpta_column_init(&table, &col_t1ha, "t1ha"));

    // создаем четыре колонки: pk, order, t1ha и dup_id
    fpta_column_set def;
    fpta_column_set_init(&def);

    if (!valid_index_ops) {
      EXPECT_NE(FPTA_OK,
                fpta_column_describe(pk_col_name.c_str(), type, index, &def));
      EXPECT_EQ(FPTA_OK, fpta_column_describe("order", fptu_int32,
                                              fpta_index_none, &def));
      ASSERT_NE(FPTA_OK, fpta_column_set_validate(&def));

      // разрушаем описание таблицы
      EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def));
      EXPECT_NE(FPTA_OK, fpta_column_set_validate(&def));
      return;
    }

    EXPECT_EQ(FPTA_OK,
              fpta_column_describe(pk_col_name.c_str(), type, index, &def));
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe("order", fptu_int32, fpta_index_none, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe("dup_id", fptu_uint16,
                                            fpta_noindex_nullable, &def));
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe("t1ha", fptu_uint64, fpta_index_none, &def));
    ASSERT_EQ(FPTA_OK, fpta_column_set_validate(&def));

    // чистим
    if (REMOVE_FILE(testdb_name) != 0)
      ASSERT_EQ(ENOENT, errno);
    if (REMOVE_FILE(testdb_name_lck) != 0)
      ASSERT_EQ(ENOENT, errno);

#ifdef FPTA_CURSOR_UT_LONG
    // пытаемся обойтись меньшей базой,
    // но для строк потребуется больше места
    unsigned megabytes = 32;
    if (type > fptu_96)
      megabytes = 56;
    if (type > fptu_256)
      megabytes = 64;
#else
    unsigned megabytes = 1;
#endif

    /* В пике нужно примерно вдвое больше места, так как один из тестов
     * будет выполнять удаление до половины строк в случайном порядке,
     * и в результате может обновить почти каждую страницу БД. */
    megabytes += megabytes;

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

    // разрушаем описание таблицы
    EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def));
    EXPECT_NE(FPTA_OK, fpta_column_set_validate(&def));

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
      EXPECT_EQ(FPTA_OK,
                fpta_cursor_open(txn, &col_pk, fpta_value_begin(),
                                 fpta_value_end(), nullptr, ordering, &cursor));
      ASSERT_NE(nullptr, cursor);
      cursor_guard.reset(cursor);
    } else {
      EXPECT_EQ(FPTA_NO_INDEX,
                fpta_cursor_open(txn, &col_pk, fpta_value_begin(),
                                 fpta_value_end(), nullptr, ordering, &cursor));
      cursor_guard.reset(cursor);
      ASSERT_EQ(nullptr, cursor);
      return;
    }

    // формируем линейную карту, чтобы проще проверять переходы
    reorder.clear();
    reorder.reserve(NNN);
    int prev_order = -1;
    for (int linear = 0; fpta_cursor_eof(cursor) == FPTA_OK; ++linear) {
      fptu_ro tuple;
      EXPECT_EQ(FPTA_OK, fpta_cursor_get(cursor_guard.get(), &tuple));
      ASSERT_STREQ(nullptr, fptu_check_ro(tuple));

      int error;
      auto tuple_order =
          (int)fptu_get_sint(tuple, col_order.column.num, &error);
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

      // проверяем упорядоченность
      if (fpta_cursor_is_ordered(ordering) && linear > 0) {
        if (fpta_cursor_is_ascending(ordering))
          ASSERT_LE(prev_order, tuple_order);
        else
          ASSERT_GE(prev_order, tuple_order);
      }
      prev_order = tuple_order;
    }
    ASSERT_EQ(NNN, reorder.size());
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
      ASSERT_TRUE(REMOVE_FILE(testdb_name) == 0);
      ASSERT_TRUE(REMOVE_FILE(testdb_name_lck) == 0);
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
  CHECK_RUNTIME_LIMIT_OR_SKIP();
  if (!valid_index_ops || !valid_cursor_ops)
    return;

  SCOPED_TRACE("type " + std::to_string(type) + ", index " +
               std::to_string(index) +
               (valid_index_ops ? ", (valid case)" : ", (invalid case)"));

  SCOPED_TRACE(
      "ordering " + std::to_string(ordering) + ", index " +
      std::to_string(index) +
      (valid_cursor_ops ? ", (valid cursor case)" : ", (invalid cursor case)"));

  ASSERT_LT(5u, n_records);
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

/* Другое имя класса требуется для инстанцирования другого (меньшего)
 * набора комбинаций в INSTANTIATE_TEST_CASE_P. */
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
  CHECK_RUNTIME_LIMIT_OR_SKIP();
  if (!valid_index_ops || !valid_cursor_ops || fpta_index_is_unique(index))
    return;

  SCOPED_TRACE("type " + std::to_string(type) + ", index " +
               std::to_string(index) +
               (valid_index_ops ? ", (valid case)" : ", (invalid case)"));

  SCOPED_TRACE(
      "ordering " + std::to_string(ordering) + ", index " +
      std::to_string(index) +
      (valid_cursor_ops ? ", (valid cursor case)" : ", (invalid cursor case)"));

  ASSERT_LT(5u, n_records);
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

//----------------------------------------------------------------------------

TEST_P(CursorPrimary, locate_and_delele) {
  /* Проверка позиционирования курсора по первичному (primary) индексу.
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
   *  5. После формирования "карты" выполняются несколько проверочных
   *     итераций, в конце каждой из которых часть записей удаляется:
   *      - выполняется позиционирование на значение ключа для каждого
   *        элемента проверочной "карты", которая была сформирована
   *        на предыдущем шаге.
   *      - проверяется успешность операции с учетом того был ли элемент
   *        уже удален или еще нет.
   *      - проверяется результирующая позиция курсора.
   *      - удаляется часть строк: текущая транзакция чтения закрывается,
   *        открывается пишущая, выполняется удаление, курсор переоткрывается.
   *      - после каждого удаления проверяется что позиция курсора
   *        соответствует ожиданиям (сделан переход к следующей записи
   *        в порядке курсора).
   *      - итерации повторяются пока не будут удалены все строки.
   *      - при выполнении всех проверок и удалений строки выбираются
   *        в стохастическом порядке.
   *
   *  6. Завершаются операции и освобождаются ресурсы.
   */
  CHECK_RUNTIME_LIMIT_OR_SKIP();
  if (!valid_index_ops || !valid_cursor_ops)
    return;

  SCOPED_TRACE("type " + std::to_string(type) + ", index " +
               std::to_string(index) +
               (valid_index_ops ? ", (valid case)" : ", (invalid case)"));

  SCOPED_TRACE(
      "ordering " + std::to_string(ordering) + ", index " +
      std::to_string(index) +
      (valid_cursor_ops ? ", (valid cursor case)" : ", (invalid cursor case)"));
  ASSERT_LT(5u, n_records);

  /* заполняем present "номерами" значений ключа существующих записей,
   * важно что эти "номера" через карту позволяют получить соответствующее
   * значения от генератора ключей */
  std::vector<int> present;
  std::map<int, int> dups_countdown;
  for (const auto &pair : reorder) {
    present.push_back(pair.first);
    dups_countdown[pair.first] = fpta_index_is_unique(index) ? 1 : n_dups;
  }
  // сохраняем исходный полный набор
  auto initial = present;

  any_keygen keygen(type, index);
  for (;;) {
    SCOPED_TRACE("records left " + std::to_string(present.size()));

    // перемешиваем
    for (size_t i = 0; i < present.size(); ++i) {
      auto remix = (4201 + i * 2017) % present.size();
      std::swap(present[i], present[remix]);
    }
    for (size_t i = 0; i < initial.size(); ++i) {
      auto remix = (44741 + i * 55001) % initial.size();
      std::swap(initial[i], initial[remix]);
    }

    // начинаем транзакцию чтения если предыдущая закрыта
    fpta_txn *txn = txn_guard.get();
    if (!txn_guard) {
      EXPECT_EQ(FPTA_OK,
                fpta_transaction_begin(db_quard.get(), fpta_read, &txn));
      ASSERT_NE(nullptr, txn);
      txn_guard.reset(txn);
    }

    // открываем курсор для чтения
    fpta_cursor *cursor = nullptr;
    EXPECT_EQ(FPTA_OK,
              fpta_cursor_open(
                  txn_guard.get(), &col_pk, fpta_value_begin(),
                  fpta_value_end(), nullptr,
                  (fpta_cursor_options)(ordering | fpta_dont_fetch), &cursor));
    ASSERT_NE(nullptr, cursor);
    cursor_guard.reset(cursor);

    // проверяем позиционирование
    for (size_t i = 0; i < initial.size(); ++i) {
      const auto linear = initial.at(i);
      ASSERT_EQ(1u, reorder.count(linear));
      const auto order = reorder[linear];
      int expected_dups =
          dups_countdown.count(linear) ? dups_countdown.at(linear) : 0;
      SCOPED_TRACE("linear " + std::to_string(linear) + ", order " +
                   std::to_string(order) +
                   (dups_countdown.count(linear) ? ", present" : ", deleted"));

      fpta_value key = keygen.make(order, NNN);
      size_t dups = 100500;
      switch (expected_dups) {
      case 0:
        /* все значения уже были удалены,
         * точный поиск (exactly=true) должен вернуть "нет данных" */
        ASSERT_EQ(FPTA_NODATA, fpta_cursor_locate(cursor, true, &key, nullptr));
        ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
        ASSERT_EQ(FPTA_ECURSOR, fpta_cursor_dups(cursor_guard.get(), &dups));
        ASSERT_EQ((size_t)FPTA_DEADBEEF, dups);
        if (present.size()) {
          /* но какие-то строки в таблице еще есть, поэтому неточный
           * поиск (exactly=false) должен вернуть "ОК" если:
           *  - для курсора определен порядок строк.
           *  - в порядке курсора есть строки "после" запрошенного значения
           *    ключа (аналогично lower_bound и с учетом того, что строк
           *    с заданным значением ключа уже нет). */
          const auto lower_bound = dups_countdown.lower_bound(linear);
          if (fpta_cursor_is_ordered(ordering) &&
              lower_bound != dups_countdown.end()) {
            const auto expected_linear = lower_bound->first;
            const auto expected_order = reorder[expected_linear];
            expected_dups = lower_bound->second;
            SCOPED_TRACE("lower-bound: linear " +
                         std::to_string(expected_linear) + ", order " +
                         std::to_string(expected_order) + ", n-dups " +
                         std::to_string(expected_dups));
            ASSERT_EQ(FPTA_OK,
                      fpta_cursor_locate(cursor, false, &key, nullptr));
            ASSERT_NO_FATAL_FAILURE(
                CheckPosition(expected_linear,
                              /* см ниже пояснение о expected_dup_number */
                              n_dups - expected_dups, expected_dups));
          } else {
            if (fpta_cursor_is_ordered(ordering) ||
                !FPTA_PROHIBIT_NEARBY4UNORDERED) {
              ASSERT_EQ(FPTA_NODATA,
                        fpta_cursor_locate(cursor, false, &key, nullptr));
            } else {
              ASSERT_NE(FPTA_OK,
                        fpta_cursor_locate(cursor, false, &key, nullptr));
            }
            ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
            ASSERT_EQ(FPTA_ECURSOR,
                      fpta_cursor_dups(cursor_guard.get(), &dups));
            ASSERT_EQ((size_t)FPTA_DEADBEEF, dups);
          }
        } else {
          if (fpta_cursor_is_ordered(ordering) ||
              !FPTA_PROHIBIT_NEARBY4UNORDERED) {
            ASSERT_EQ(FPTA_NODATA,
                      fpta_cursor_locate(cursor, false, &key, nullptr));
          } else {
            ASSERT_NE(FPTA_OK,
                      fpta_cursor_locate(cursor, false, &key, nullptr));
          }
          ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
          ASSERT_EQ(FPTA_ECURSOR, fpta_cursor_dups(cursor_guard.get(), &dups));
          ASSERT_EQ((size_t)FPTA_DEADBEEF, dups);
        }
        continue;
      case 1:
        if (fpta_cursor_is_ordered(ordering) ||
            !FPTA_PROHIBIT_NEARBY4UNORDERED) {
          ASSERT_EQ(FPTA_OK, fpta_cursor_locate(cursor, false, &key, nullptr));
          ASSERT_NO_FATAL_FAILURE(CheckPosition(linear, -1, 1));
        } else {
          ASSERT_NE(FPTA_OK, fpta_cursor_locate(cursor, false, &key, nullptr));
          ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
          ASSERT_EQ(FPTA_ECURSOR, fpta_cursor_dups(cursor_guard.get(), &dups));
        }
        ASSERT_EQ(FPTA_OK, fpta_cursor_locate(cursor, true, &key, nullptr));
        ASSERT_NO_FATAL_FAILURE(CheckPosition(linear, -1, 1));
        break;
      default:
        /* Пояснения о expected_dup_number:
         *  - курсор позиционируется на первый дубликат в порядке сортировки
         *    в соответствии с типом курсора;
         *  - удаление записей (ниже по коду) выполняется после такого
         *    позиционирования;
         *  - соответственно, постепенно удаляются все дубликаты,
         *    начиная с первого в порядке сортировки курсора.
         *
         * Таким образом, ожидаемое количество дубликатов также определяет
         * dup_id первой строки-дубликата, на которую должен встать курсор. */
        int const expected_dup_number = n_dups - expected_dups;
        SCOPED_TRACE("multi-val: n-dups " + std::to_string(expected_dups) +
                     ", here-dup " + std::to_string(expected_dup_number));
        ASSERT_EQ(FPTA_OK, fpta_cursor_locate(cursor, true, &key, nullptr));
        ASSERT_EQ(FPTA_OK, fpta_cursor_eof(cursor));
        ASSERT_NO_FATAL_FAILURE(
            CheckPosition(linear, expected_dup_number, expected_dups));

        if (fpta_cursor_is_ordered(ordering) ||
            !FPTA_PROHIBIT_NEARBY4UNORDERED) {
          ASSERT_EQ(FPTA_OK, fpta_cursor_locate(cursor, false, &key, nullptr));
          ASSERT_NO_FATAL_FAILURE(
              CheckPosition(linear, expected_dup_number, expected_dups));
        } else {
          ASSERT_NE(FPTA_OK, fpta_cursor_locate(cursor, false, &key, nullptr));
          ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
          ASSERT_EQ(FPTA_ECURSOR, fpta_cursor_dups(cursor_guard.get(), &dups));
        }
        break;
      }
    }

    // закрываем читающий курсор и транзакцию
    ASSERT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
    cursor = nullptr;
    ASSERT_EQ(FPTA_OK, fpta_transaction_end(txn_guard.release(), true));
    txn = nullptr;

    if (present.size() == 0)
      break;

    // начинаем пишущую транзакцию для удаления
    EXPECT_EQ(FPTA_OK,
              fpta_transaction_begin(db_quard.get(), fpta_write, &txn));
    ASSERT_NE(nullptr, txn);
    txn_guard.reset(txn);

    // открываем курсор для удаления
    EXPECT_EQ(FPTA_OK,
              fpta_cursor_open(txn_guard.get(), &col_pk, fpta_value_begin(),
                               fpta_value_end(), nullptr, ordering, &cursor));
    ASSERT_NE(nullptr, cursor);
    cursor_guard.reset(cursor);

    // проверяем позиционирование и удаляем
    for (size_t i = present.size(); i > present.size() / 2;) {
      const auto linear = present.at(--i);
      const auto order = reorder.at(linear);
      int expected_dups = dups_countdown.at(linear);
      SCOPED_TRACE("delete: linear " + std::to_string(linear) + ", order " +
                   std::to_string(order) + ", dups left " +
                   std::to_string(expected_dups));
      fpta_value key = keygen.make(order, NNN);

      ASSERT_EQ(FPTA_OK, fpta_cursor_locate(cursor, true, &key, nullptr));
      ASSERT_EQ(0, fpta_cursor_eof(cursor));
      size_t dups = 100500;
      ASSERT_EQ(FPTA_OK, fpta_cursor_dups(cursor_guard.get(), &dups));
      ASSERT_EQ(expected_dups, dups);

      ASSERT_EQ(FPTA_OK, fpta_cursor_delete(cursor));
      expected_dups = --dups_countdown.at(linear);
      if (expected_dups == 0) {
        present.erase(present.begin() + (int)i);
        dups_countdown.erase(linear);
      }

      // проверяем состояние курсора и его переход к следующей строке
      if (present.empty()) {
        ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
        ASSERT_EQ(FPTA_NODATA, fpta_cursor_dups(cursor_guard.get(), &dups));
        ASSERT_EQ(0u, dups);
      } else if (expected_dups) {
        ASSERT_NO_FATAL_FAILURE(
            CheckPosition(linear,
                          /* см выше пояснение о expected_dup_number */
                          n_dups - expected_dups, expected_dups));
      } else if (fpta_cursor_is_ordered(ordering)) {
        const auto lower_bound = dups_countdown.lower_bound(linear);
        if (lower_bound != dups_countdown.end()) {
          const auto expected_linear = lower_bound->first;
          const auto expected_order = reorder[expected_linear];
          expected_dups = lower_bound->second;
          SCOPED_TRACE("after-delete: linear " +
                       std::to_string(expected_linear) + ", order " +
                       std::to_string(expected_order) + ", n-dups " +
                       std::to_string(expected_dups));
          ASSERT_NO_FATAL_FAILURE(
              CheckPosition(expected_linear,
                            /* см выше пояснение о expected_dup_number */
                            n_dups - expected_dups, expected_dups));

        } else {
          ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
          ASSERT_EQ(FPTA_NODATA, fpta_cursor_dups(cursor_guard.get(), &dups));
          ASSERT_EQ(0u, dups);
        }
      }
    }

    // завершаем транзакцию удаления
    ASSERT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
    cursor = nullptr;
    ASSERT_EQ(FPTA_OK, fpta_transaction_end(txn_guard.release(), false));
    txn = nullptr;
  }
}

//----------------------------------------------------------------------------

TEST_P(CursorPrimary, update_and_KeyMismatch) {
  /* Проверка обновления через курсор, в том числе с попытками изменить
   * значение "курсорной" колонки.
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
   * 5. Примерно половина строк (без учета дубликатов) изменяется
   *    через курсор:
   *      - в качестве критерия изменять/не-менять используется
   *        младший бит значения колонки "t1ha".
   *      - в изменяемых строках инвертируется знак колонки "ордер",
   *        а колонка"dup_id" устанавливается в 4242.
   *      - при каждом реальном обновлении делается две попытки обновить
   *        строку с изменением значения ключевой "курсорной" колонки.
   *
   * 6. Выполняется проверка всех строк, как исходных, так и измененных.
   *    При наличии дубликатов, измененные строки ищутся по значению
   *    колонки "dup_id".
   */
  CHECK_RUNTIME_LIMIT_OR_SKIP();
  if (!valid_index_ops || !valid_cursor_ops)
    return;

  SCOPED_TRACE("type " + std::to_string(type) + ", index " +
               std::to_string(index) +
               (valid_index_ops ? ", (valid case)" : ", (invalid case)"));

  SCOPED_TRACE(
      "ordering " + std::to_string(ordering) + ", index " +
      std::to_string(index) +
      (valid_cursor_ops ? ", (valid cursor case)" : ", (invalid cursor case)"));

  any_keygen keygen(type, index);
  const int expected_dups = fpta_index_is_unique(index) ? 1 : n_dups;

  // закрываем читающий курсор и транзакцию
  ASSERT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  ASSERT_EQ(FPTA_OK, fpta_transaction_end(txn_guard.release(), true));

  // начинаем пишущую транзакцию для изменений
  fpta_txn *txn = nullptr;
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db_quard.get(), fpta_write, &txn));
  ASSERT_NE(nullptr, txn);
  txn_guard.reset(txn);

  // открываем курсор для изменения
  fpta_cursor *cursor = nullptr;
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn_guard.get(), &col_pk, fpta_value_begin(),
                             fpta_value_end(), nullptr, ordering, &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);

  // обновляем половину строк
  for (int order = 0; order < NNN; ++order) {
    SCOPED_TRACE("order " + std::to_string(order));
    fpta_value value_pk = keygen.make(order, NNN);

    ASSERT_EQ(FPTA_OK, fpta_cursor_locate(cursor, true, &value_pk, nullptr));
    ASSERT_EQ(FPTA_OK, fpta_cursor_eof(cursor_guard.get()));

    fptu_ro tuple;
    ASSERT_EQ(FPTA_OK, fpta_cursor_get(cursor_guard.get(), &tuple));
    ASSERT_STREQ(nullptr, fptu_check_ro(tuple));

    int error;
    auto tuple_order = (int)fptu_get_sint(tuple, col_order.column.num, &error);
    ASSERT_EQ(FPTU_OK, error);
    ASSERT_EQ(order, tuple_order);

    auto tuple_checksum = fptu_get_uint(tuple, col_t1ha.column.num, &error);
    ASSERT_EQ(FPTU_OK, error);
    const auto checksum = order_checksum(tuple_order, type, index).uint;
    ASSERT_EQ(checksum, tuple_checksum);

    if (checksum & 1) {
      uint8_t buffer[(fpta_max_keylen + 4) * 4 + sizeof(fptu_rw)];
      fptu_rw *row = fptu_fetch(tuple, buffer, sizeof(buffer), 1);
      ASSERT_NE(nullptr, row);
      ASSERT_STREQ(nullptr, fptu_check(row));

      // инвертируем знак order и пытаемся обновить строку с изменением ключа
      ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_order,
                                            fpta_value_sint(-tuple_order)));
      value_pk = keygen.make((order + 42) % NNN, NNN);
      ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_pk, value_pk));
      ASSERT_EQ(FPTA_KEY_MISMATCH,
                fpta_cursor_probe_and_update(cursor, fptu_take(row)));

      // восстанавливаем значение ключа и обновляем строку
      value_pk = keygen.make(order, NNN);
      ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_pk, value_pk));
      // для простоты контроля среди дубликатов устанавливаем dup_id = 4242
      ASSERT_EQ(FPTA_OK,
                fpta_upsert_column(row, &col_dup_id, fpta_value_sint(4242)));
      ASSERT_EQ(FPTA_OK, fpta_cursor_probe_and_update(cursor, fptu_take(row)));

      // для проверки еще раз пробуем "сломать" ключ
      value_pk = keygen.make((order + 24) % NNN, NNN);
      ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_pk, value_pk));
      ASSERT_EQ(FPTA_KEY_MISMATCH,
                fpta_cursor_probe_and_update(cursor, fptu_take_noshrink(row)));

      size_t ndups;
      ASSERT_EQ(FPTA_OK, fpta_cursor_dups(cursor, &ndups));
      ASSERT_EQ(expected_dups, ndups);
    }
  }

  // завершаем транзакцию изменения
  ASSERT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;
  ASSERT_EQ(FPTA_OK, fpta_transaction_commit(txn_guard.release()));
  txn = nullptr;

  // начинаем транзакцию чтения если предыдущая закрыта
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db_quard.get(), fpta_read, &txn));
  ASSERT_NE(nullptr, txn);
  txn_guard.reset(txn);

  // открываем курсор для чтения
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn_guard.get(), &col_pk, fpta_value_begin(),
                             fpta_value_end(), nullptr,
                             (fpta_cursor_options)(ordering | fpta_dont_fetch),
                             &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);

  // проверяем обновления
  for (int order = 0; order < NNN; ++order) {
    SCOPED_TRACE("order " + std::to_string(order));
    const fpta_value value_pk = keygen.make(order, NNN);
    fptu_ro tuple;
    int error;

    ASSERT_EQ(FPTA_OK, fpta_cursor_locate(cursor, true, &value_pk, nullptr));
    ASSERT_EQ(FPTA_OK, fpta_cursor_eof(cursor_guard.get()));

    size_t ndups;
    ASSERT_EQ(FPTA_OK, fpta_cursor_dups(cursor, &ndups));
    ASSERT_EQ(expected_dups, ndups);

    const auto checksum = order_checksum(order, type, index).uint;
    for (;;) {
      ASSERT_EQ(FPTA_OK, fpta_cursor_get(cursor_guard.get(), &tuple));
      ASSERT_STREQ(nullptr, fptu_check_ro(tuple));

      auto tuple_checksum = fptu_get_uint(tuple, col_t1ha.column.num, &error);
      ASSERT_EQ(FPTU_OK, error);
      ASSERT_EQ(checksum, tuple_checksum);

      auto tuple_dup_id =
          (int)fptu_get_uint(tuple, col_dup_id.column.num, &error);
      ASSERT_EQ(FPTU_OK, error);

      // идем по строкам-дубликатом до той, которую обновляли
      if (tuple_dup_id != 4242 && (checksum & 1))
        ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_dup_next));
      else
        break;
    }

    auto tuple_order = (int)fptu_get_sint(tuple, col_order.column.num, &error);
    ASSERT_EQ(FPTU_OK, error);
    int expected_order = order;
    if (checksum & 1)
      expected_order = -expected_order;
    EXPECT_EQ(expected_order, tuple_order);
  }
}

//----------------------------------------------------------------------------

#if GTEST_HAS_COMBINE

INSTANTIATE_TEST_CASE_P(
    Combine, CursorPrimary,
    ::testing::Combine(
        ::testing::Values(fptu_null, fptu_uint16, fptu_int32, fptu_uint32,
                          fptu_fp32, fptu_int64, fptu_uint64, fptu_fp64,
                          fptu_datetime, fptu_96, fptu_128, fptu_160, fptu_256,
                          fptu_cstr, fptu_opaque
                          /*, fptu_nested, fptu_farray */),
        ::testing::Values(fpta_primary_unique_ordered_obverse,
                          fpta_primary_unique_ordered_reverse,
                          fpta_primary_withdups_ordered_obverse,
                          fpta_primary_withdups_ordered_reverse,
                          fpta_primary_unique_unordered,
                          fpta_primary_withdups_unordered),
        ::testing::Values(fpta_unsorted, fpta_ascending, fpta_descending)));

INSTANTIATE_TEST_CASE_P(
    Combine, CursorPrimaryDups,
    ::testing::Combine(
        ::testing::Values(fptu_null, fptu_uint16, fptu_int32, fptu_uint32,
                          fptu_fp32, fptu_int64, fptu_uint64, fptu_fp64,
                          fptu_datetime, fptu_96, fptu_128, fptu_160, fptu_256,
                          fptu_cstr, fptu_opaque
                          /*, fptu_nested, fptu_farray */),
        ::testing::Values(fpta_primary_withdups_ordered_obverse,
                          fpta_primary_withdups_ordered_reverse,
                          fpta_primary_withdups_unordered),
        ::testing::Values(fpta_unsorted, fpta_ascending, fpta_descending)));

#else

TEST(CursorPrimary, GoogleTestCombine_IS_NOT_Supported_OnThisPlatform) {}
TEST(CursorPrimaryDups, GoogleTestCombine_IS_NOT_Supported_OnThisPlatform) {}

#endif /* GTEST_HAS_COMBINE */

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

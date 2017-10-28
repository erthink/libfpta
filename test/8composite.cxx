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

static const char testdb_name[] = TEST_DB_DIR "ut_composite.fpta";
static const char testdb_name_lck[] =
    TEST_DB_DIR "ut_composite.fpta" MDBX_LOCK_SUFFIX;

//----------------------------------------------------------------------------
TEST(SmokeComposite, Primary) {
  /* Smoke-проверка жизнеспособности составных индексов в роли первичных.
   *
   * Сценарий:
   *  1. Создаем базу с одной таблицей, в которой три колонки
   *     и один составной primary индекс (четвертая псевдо-колонка).
   *  2. Добавляем данные:
   *     - добавляем "первую" запись, одновременно пытаясь
   *       добавить в строку-кортеж поля с "плохими" значениями.
   *     - добавляем "вторую" запись, которая отличается от первой
   *       всеми колонками.
   *     - также попутно пытаемся обновить несуществующие записи
   *       и вставить дубликаты.
   *  3. Читаем добавленное:
   *     - открываем курсор по составному индексу, без фильтра,
   *       на всю таблицу (весь диапазон строк),
   *       и проверяем кол-во записей и дубликатов.
   *     - переходим к последней, читаем и проверяем её (должна быть
   *       "вторая").
   *     - переходим к первой, читаем и проверяем её (должна быть "первая").
   *  4. Удаляем данные:
   *     - сначала "вторую" запись, потом "первую".
   *     - проверяем кол-во записей и дубликатов, eof для курсора.
   *  5. Завершаем операции и освобождаем ресурсы.
   */
  if (REMOVE_FILE(testdb_name) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }
  if (REMOVE_FILE(testdb_name_lck) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }

  // открываем/создаем базульку в 1 мегабайт
  fpta_db *db = nullptr;
  EXPECT_EQ(FPTA_SUCCESS,
            fpta_db_open(testdb_name, fpta_weak, fpta_regime_default, 0644, 1,
                         true, &db));
  ASSERT_NE(nullptr, db);

  // описываем простейшую таблицу с тремя колонками и одним составным PK
  fpta_column_set def;
  fpta_column_set_init(&def);

  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("a_str", fptu_cstr, fpta_index_none, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("b_uint", fptu_uint64, fpta_index_none, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "c_fp", fptu_fp64,
                         fpta_secondary_withdups_ordered_obverse, &def));
/* у нас всегда код C++, но ниже просто пример использования
 * расширенного интерфейса и его аналог на C. */
#ifdef __cplusplus
  EXPECT_EQ(FPTA_OK, fpta::describe_composite_index(
                         "pk", fpta_primary_unique_ordered_obverse, &def,
                         "b_uint", "a_str", "c_fp"));
#else
  EXPECT_EQ(FPTA_OK, fpta_describe_composite_index_va(
                         "pk", fpta_primary_unique_ordered_obverse, &def,
                         "b_uint", "a_str", "c_fp", nullptr));
#endif
  EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def));

  // запускам транзакцию и создаем таблицу с обозначенным набором колонок
  fpta_txn *txn = (fpta_txn *)&txn;
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
  ASSERT_NE(nullptr, txn);
  EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "table_1", &def));
  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  // разрушаем описание таблицы
  EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def));
  EXPECT_NE(FPTA_OK, fpta_column_set_validate(&def));

  // инициализируем идентификаторы таблицы и её колонок
  fpta_name table, col_a, col_b, col_c, col_pk;
  EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "table_1"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_a, "a_str"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_b, "b_uint"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_c, "c_fp"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_pk, "pk"));

  // начинаем транзакцию для вставки данных
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_write, &txn));
  ASSERT_NE(nullptr, txn);
  // ради теста делаем привязку вручную
  EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &col_pk));
  EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_a));
  EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_b));
  EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_c));

  // проверяем иформацию о таблице (сейчас таблица пуста)
  size_t row_count;
  fpta_table_stat stat;
  memset(&row_count, 42, sizeof(row_count));
  memset(&stat, 42, sizeof(stat));
  EXPECT_EQ(FPTA_OK, fpta_table_info(txn, &table, &row_count, &stat));
  EXPECT_EQ(0u, row_count);
  EXPECT_EQ(row_count, stat.row_count);
  EXPECT_EQ(0u, stat.btree_depth);
  EXPECT_EQ(0u, stat.large_pages);
  EXPECT_EQ(0u, stat.branch_pages);
  EXPECT_EQ(0u, stat.leaf_pages);
  EXPECT_EQ(0u, stat.total_bytes);

  // создаем кортеж, который станет первой записью в таблице
  fptu_rw *pt1 = fptu_alloc(3, 42);
  ASSERT_NE(nullptr, pt1);
  ASSERT_STREQ(nullptr, fptu_check(pt1));

  // ради проверки пытаемся сделать нехорошее (добавить поля с нарушениями)
  EXPECT_EQ(FPTA_ETYPE, fpta_upsert_column(pt1, &col_a, fpta_value_uint(12)));
  EXPECT_EQ(FPTA_EVALUE, fpta_upsert_column(pt1, &col_b, fpta_value_sint(-34)));
  EXPECT_EQ(FPTA_ETYPE,
            fpta_upsert_column(pt1, &col_c, fpta_value_cstr("x-string")));

  // добавляем нормальные значения
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt1, &col_c, fpta_value_float(56.78)));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt1, &col_a, fpta_value_cstr("string")));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt1, &col_b, fpta_value_sint(34)));
  ASSERT_STREQ(nullptr, fptu_check(pt1));

  // создаем еще один кортеж для второй записи
  fptu_rw *pt2 = fptu_alloc(3, 42);
  ASSERT_NE(nullptr, pt2);
  ASSERT_STREQ(nullptr, fptu_check(pt2));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt2, &col_a, fpta_value_cstr("zzz")));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt2, &col_b, fpta_value_sint(90)));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt2, &col_c, fpta_value_float(12.34)));
  ASSERT_STREQ(nullptr, fptu_check(pt2));

  // пытаемся обновить несуществующую запись
  EXPECT_EQ(FPTA_NOTFOUND,
            fpta_update_row(txn, &table, fptu_take_noshrink(pt1)));
  // вставляем и обновляем
  EXPECT_EQ(FPTA_OK, fpta_insert_row(txn, &table, fptu_take_noshrink(pt1)));
  EXPECT_EQ(FPTA_OK, fpta_upsert_row(txn, &table, fptu_take_noshrink(pt1)));
  EXPECT_EQ(FPTA_OK, fpta_update_row(txn, &table, fptu_take_noshrink(pt1)));
  EXPECT_EQ(FPTA_KEYEXIST,
            fpta_insert_row(txn, &table, fptu_take_noshrink(pt1)));

  // аналогично со второй записью
  EXPECT_EQ(FPTA_NOTFOUND,
            fpta_update_row(txn, &table, fptu_take_noshrink(pt2)));
  EXPECT_EQ(FPTA_OK, fpta_insert_row(txn, &table, fptu_take_noshrink(pt2)));
  EXPECT_EQ(FPTA_OK, fpta_upsert_row(txn, &table, fptu_take_noshrink(pt2)));
  EXPECT_EQ(FPTA_OK, fpta_update_row(txn, &table, fptu_take_noshrink(pt2)));
  EXPECT_EQ(FPTA_KEYEXIST,
            fpta_insert_row(txn, &table, fptu_take_noshrink(pt2)));

  // фиксируем изменения
  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  // и начинаем следующую транзакцию
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_write, &txn));
  ASSERT_NE(nullptr, txn);

  // открываем простейщий курсор: на всю таблицу, без фильтра
  fpta_cursor *cursor;
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn, &col_pk, fpta_value_begin(), fpta_value_end(),
                             nullptr, fpta_unsorted_dont_fetch, &cursor));
  ASSERT_NE(nullptr, cursor);

  // узнам сколько записей за курсором (в таблице).
  size_t count;
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(2u, count);

  // снова проверяем иформацию о таблице (сейчас в таблице две строки)
  memset(&row_count, 42, sizeof(row_count));
  memset(&stat, 42, sizeof(stat));
  EXPECT_EQ(FPTA_OK, fpta_table_info(txn, &table, &row_count, &stat));
  EXPECT_EQ(2u, row_count);
  EXPECT_EQ(row_count, stat.row_count);
  EXPECT_EQ(1u, stat.btree_depth);
  EXPECT_EQ(0u, stat.large_pages);
  EXPECT_EQ(0u, stat.branch_pages);
  EXPECT_EQ(1u, stat.leaf_pages);
  EXPECT_LE(512u, stat.total_bytes);

  // переходим к последней записи
  EXPECT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_last));
  // ради проверки убеждаемся что за курсором есть данные
  EXPECT_EQ(FPTA_OK, fpta_cursor_eof(cursor));

  // считаем повторы, их не должно быть
  size_t dups;
  EXPECT_EQ(FPTA_OK, fpta_cursor_dups(cursor, &dups));
  EXPECT_EQ(1u, dups);

  // получаем текущую строку, она должна совпадать со вторым кортежем
  fptu_ro row2;
  EXPECT_EQ(FPTA_OK, fpta_cursor_get(cursor, &row2));
  ASSERT_STREQ(nullptr, fptu_check_ro(row2));
  EXPECT_EQ(fptu_eq, fptu_cmp_tuples(fptu_take_noshrink(pt2), row2));

  // создаем третий кортеж для получения составного ключа
  fptu_rw *pt3 = fptu_alloc(3, 21);
  ASSERT_NE(nullptr, pt3);

  // сначала пробуем несуществующую комбинацию,
  // причем из существующих значений, но из разных строк таблицы
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt3, &col_b, fpta_value_sint(90)));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt3, &col_a, fpta_value_cstr("string")));
  ASSERT_STREQ(nullptr, fptu_check(pt3));

  // получаем составной ключ
  uint8_t key_buffer[fpta_keybuf_len];
  fpta_value pk_composite_key;
  // пробуем без одной колонки
  EXPECT_EQ(FPTA_COLUMN_MISSING,
            fpta_get_column2buffer(fptu_take_noshrink(pt3), &col_pk,
                                   &pk_composite_key, key_buffer,
                                   sizeof(key_buffer)));
  // добавляем недостающую колонку
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt3, &col_c, fpta_value_float(56.78)));
  ASSERT_EQ(FPTA_OK, fpta_get_column2buffer(fptu_take_noshrink(pt3), &col_pk,
                                            &pk_composite_key, key_buffer,
                                            sizeof(key_buffer)));

  // получаем составной ключ из оригинального кортежа
  uint8_t key_buffer2[fpta_keybuf_len];
  fpta_value pk_composite_origin;
  ASSERT_EQ(FPTA_OK, fpta_get_column2buffer(fptu_take_noshrink(pt1), &col_pk,
                                            &pk_composite_origin, key_buffer2,
                                            sizeof(key_buffer2)));
  EXPECT_EQ(pk_composite_origin.binary_length, pk_composite_key.binary_length);
  EXPECT_GT(0, memcmp(pk_composite_origin.binary_data,
                      pk_composite_key.binary_data,
                      pk_composite_key.binary_length));

  // позиционируем курсор на конкретное НЕсуществующее составное значение
  EXPECT_EQ(FPTA_NODATA,
            fpta_cursor_locate(cursor, true, &pk_composite_key, nullptr));
  EXPECT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));

  // теперь формируем ключ для существующей комбинации
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt3, &col_b, fpta_value_sint(34)));
  ASSERT_EQ(FPTA_OK, fpta_get_column2buffer(fptu_take_noshrink(pt3), &col_pk,
                                            &pk_composite_key, key_buffer,
                                            sizeof(key_buffer)));
  EXPECT_EQ(pk_composite_origin.binary_length, pk_composite_key.binary_length);
  EXPECT_EQ(0, memcmp(pk_composite_origin.binary_data,
                      pk_composite_key.binary_data,
                      pk_composite_key.binary_length));

  // позиционируем курсор на конкретное составное значение
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_locate(cursor, true, &pk_composite_key, nullptr));
  EXPECT_EQ(FPTA_OK, fpta_cursor_eof(cursor));

  // ради проверки считаем повторы
  EXPECT_EQ(FPTA_OK, fpta_cursor_dups(cursor, &dups));
  EXPECT_EQ(1u, dups);

  // получаем текущую строку, она должна совпадать с первым кортежем
  fptu_ro row1;
  EXPECT_EQ(FPTA_OK, fpta_cursor_get(cursor, &row1));
  ASSERT_STREQ(nullptr, fptu_check_ro(row1));
  EXPECT_EQ(fptu_eq, fptu_cmp_tuples(fptu_take_noshrink(pt1), row1));

  // разрушаем созданные кортежи
  // на всякий случай предварительно проверяя их
  ASSERT_STREQ(nullptr, fptu_check(pt1));
  free(pt1);
  pt1 = nullptr;
  ASSERT_STREQ(nullptr, fptu_check(pt2));
  free(pt2);
  pt2 = nullptr;
  ASSERT_STREQ(nullptr, fptu_check(pt3));
  free(pt3);
  pt3 = nullptr;

  // удяляем текущую запись через курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_delete(cursor));
  // считаем сколько записей теперь, должа быть одна
  EXPECT_EQ(FPTA_OK, fpta_cursor_dups(cursor, &dups));
  EXPECT_EQ(1u, dups);
  // ради теста проверям что данные есть
  EXPECT_EQ(FPTA_OK, fpta_cursor_eof(cursor));
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(1u, count);

  // переходим к первой записи
  EXPECT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_first));
  // еще раз удаляем запись
  EXPECT_EQ(FPTA_OK, fpta_cursor_delete(cursor));
#if FPTA_ENABLE_RETURN_INTO_RANGE
  // теперь должно быть пусто
  EXPECT_EQ(FPTA_NODATA, fpta_cursor_dups(cursor, &dups));
  EXPECT_EQ(0u, dups);
#else
  // курсор должен стать неустановленным
  EXPECT_EQ(FPTA_ECURSOR, fpta_cursor_dups(cursor, &dups));
  EXPECT_EQ((size_t)FPTA_DEADBEEF, dups);
#endif
  // ради теста проверям что данных больше нет
  EXPECT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(0u, count);

  // закрываем курсор и завершаем транзакцию
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor));
  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  // разрушаем привязанные идентификаторы
  fpta_name_destroy(&table);
  fpta_name_destroy(&col_a);
  fpta_name_destroy(&col_b);
  fpta_name_destroy(&col_c);
  fpta_name_destroy(&col_pk);

  // закрываем базульку
  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));

  // пока не удялем файлы чтобы можно было посмотреть и натравить mdbx_chk
  if (false) {
    ASSERT_TRUE(REMOVE_FILE(testdb_name) == 0);
    ASSERT_TRUE(REMOVE_FILE(testdb_name_lck) == 0);
  }
}

//----------------------------------------------------------------------------

TEST(SmokeIndex, Secondary) {
  /* Smoke-проверка жизнеспособности составных индексов в роли вторичных.
   *
   * Сценарий:
   *  1. Создаем базу с одной таблицей, в которой три колонки, и два индекса:
   *       - primary.
   *       - составной secondary (четвертая псевдо-колонка).
   *  2. Добавляем данные:
   *      - добавляем "первую" запись, одновременно пытаясь
   *        добавить в строку-кортеж поля с "плохими" значениями.
   *      - добавляем "вторую" запись, которая отличается от первой
   *        всеми колонками.
   *      - также попутно пытаемся обновить несуществующие записи
   *        и вставить дубликаты.
   *  3. Читаем добавленное:
   *     - открываем курсор по вторичному индексу, без фильтра,
   *       на всю таблицу (весь диапазон строк),
   *       и проверяем кол-во записей и дубликатов.
   *     - переходим к последней, читаем и проверяем её (должна быть
   *       "вторая").
   *     - переходим к первой, читаем и проверяем её (должна быть "первая").
   *  4. Удаляем данные:
   *     - сначала "вторую" запись, потом "первую".
   *     - проверяем кол-во записей и дубликатов, eof для курсора.
   *  5. Завершаем операции и освобождаем ресурсы.
   */
  if (REMOVE_FILE(testdb_name) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }
  if (REMOVE_FILE(testdb_name_lck) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }

  // открываем/создаем базульку в 1 мегабайт
  fpta_db *db = nullptr;
  EXPECT_EQ(FPTA_SUCCESS,
            fpta_db_open(testdb_name, fpta_weak, fpta_regime_default, 0644, 1,
                         true, &db));
  ASSERT_NE(nullptr, db);

  // описываем простейшую таблицу с тремя колонками,
  // одним Primary и одним составным Secondary
  fpta_column_set def;
  fpta_column_set_init(&def);

  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("pk_str_uniq", fptu_cstr,
                                 fpta_primary_unique_ordered_reverse, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "a_sint", fptu_int64,
                         fpta_secondary_withdups_ordered_obverse, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe(
                "b_fp", fptu_fp64,
                fpta_secondary_withdups_unordered_nullable_obverse, &def));

  // пробуем несколько недопустимых комбинаций:
  // - избыточная уникальность для составного индекса при уникальности
  //   одной из колонок (pk_str_uniq)
  EXPECT_EQ(FPTA_SIMILAR_INDEX,
            fpta_describe_composite_index_va(
                "se", fpta_secondary_unique_ordered_obverse, &def, "a_sint",
                "b_fp", "pk_str_uniq", nullptr));

  // - по колонке a_sint уже есть сортирующий/упорядоченный индекс,
  //   и эта колонка первая в составном индексе (который также упорядоченный),
  //   поэтому индекс по колонке a_sint избыточен, так как вместо него может
  //   быть использован составной.
  EXPECT_EQ(FPTA_SIMILAR_INDEX,
            fpta_describe_composite_index_va(
                "se", fpta_secondary_withdups_ordered_obverse, &def, "a_sint",
                "b_fp", "pk_str_uniq", nullptr));

  // - аналогично по колонке pk_str_uniq уже есть сортирующий/упорядоченный
  //   реверсивный индекс и эта колонка последняя в составном индексе (который
  //   также упорядоченный и реверсивный) поэтому индекс по колонке pk_str_uniq
  //   избыточен, так как вместо него может быть использован составной.
  EXPECT_EQ(FPTA_SIMILAR_INDEX,
            fpta_describe_composite_index_va(
                "se", fpta_secondary_withdups_ordered_reverse, &def, "a_sint",
                "b_fp", "pk_str_uniq", nullptr));

  // допустимый вариант, колонку pk_str_uniq ставим последней, чтобы проще
  // проверить усечение составного ключа.
  EXPECT_EQ(FPTA_OK, fpta_describe_composite_index_va(
                         "se", fpta_secondary_withdups_ordered_obverse, &def,
                         "b_fp", "a_sint", "pk_str_uniq", nullptr));

  EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def));

  // запускам транзакцию и создаем таблицу с обозначенным набором колонок
  fpta_txn *txn = (fpta_txn *)&txn;
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
  ASSERT_NE(nullptr, txn);
  EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "table_1", &def));
  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  // разрушаем описание таблицы
  EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def));
  EXPECT_NE(FPTA_OK, fpta_column_set_validate(&def));

  // инициализируем идентификаторы таблицы и её колонок
  fpta_name table, col_pk, col_a, col_b, col_se;
  EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "table_1"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_pk, "pk_str_uniq"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_a, "a_sint"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_b, "b_fp"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_se, "se"));

  // начинаем транзакцию для вставки данных
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_write, &txn));
  ASSERT_NE(nullptr, txn);
  // ради теста делаем привязку вручную
  EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &col_pk));
  EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_a));
  EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_b));

  // создаем кортеж, который станет первой записью в таблице
  fptu_rw *pt1 = fptu_alloc(3, 42);
  ASSERT_NE(nullptr, pt1);
  ASSERT_STREQ(nullptr, fptu_check(pt1));

  // ради проверки пытаемся сделать нехорошее (добавить поля с нарушениями)
  EXPECT_EQ(FPTA_ETYPE, fpta_upsert_column(pt1, &col_pk, fpta_value_uint(12)));
  EXPECT_EQ(FPTA_ETYPE, fpta_upsert_column(pt1, &col_a, fpta_value_float(1.0)));
  EXPECT_EQ(FPTA_ETYPE,
            fpta_upsert_column(pt1, &col_b, fpta_value_cstr("x-string")));

  // добавляем нормальные значения
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt1, &col_pk, fpta_value_cstr("first_")));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt1, &col_a, fpta_value_sint(90)));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt1, &col_b, fpta_value_float(56.78)));
  ASSERT_STREQ(nullptr, fptu_check(pt1));

  // создаем еще один кортеж для второй записи
  fptu_rw *pt2 = fptu_alloc(3, 42 + fpta_max_keylen);
  ASSERT_NE(nullptr, pt2);
  ASSERT_STREQ(nullptr, fptu_check(pt2));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(
                         pt2, &col_pk,
                         fpta_value_str(std::string(fpta_max_keylen, 'z'))));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt2, &col_a, fpta_value_sint(90)));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt2, &col_b, fpta_value_float(56.78)));
  ASSERT_STREQ(nullptr, fptu_check(pt2));

  // пытаемся обновить несуществующую запись
  EXPECT_EQ(FPTA_NOTFOUND,
            fpta_update_row(txn, &table, fptu_take_noshrink(pt1)));
  // вставляем и обновляем
  EXPECT_EQ(FPTA_OK, fpta_insert_row(txn, &table, fptu_take_noshrink(pt1)));
  EXPECT_EQ(FPTA_OK, fpta_upsert_row(txn, &table, fptu_take_noshrink(pt1)));
  EXPECT_EQ(FPTA_OK, fpta_update_row(txn, &table, fptu_take_noshrink(pt1)));
  EXPECT_EQ(FPTA_KEYEXIST,
            fpta_insert_row(txn, &table, fptu_take_noshrink(pt1)));

  // аналогично со второй записью
  EXPECT_EQ(FPTA_NOTFOUND,
            fpta_update_row(txn, &table, fptu_take_noshrink(pt2)));
  EXPECT_EQ(FPTA_OK, fpta_insert_row(txn, &table, fptu_take_noshrink(pt2)));
  EXPECT_EQ(FPTA_OK, fpta_upsert_row(txn, &table, fptu_take_noshrink(pt2)));
  EXPECT_EQ(FPTA_OK, fpta_update_row(txn, &table, fptu_take_noshrink(pt2)));
  EXPECT_EQ(FPTA_KEYEXIST,
            fpta_insert_row(txn, &table, fptu_take_noshrink(pt2)));

  // фиксируем изменения
  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  // и начинаем следующую транзакцию
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_write, &txn));
  ASSERT_NE(nullptr, txn);

  // открываем простейщий курсор: на всю таблицу, без фильтра
  fpta_cursor *cursor;
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn, &col_se, fpta_value_begin(), fpta_value_end(),
                             nullptr, fpta_unsorted_dont_fetch, &cursor));
  ASSERT_NE(nullptr, cursor);
  fptu_ro row;

  // узнам сколько записей за курсором (в таблице).
  size_t count;
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(2u, count);

  // переходим к первой записи
  EXPECT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_first));
  // ради проверки убеждаемся что за курсором есть данные
  EXPECT_EQ(FPTA_OK, fpta_cursor_eof(cursor));
  // получаем текущую строку, она должна совпадать с первым кортежем
  EXPECT_EQ(FPTA_OK, fpta_cursor_get(cursor, &row));
  ASSERT_STREQ(nullptr, fptu_check_ro(row));
  EXPECT_EQ(fptu_eq, fptu_cmp_tuples(fptu_take_noshrink(pt1), row));

  // считаем повторы, их не должно быть
  size_t dups;
  EXPECT_EQ(FPTA_OK, fpta_cursor_dups(cursor, &dups));
  ASSERT_EQ(1u, dups);

  // переходим к последней записи
  EXPECT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_last));
  // ради проверки убеждаемся что за курсором есть данные
  EXPECT_EQ(FPTA_OK, fpta_cursor_eof(cursor));

  // получаем текущую строку, она должна совпадать со вторым кортежем
  EXPECT_EQ(FPTA_OK, fpta_cursor_get(cursor, &row));
  ASSERT_STREQ(nullptr, fptu_check_ro(row));
  EXPECT_EQ(fptu_eq, fptu_cmp_tuples(fptu_take_noshrink(pt2), row));

  // считаем повторы, их не должно быть
  EXPECT_EQ(FPTA_OK, fpta_cursor_dups(cursor, &dups));
  ASSERT_EQ(1u, dups);

  // получаем составной ключ из оригинального кортежа
  uint8_t key_buffer_origin[fpta_keybuf_len];
  fpta_value se_composite_origin;
  ASSERT_EQ(FPTA_OK, fpta_get_column2buffer(
                         fptu_take_noshrink(pt1), &col_se, &se_composite_origin,
                         key_buffer_origin, sizeof(key_buffer_origin)));

  // создаем третий кортеж для получения составного ключа
  fptu_rw *pt3 = fptu_alloc(4, 42 + fpta_max_keylen);
  ASSERT_NE(nullptr, pt3);
  // здесь будет составной ключ
  uint8_t key_buffer[fpta_keybuf_len];
  fpta_value se_composite_key;

  // пробуем без двух колонок, из которых одна nullable
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt3, &col_pk, fpta_value_cstr("absent")));
  ASSERT_STREQ(nullptr, fptu_check(pt3));
  EXPECT_EQ(FPTA_COLUMN_MISSING,
            fpta_get_column2buffer(fptu_take_noshrink(pt3), &col_se,
                                   &se_composite_key, key_buffer,
                                   sizeof(key_buffer)));

  // добавляем недостающую обязательную колонку
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt3, &col_a, fpta_value_sint(90)));
  // теперь мы должы получить составной ключ
  EXPECT_EQ(FPTA_OK, fpta_get_column2buffer(fptu_take_noshrink(pt3), &col_se,
                                            &se_composite_key, key_buffer,
                                            sizeof(key_buffer)));
  // но это будет несуществующая комбинация
  EXPECT_EQ(FPTA_NODATA,
            fpta_cursor_locate(cursor, true, &se_composite_key, nullptr));
  EXPECT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
  // ключ от которой должен быть:
  //  - РАВНОЙ ДЛИНЫ, так как все компоненты имеют равную длину:
  //    значения в col_pk имеют равную длину строк, а отсутствующая col_b
  //    будет заменена на DENIL (NaN).
  //  - МЕНЬШЕ при сравнении через memcmp(), так как на первом месте
  //    в составном индексе идет col_b типа fptu64, и DENIL (NaN) для неё
  //    после конвертации в байты составного ключа должен быть меньше 56.87
  EXPECT_EQ(se_composite_origin.binary_length, se_composite_key.binary_length);
  EXPECT_LT(0, memcmp(se_composite_origin.binary_data,
                      se_composite_key.binary_data,
                      se_composite_key.binary_length));

  // добавляем недостающую nullable колонку
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt3, &col_b, fpta_value_float(56.78)));
  EXPECT_EQ(FPTA_OK, fpta_get_column2buffer(fptu_take_noshrink(pt3), &col_se,
                                            &se_composite_key, key_buffer,
                                            sizeof(key_buffer)));
  // но это также будет несуществующая комбинация
  EXPECT_EQ(FPTA_NODATA,
            fpta_cursor_locate(cursor, true, &se_composite_key, nullptr));
  EXPECT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
  // ключ от которой должен быть:
  //  - РАВНОЙ ДЛИНЫ, так как все компоненты имеют равную длину.
  //  - МЕНЬШЕ при сравнении через memcmp(), так как отличие только
  //    в значениях "absent" < "first_"
  EXPECT_EQ(se_composite_origin.binary_length, se_composite_key.binary_length);
  EXPECT_LT(0, memcmp(se_composite_origin.binary_data,
                      se_composite_key.binary_data,
                      se_composite_key.binary_length));

  // позиционируем курсор на конкретное НЕсуществующее составное значение
  EXPECT_EQ(FPTA_NODATA,
            fpta_cursor_locate(cursor, true, &se_composite_key, nullptr));
  EXPECT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));

  // теперь формируем ключ для существующей комбинации
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt3, &col_pk, fpta_value_cstr("first_")));
  EXPECT_EQ(FPTA_OK, fpta_get_column2buffer(fptu_take_noshrink(pt3), &col_se,
                                            &se_composite_key, key_buffer,
                                            sizeof(key_buffer)));
  EXPECT_EQ(se_composite_origin.binary_length, se_composite_key.binary_length);
  EXPECT_EQ(0, memcmp(se_composite_origin.binary_data,
                      se_composite_key.binary_data,
                      se_composite_key.binary_length));

  // позиционируем курсор на конкретное составное значение
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_locate(cursor, true, &se_composite_key, nullptr));
  EXPECT_EQ(FPTA_OK, fpta_cursor_eof(cursor));

  // ради проверки считаем повторы
  EXPECT_EQ(FPTA_OK, fpta_cursor_dups(cursor, &dups));
  EXPECT_EQ(1u, dups);

  // получаем текущую строку, она должна совпадать с первым кортежем
  EXPECT_EQ(FPTA_OK, fpta_cursor_get(cursor, &row));
  ASSERT_STREQ(nullptr, fptu_check_ro(row));
  EXPECT_EQ(fptu_eq, fptu_cmp_tuples(fptu_take_noshrink(pt1), row));

  // теперь формируем ключ для второй существующей комбинации
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(
                         pt3, &col_pk,
                         fpta_value_str(std::string(fpta_max_keylen, 'z'))));
  EXPECT_EQ(FPTA_OK, fpta_get_column2buffer(fptu_take_noshrink(pt3), &col_se,
                                            &se_composite_key, key_buffer,
                                            sizeof(key_buffer)));
  // ключ от которой должен быть ДЛИННЕЕ и БОЛЬШЕ:
  EXPECT_LT(se_composite_origin.binary_length, se_composite_key.binary_length);
  EXPECT_GT(0, memcmp(se_composite_origin.binary_data,
                      se_composite_key.binary_data,
                      std::min(se_composite_key.binary_length,
                               se_composite_origin.binary_length)));
  // позиционируем курсор на конкретное составное значение,
  // это вторая и последняя строка таблицы
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_locate(cursor, true, &se_composite_key, nullptr));
  EXPECT_EQ(FPTA_OK, fpta_cursor_eof(cursor));

  // получаем составной ключ, для комбинации с отрицательным значением
  // посередине
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt3, &col_a, fpta_value_sint(INT64_MIN)));
  EXPECT_EQ(FPTA_OK, fpta_get_column2buffer(fptu_take_noshrink(pt3), &col_se,
                                            &se_composite_key, key_buffer,
                                            sizeof(key_buffer)));
  // ключ от которой должен быть ДЛИННЕЕ и МЕНЬШЕ:
  EXPECT_LT(se_composite_origin.binary_length, se_composite_key.binary_length);
  EXPECT_LT(0, memcmp(se_composite_origin.binary_data,
                      se_composite_key.binary_data,
                      std::min(se_composite_key.binary_length,
                               se_composite_origin.binary_length)));

  // разрушаем созданные кортежи
  // на всякий случай предварительно проверяя их
  ASSERT_STREQ(nullptr, fptu_check(pt1));
  free(pt1);
  pt1 = nullptr;
  ASSERT_STREQ(nullptr, fptu_check(pt2));
  free(pt2);
  pt2 = nullptr;
  ASSERT_STREQ(nullptr, fptu_check(pt3));
  free(pt3);
  pt3 = nullptr;

  // удяляем текущую запись через курсор, это вторая и последняя строка таблицы
  EXPECT_EQ(FPTA_OK, fpta_cursor_delete(cursor));
  // считаем сколько записей теперь, должа быть одна
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(1u, count);

  // переходим к первой записи
  EXPECT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_first));
  // еще раз удаляем запись
  EXPECT_EQ(FPTA_OK, fpta_cursor_delete(cursor));
#if FPTA_ENABLE_RETURN_INTO_RANGE
  // теперь должно быть пусто
  EXPECT_EQ(FPTA_NODATA, fpta_cursor_dups(cursor, &dups));
  EXPECT_EQ(0u, dups);
#else
  // курсор должен стать неустановленным
  EXPECT_EQ(FPTA_ECURSOR, fpta_cursor_dups(cursor, &dups));
  EXPECT_EQ((size_t)FPTA_DEADBEEF, dups);
#endif
  // ради теста проверям что данных больше нет
  EXPECT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(0u, count);

  // закрываем курсор и завершаем транзакцию
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor));
  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  // разрушаем привязанные идентификаторы
  fpta_name_destroy(&table);
  fpta_name_destroy(&col_pk);
  fpta_name_destroy(&col_a);
  fpta_name_destroy(&col_b);
  fpta_name_destroy(&col_se);

  // закрываем базульку
  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));

  // пока не удялем файлы чтобы можно было посмотреть и натравить mdbx_chk
  if (false) {
    ASSERT_TRUE(REMOVE_FILE(testdb_name) == 0);
    ASSERT_TRUE(REMOVE_FILE(testdb_name_lck) == 0);
  }
}

//----------------------------------------------------------------------------

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

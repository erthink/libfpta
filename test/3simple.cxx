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

static const char testdb_name[] = "ut_simple.fpta";
static const char testdb_name_lck[] = "ut_simple.fpta-lock";

TEST(Simple, Base)
{
    ASSERT_TRUE(unlink(testdb_name) == 0 || errno == ENOENT);
    ASSERT_TRUE(unlink(testdb_name_lck) == 0 || errno == ENOENT);

    // открываем/создаем базульку в 1 мегабайт
    fpta_db *db = nullptr;
    EXPECT_EQ(FPTA_SUCCESS,
              fpta_db_open(testdb_name, fpta_async, 0644, 1, true, &db));
    ASSERT_NE(nullptr, db);

    // описываем простейшую таблицу с тремя колонками и одним PK
    fpta_column_set def;
    fpta_column_set_init(&def);

    EXPECT_EQ(FPTA_OK, fpta_column_describe("pk_str_uniq", fptu_cstr,
                                            fpta_primary_unique, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe("a_uint", fptu_uint64,
                                            fpta_index_none, &def));
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe("b_fp", fptu_fp64, fpta_index_none, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def));

    // запускам транзакцию и создаем таблицу с обозначенным набором колонок
    fpta_txn *txn = (fpta_txn *)&txn;
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
    ASSERT_NE(nullptr, txn);
    EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "table_1", &def));
    EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
    txn = nullptr;

    // инициализируем идентификаторы таблицы и её колонок
    fpta_name table, col_pk, col_a, col_b;
    EXPECT_EQ(FPTA_OK, fpta_name_init(&table, "table_1", fpta_table));
    EXPECT_EQ(FPTA_OK, fpta_name_init(&col_pk, "pk_str_uniq", fpta_column));
    EXPECT_EQ(FPTA_OK, fpta_name_init(&col_a, "a_uint", fpta_column));
    EXPECT_EQ(FPTA_OK, fpta_name_init(&col_b, "b_fp", fpta_column));

    // начинаем транзакцию для вставки данных
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_write, &txn));
    ASSERT_NE(nullptr, txn);
    // ради теста делаем привязку вручную
    EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &table, &col_pk));
    EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &table, &col_a));
    EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &table, &col_b));

    // создаем кортеж, который станет первой записью в таблице
    fptu_rw *pt1 = fptu_alloc(3, 42);
    ASSERT_NE(nullptr, pt1);
    ASSERT_STREQ(nullptr, fptu_check(pt1));

    // ради проверки пытаемся сделать нехорошее (добавить поля с нарушениями)
    EXPECT_EQ(FPTA_ETYPE,
              fpta_upsert_column(pt1, &col_pk, fpta_value_uint(12)));
    EXPECT_EQ(FPTA_EVALUE,
              fpta_upsert_column(pt1, &col_a, fpta_value_sint(-34)));
    EXPECT_EQ(FPTA_ETYPE,
              fpta_upsert_column(pt1, &col_b, fpta_value_cstr("string")));

    // добавляем нормальные значения
    EXPECT_EQ(FPTA_OK,
              fpta_upsert_column(pt1, &col_pk, fpta_value_cstr("pk-string")));
    EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt1, &col_a, fpta_value_sint(34)));
    EXPECT_EQ(FPTA_OK,
              fpta_upsert_column(pt1, &col_b, fpta_value_float(56.78)));
    ASSERT_STREQ(nullptr, fptu_check(pt1));

    // создаем еще один кортеж для второй записи
    fptu_rw *pt2 = fptu_alloc(3, 42);
    ASSERT_NE(nullptr, pt2);
    ASSERT_STREQ(nullptr, fptu_check(pt2));
    EXPECT_EQ(FPTA_OK,
              fpta_upsert_column(pt2, &col_pk, fpta_value_cstr("zzz")));
    EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt2, &col_a, fpta_value_sint(90)));
    EXPECT_EQ(FPTA_OK,
              fpta_upsert_column(pt2, &col_b, fpta_value_float(12.34)));
    ASSERT_STREQ(nullptr, fptu_check(pt2));

    // пытаемся обновить несуществующую запись
    EXPECT_EQ(MDB_NOTFOUND,
              fpta_update_row(txn, &table, fptu_take_noshrink(pt1)));
    // вставляем и обновляем
    EXPECT_EQ(FPTA_OK, fpta_insert_row(txn, &table, fptu_take_noshrink(pt1)));
    EXPECT_EQ(FPTA_OK, fpta_upsert_row(txn, &table, fptu_take_noshrink(pt1)));
    EXPECT_EQ(FPTA_OK, fpta_update_row(txn, &table, fptu_take_noshrink(pt1)));
    EXPECT_EQ(MDB_KEYEXIST,
              fpta_insert_row(txn, &table, fptu_take_noshrink(pt1)));

    // аналогичто со второй записью
    EXPECT_EQ(MDB_NOTFOUND,
              fpta_update_row(txn, &table, fptu_take_noshrink(pt2)));
    EXPECT_EQ(FPTA_OK, fpta_insert_row(txn, &table, fptu_take_noshrink(pt2)));
    EXPECT_EQ(FPTA_OK, fpta_upsert_row(txn, &table, fptu_take_noshrink(pt2)));
    EXPECT_EQ(FPTA_OK, fpta_update_row(txn, &table, fptu_take_noshrink(pt2)));
    EXPECT_EQ(MDB_KEYEXIST,
              fpta_insert_row(txn, &table, fptu_take_noshrink(pt2)));

    // фиксируем изменения
    EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
    // и начинаем следующую транзакцию
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_write, &txn));
    ASSERT_NE(nullptr, txn);

    // открываем простейщий курсор: на всю таблицу, без фильтра
    fpta_cursor *cursor;
    EXPECT_EQ(FPTA_OK,
              fpta_cursor_open(txn, &table, &col_pk, fpta_value_begin(),
                               fpta_value_end(), nullptr,
                               fpta_unsorted_dont_fetch, &cursor));
    ASSERT_NE(nullptr, cursor);

    // узнам сколько записей за курсором (в таблице).
    size_t count;
    EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
    EXPECT_EQ(2, count);

    // переходим к последней записи
    EXPECT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_last));
    // ради проверки убеждаемся что за курсором есть данные
    EXPECT_EQ(FPTA_OK, fpta_cursor_eof(cursor));

    // считаем повторы, их не должно быть
    size_t dups;
    EXPECT_EQ(FPTA_OK, fpta_cursor_dups(cursor, &dups));
    EXPECT_EQ(1, dups);

    // получаем текущую строку, она должна совпадать со вторым кортежем
    fptu_ro row2;
    EXPECT_EQ(FPTA_OK, fpta_cursor_get(cursor, &row2));
    ASSERT_STREQ(nullptr, fptu_check_ro(row2));
    EXPECT_EQ(fptu_eq, fptu_cmp_tuples(fptu_take_noshrink(pt2), row2));

    // позиционируем курсор на конкретное значение ключевого поля
    fpta_value pk = fpta_value_cstr("pk-string");
    EXPECT_EQ(FPTA_OK, fpta_cursor_locate(cursor, true, &pk, nullptr));
    EXPECT_EQ(FPTA_OK, fpta_cursor_eof(cursor));

    // ради проверки считаем повторы
    EXPECT_EQ(FPTA_OK, fpta_cursor_dups(cursor, &dups));
    EXPECT_EQ(1, dups);

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

    // удяляем текущую запись через курсор
    EXPECT_EQ(FPTA_OK, fpta_cursor_delete(cursor, true));
    // считаем сколько записей теперь, должа быть одна
    EXPECT_EQ(FPTA_OK, fpta_cursor_dups(cursor, &dups));
    EXPECT_EQ(1, dups);
    // ради теста проверям что данные есть
    EXPECT_EQ(FPTA_OK, fpta_cursor_eof(cursor));
    EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
    EXPECT_EQ(1, count);

    // переходим к первой записи
    EXPECT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_first));
    // еще раз удаляем запись
    EXPECT_EQ(FPTA_OK, fpta_cursor_delete(cursor, true));
// теперь должно быть пусто
#if FPTA_ENABLE_RETURN_INTO_RANGE
    EXPECT_EQ(FPTA_NODATA, fpta_cursor_dups(cursor, &dups));
    EXPECT_EQ(0, dups);
#else
    EXPECT_EQ(FPTA_ECURSOR, fpta_cursor_dups(cursor, &dups));
    EXPECT_EQ(FPTA_DEADBEEF, dups);
#endif
    // ради теста проверям что данных больше нет
    EXPECT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
    EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
    EXPECT_EQ(0, count);

    // закрываем курсор и завершаем транзакцию
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor));
    EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
    txn = nullptr;

    // разрушаем привязанные идентификаторы
    fpta_name_destroy(&table);
    fpta_name_destroy(&col_pk);
    fpta_name_destroy(&col_a);
    fpta_name_destroy(&col_b);

    // закрываем базульку
    EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));

    // пока не удялем файлы чтобы можно было посмотреть и натравить mdbx_chk
    if (false) {
        ASSERT_TRUE(unlink(testdb_name) == 0);
        ASSERT_TRUE(unlink(testdb_name_lck) == 0);
    }
}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

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

#define FPTA_QUICK_INDEX_UT
#include "keygen.hpp"

struct db_deleter : public std::unary_function<void, fpta_db *> {
    void operator()(fpta_db *db) const
    {
        if (db)
            EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));
    }
};

struct txn_deleter : public std::unary_function<void, fpta_txn *> {
    void operator()(fpta_txn *txn) const
    {
        if (txn)
            ASSERT_EQ(FPTA_OK, fpta_transaction_end(txn, true));
    }
};

struct cursor_deleter : public std::unary_function<void, fpta_cursor *> {
    void operator()(fpta_cursor *cursor) const
    {
        if (cursor)
            ASSERT_EQ(FPTA_OK, fpta_cursor_close(cursor));
    }
};

typedef std::unique_ptr<fpta_db, db_deleter> scoped_db_guard;
typedef std::unique_ptr<fpta_txn, txn_deleter> scoped_txn_guard;
typedef std::unique_ptr<fpta_cursor, cursor_deleter> scoped_cursor_guard;

static const char testdb_name[] = "ut_index.fpta";
static const char testdb_name_lck[] = "ut_index.fpta-lock";

TEST(Index, keygen)
{
    scalar_range_stepper<float, 43>::test();
    scalar_range_stepper<double, 43>::test();
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

    fixbin_stepper<11>::test();
    varbin_stepper<fptu_cstr, 421>::test();
    varbin_stepper<fptu_opaque, 421>::test();
}

TEST(Index, Invalid)
{
    // TODO
    //    static const fpta_index_type index_cases[] = {
    //        /* clang-format off */
    //        fpta_primary_unique, fpta_primary_unique_unordered,
    //        fpta_primary_unique_reversed
    //        /* clang-format on */
    //    };
}

template <fptu_type type, fpta_index_type index> void TestPrimary()
{
    const bool valid = is_valid4pk(type, index);
    scoped_db_guard db_quard;
    scoped_txn_guard txn_guard;

    SCOPED_TRACE("type " + std::to_string(type) + ", index " +
                 std::to_string(index) +
                 (valid ? ", (valid case)" : ", (invalid case)"));
    // создаем четыре колонки: pk, order, t1ha и dup_id
    fpta_column_set def;
    fpta_column_set_init(&def);

    const std::string pk_col_name = "pk_" + std::to_string(type);
    if (valid) {
        EXPECT_EQ(FPTA_OK, fpta_column_describe(pk_col_name.c_str(), type,
                                                index, &def));
        EXPECT_EQ(FPTA_OK, fpta_column_describe("order", fptu_int32,
                                                fpta_index_none, &def));
        EXPECT_EQ(FPTA_OK, fpta_column_describe("dup_id", fptu_uint16,
                                                fpta_index_none, &def));
        EXPECT_EQ(FPTA_OK, fpta_column_describe("t1ha", fptu_uint64,
                                                fpta_index_none, &def));
        ASSERT_EQ(FPTA_OK, fpta_column_set_validate(&def));
    } else {
        EXPECT_EQ(FPTA_EINVAL, fpta_column_describe(pk_col_name.c_str(), type,
                                                    index, &def));
        EXPECT_EQ(FPTA_OK, fpta_column_describe("order", fptu_int32,
                                                fpta_index_none, &def));
        ASSERT_NE(FPTA_OK, fpta_column_set_validate(&def));
        return;
    }

    // чистим
    ASSERT_TRUE(unlink(testdb_name) == 0 || errno == ENOENT);
    ASSERT_TRUE(unlink(testdb_name_lck) == 0 || errno == ENOENT);

// пытаемся обойтись меньшей базой, но для строк потребуется больше места
#ifdef FPTA_QUICK_INDEX_UT
    const unsigned megabytes = 1;
#else
    unsigned megabytes = 16;
    if (type > fptu_128)
        megabytes = 20;
    if (type > fptu_256)
        megabytes = 32;
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
    fpta_name table, col_pk, col_order, col_dup_id, col_t1ha;
    EXPECT_EQ(FPTA_OK, fpta_name_init(&table, "table", fpta_table));
    EXPECT_EQ(FPTA_OK,
              fpta_name_init(&col_pk, pk_col_name.c_str(), fpta_column));
    EXPECT_EQ(FPTA_OK, fpta_name_init(&col_order, "order", fpta_column));
    EXPECT_EQ(FPTA_OK, fpta_name_init(&col_dup_id, "dup_id", fpta_column));
    EXPECT_EQ(FPTA_OK, fpta_name_init(&col_t1ha, "t1ha", fpta_column));

    //------------------------------------------------------------------------

    // начинаем транзакцию записи
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_write, &txn));
    ASSERT_NE(nullptr, txn);
    txn_guard.reset(txn);

    // связываем идентификаторы с ранее созданной схемой
    ASSERT_EQ(FPTA_OK, fpta_name_refresh(txn, &table, &col_pk));
    ASSERT_EQ(FPTA_OK, fpta_name_refresh(txn, &table, &col_order));
    ASSERT_EQ(FPTA_OK, fpta_name_refresh(txn, &table, &col_dup_id));
    ASSERT_EQ(FPTA_OK, fpta_name_refresh(txn, &table, &col_t1ha));

    fptu_rw *row = fptu_alloc(4, fpta_max_keylen * 2 + 4 + 4);
    ASSERT_NE(nullptr, row);
    ASSERT_STREQ(nullptr, fptu_check(row));

    unsigned n = 0;
    for (int order = keygen<index, type>::order_from;
         order <= keygen<index, type>::order_to; ++order) {
        SCOPED_TRACE("order " + std::to_string(order));
        fpta_value value_pk = keygen<index, type>::make(order);
        if (value_pk.type == fpta_end)
            break;
        if (value_pk.type == fpta_begin)
            continue;

        // теперь формируем кортеж
        ASSERT_EQ(FPTU_OK, fptu_clear(row));
        ASSERT_STREQ(nullptr, fptu_check(row));
        ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_order,
                                              fpta_value_sint(order)));
        ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_pk, value_pk));
        // t1ha как "checksum" для order
        ASSERT_EQ(FPTA_OK,
                  fpta_upsert_column(row, &col_t1ha,
                                     order_checksum(order, type, index)));

        // пытаемся обновить несуществующую запись
        ASSERT_EQ(MDB_NOTFOUND,
                  fpta_update_row(txn, &table, fptu_take_noshrink(row)));

        if (fpta_index_is_unique(index)) {
            // вставляем
            ASSERT_EQ(FPTA_OK,
                      fpta_insert_row(txn, &table, fptu_take_noshrink(row)));
            n++;
            // проверяем что полный дубликат не вставляется
            ASSERT_EQ(MDB_KEYEXIST,
                      fpta_insert_row(txn, &table, fptu_take_noshrink(row)));

            // обновляем dup_id и проверям что дубликат по ключу не проходит
            ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_dup_id,
                                                  fpta_value_uint(1)));
            ASSERT_EQ(MDB_KEYEXIST,
                      fpta_insert_row(txn, &table, fptu_take_noshrink(row)));

            // проверяем что upsert и update работают,
            // сначала upsert с текущим dup_id = 1
            ASSERT_EQ(FPTA_OK,
                      fpta_upsert_row(txn, &table, fptu_take_noshrink(row)));
            // теперь update c dup_id = 42, должен остаться только этот
            // вариант
            ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_dup_id,
                                                  fpta_value_uint(42)));
            ASSERT_EQ(FPTA_OK,
                      fpta_update_row(txn, &table, fptu_take_noshrink(row)));
        } else {
            // вставляем c dup_id = 0
            ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_dup_id,
                                                  fpta_value_uint(0)));
            ASSERT_EQ(FPTA_OK,
                      fpta_insert_row(txn, &table, fptu_take_noshrink(row)));
            n++;
            // проверяем что полный дубликат не вставляется
            ASSERT_EQ(MDB_KEYEXIST,
                      fpta_insert_row(txn, &table, fptu_take_noshrink(row)));

            // обновляем dup_id и вставляем дубль по ключу
            ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_dup_id,
                                                  fpta_value_uint(1)));
            ASSERT_EQ(FPTA_OK,
                      fpta_insert_row(txn, &table, fptu_take_noshrink(row)));
            n++;

            // проверяем что upsert не вставляет дубликат,
            // сначала удаляем dup_id, чтобы строка не была полным дублем
            ASSERT_EQ(1, fptu_erase(row, col_dup_id.column.num, fptu_any));
            ASSERT_EQ(MDB_KEYEXIST,
                      fpta_upsert_row(txn, &table, fptu_take_noshrink(row)));
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
    EXPECT_EQ(FPTA_OK,
              fpta_cursor_open(txn, &table, &col_pk, fpta_value_begin(),
                               fpta_value_end(), nullptr,
                               fpta_index_is_ordered(index)
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

    int order = keygen<index, type>::order_from;
    for (unsigned i = 0; i < n;) {
        SCOPED_TRACE(std::to_string(i) + " of " + std::to_string(n) +
                     ", order " + std::to_string(order));
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
        if (fpta_index_is_ordered(index))
            ASSERT_EQ(order, tuple_order);

        auto tuple_checksum =
            fptu_get_uint(tuple, col_t1ha.column.num, &error);
        ASSERT_EQ(FPTU_OK, error);
        auto checksum = order_checksum(tuple_order, type, index).uint;
        ASSERT_EQ(checksum, tuple_checksum);

        auto tuple_dup_id =
            fptu_get_uint(tuple, col_dup_id.column.num, &error);
        ASSERT_EQ(FPTU_OK, error);
        if (fpta_index_is_unique(index))
            ASSERT_EQ(42, tuple_dup_id);
        else
            ASSERT_EQ(i & 1, tuple_dup_id);

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
    ASSERT_TRUE(unlink(testdb_name) == 0);
    ASSERT_TRUE(unlink(testdb_name_lck) == 0);
}

template <typename TypeParam> class PrimaryIndex : public ::testing::Test
{
};
TYPED_TEST_CASE_P(PrimaryIndex);

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

TYPED_TEST_CASE(PrimaryIndex, ColumnTypes);

TYPED_TEST(PrimaryIndex, obverse_unique)
{
    TestPrimary<TypeParam::type, fpta_primary_unique>();
}

TYPED_TEST(PrimaryIndex, unordered_unique)
{
    TestPrimary<TypeParam::type, fpta_primary_unique_unordered>();
}

TYPED_TEST(PrimaryIndex, reverse_unique)
{
    TestPrimary<TypeParam::type, fpta_primary_unique_reversed>();
}

TYPED_TEST(PrimaryIndex, obverse_withdups)
{
    TestPrimary<TypeParam::type, fpta_primary_withdups>();
}

TYPED_TEST(PrimaryIndex, unordered_withdups)
{
    TestPrimary<TypeParam::type, fpta_primary_withdups_unordered>();
}

TYPED_TEST(PrimaryIndex, reverse_withdups)
{
    TestPrimary<TypeParam::type, fpta_primary_withdups_reversed>();
}

//----------------------------------------------------------------------------

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

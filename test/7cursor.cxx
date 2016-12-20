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
#include <tuple>
#include <unordered_map>

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

static const char testdb_name[] = "ut_cursor.fpta";
static const char testdb_name_lck[] = "ut_cursor.fpta-lock";

TEST(Cursor, Invalid)
{
    // TODO
}

/* карманный бильярд для закатки keygen-шаблонов в один не-шаблонный */
class any_keygen
{
    typedef fpta_value (*maker_type)(int order);
    const maker_type maker;

    struct init_tier {
        fptu_type type;
        fpta_index_type index;
        int order_from;
        int order_to;
        maker_type maker;

        static fpta_value end(int order)
        {
            (void)order;
            return fpta_value_end();
        }

        void stub(fptu_type type, fpta_index_type index)
        {
            this->type = type;
            this->index = index;
            order_from = order_to = 0;
            maker = end;
        }

        template <fpta_index_type index, fptu_type type> void glue()
        {
            this->type = type;
            this->index = index;
            order_from = keygen<index, type>::order_from;
            order_to = keygen<index, type>::order_to;
            maker = keygen<index, type>::make;
        }

        template <fpta_index_type index> void unroll(fptu_type type)
        {
            switch (type) {
            default /* arrays */:
            case fptu_null:
                assert(false && "wrong type");
                return stub(type, index);
            case fptu_uint16:
                return glue<index, fptu_uint16>();
            case fptu_int32:
                return glue<index, fptu_int32>();
            case fptu_uint32:
                return glue<index, fptu_uint32>();
            case fptu_fp32:
                return glue<index, fptu_fp32>();
            case fptu_int64:
                return glue<index, fptu_int64>();
            case fptu_uint64:
                return glue<index, fptu_uint64>();
            case fptu_fp64:
                return glue<index, fptu_fp64>();
            case fptu_96:
                return glue<index, fptu_96>();
            case fptu_128:
                return glue<index, fptu_128>();
            case fptu_160:
                return glue<index, fptu_160>();
            case fptu_192:
                return glue<index, fptu_192>();
            case fptu_256:
                return glue<index, fptu_256>();
            case fptu_cstr:
                return glue<index, fptu_cstr>();
            case fptu_opaque:
                return glue<index, fptu_opaque>();
            case fptu_nested:
                return glue<index, fptu_nested>();
            }
        }

        init_tier(fptu_type type, fpta_index_type index)
        {
            switch (index) {
            default:
                assert(false && "wrong index");
                stub(type, index);
                break;
            case fpta_primary_withdups:
                unroll<fpta_primary_withdups>(type);
                break;
            case fpta_primary_unique:
                unroll<fpta_primary_unique>(type);
                break;
            case fpta_primary_withdups_unordered:
                unroll<fpta_primary_withdups_unordered>(type);
                break;
            case fpta_primary_unique_unordered:
                unroll<fpta_primary_unique_unordered>(type);
                break;
            case fpta_primary_withdups_reversed:
                unroll<fpta_primary_withdups_reversed>(type);
                break;
            case fpta_primary_unique_reversed:
                unroll<fpta_primary_unique_reversed>(type);
                break;
            case fpta_secondary_withdups:
                unroll<fpta_secondary_withdups>(type);
                break;
            case fpta_secondary_unique:
                unroll<fpta_secondary_unique>(type);
                break;
            case fpta_secondary_withdups_unordered:
                unroll<fpta_secondary_withdups_unordered>(type);
                break;
            case fpta_secondary_unique_unordered:
                unroll<fpta_primary_withdups>(type);
                break;
            case fpta_secondary_withdups_reversed:
                unroll<fpta_secondary_withdups_reversed>(type);
                break;
            case fpta_secondary_unique_reversed:
                unroll<fpta_secondary_unique_reversed>(type);
                break;
            }
        }
    };

    any_keygen(const init_tier &init, fptu_type type, fpta_index_type index)
        : maker(init.maker), order_from(init.order_from),
          order_to(init.order_to)
    {
        // страховка от опечаток в параметрах при инстанцировании шаблонов.
        assert(init.type == type);
        assert(init.index == index);
    }

  public:
    const int order_from;
    const int order_to;

    any_keygen(fptu_type type, fpta_index_type index)
        : any_keygen(init_tier(type, index), type, index)
    {
    }

    fpta_value make(int order) const { return maker(order); }
};

class CursorPrimary
    : public ::testing::TestWithParam<
          std::tuple<fptu_type, fpta_index_type, fpta_cursor_options>>
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
    int order_from, order_to;
    unsigned n;
    std::unordered_map<int, int> reorder;

    void CheckPosition(int linear, int dup)
    {
        if (linear < 0)
            linear += reorder.size();

        SCOPED_TRACE("linear-order " + std::to_string(linear) + " [0..." +
                     std::to_string(reorder.size() - 1) + "], dup " +
                     std::to_string(dup));

        ASSERT_EQ(1, reorder.count(linear));
        const auto expected_order = reorder[linear];

        SCOPED_TRACE("logical-order " + std::to_string(expected_order) +
                     " [" + std::to_string(order_from) + "..." +
                     std::to_string(order_to) + "]");

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
            ASSERT_EQ(dup, tuple_dup_id);
    }

    void Fill()
    {
        fptu_rw *row = fptu_alloc(4, fpta_max_keylen * 2 + 4 + 4);
        ASSERT_NE(nullptr, row);
        ASSERT_STREQ(nullptr, fptu_check(row));
        fpta_txn *const txn = txn_guard.get();

        any_keygen keygen(type, index);
        order_from = keygen.order_from;
        order_to = keygen.order_to;
        n = 0;
        for (int order = order_from; order <= order_to; ++order) {
            SCOPED_TRACE("order " + std::to_string(order));
            fpta_value value_pk = keygen.make(order);
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
                // вставляем одну запись с dup_id = 42
                ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_dup_id,
                                                      fpta_value_uint(42)));
                ASSERT_EQ(FPTA_OK, fpta_insert_row(txn, &table,
                                                   fptu_take_noshrink(row)));
                ++n;
            } else {
                for (unsigned dup_id = 0; dup_id < 5; ++dup_id) {
                    ASSERT_EQ(FPTA_OK,
                              fpta_upsert_column(row, &col_dup_id,
                                                 fpta_value_uint(dup_id)));
                    ASSERT_EQ(FPTA_OK,
                              fpta_insert_row(txn, &table,
                                              fptu_take_noshrink(row)));
                    ++n;
                }
            }
        }

        // разрушаем кортеж
        ASSERT_STREQ(nullptr, fptu_check(row));
        free(row);
    }

    virtual void SetUp()
    {
        type = std::get<0>(GetParam());
        index = std::get<1>(GetParam());
        ordering = std::get<2>(GetParam());
        valid_index_ops = is_valid4pk(type, index);
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
            EXPECT_EQ(FPTA_EINVAL, fpta_column_describe(pk_col_name.c_str(),
                                                        type, index, &def));
            EXPECT_EQ(FPTA_OK, fpta_column_describe("order", fptu_int32,
                                                    fpta_index_none, &def));
            ASSERT_NE(FPTA_OK, fpta_column_set_validate(&def));
            return;
        }

        EXPECT_EQ(FPTA_OK, fpta_column_describe(pk_col_name.c_str(), type,
                                                index, &def));
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
                      fpta_cursor_open(txn, &table, &col_pk,
                                       fpta_value_begin(), fpta_value_end(),
                                       nullptr, ordering, &cursor));
            ASSERT_NE(nullptr, cursor);
            cursor_guard.reset(cursor);
        } else {
            EXPECT_EQ(FPTA_EINVAL,
                      fpta_cursor_open(txn, &table, &col_pk,
                                       fpta_value_begin(), fpta_value_end(),
                                       nullptr, ordering, &cursor));
            cursor_guard.reset(cursor);
            ASSERT_EQ(nullptr, cursor);
            return;
        }

        // формируем линейную карту, чтобы проще проверять переходы
        reorder.clear();
        reorder.reserve(order_to - order_from + 1);
        for (int linear = 0; fpta_cursor_eof(cursor) == FPTA_OK; ++linear) {
            fptu_ro tuple;
            EXPECT_EQ(FPTA_OK, fpta_cursor_get(cursor_guard.get(), &tuple));
            ASSERT_STREQ(nullptr, fptu_check_ro(tuple));

            int error;
            auto tuple_order =
                fptu_get_sint(tuple, col_order.column.num, &error);
            ASSERT_EQ(FPTU_OK, error);
            auto tuple_checksum =
                fptu_get_uint(tuple, col_t1ha.column.num, &error);
            ASSERT_EQ(FPTU_OK, error);

            auto checksum = order_checksum(tuple_order, type, index).uint;
            ASSERT_EQ(checksum, tuple_checksum);

            reorder[linear] = tuple_order;

            error = fpta_cursor_move(cursor, fpta_key_next);
            if (error == FPTA_NODATA)
                break;
            ASSERT_EQ(FPTA_SUCCESS, error);
        }

        ASSERT_EQ(order_to - order_from + 1, reorder.size());

        if (fpta_cursor_is_ordered(ordering)) {
            std::map<int, int> probe;
            for (auto pair : reorder)
                probe[pair.first] = pair.second;
            ASSERT_EQ(probe.size(), reorder.size());
            ASSERT_TRUE(is_properly_ordered(
                probe, fpta_cursor_is_descending(ordering)));
        }
    }

    virtual void TearDown()
    {
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
            ASSERT_EQ(FPTA_OK,
                      fpta_transaction_end(txn_guard.release(), true));
        if (db_quard) {
            // закрываем и удаляем базу
            ASSERT_EQ(FPTA_SUCCESS, fpta_db_close(db_quard.release()));
            ASSERT_TRUE(unlink(testdb_name) == 0);
            ASSERT_TRUE(unlink(testdb_name_lck) == 0);
        }
    }
};

TEST_P(CursorPrimary, basicMoves)
{
    SCOPED_TRACE("type " + std::to_string(type) + ", index " +
                 std::to_string(index) +
                 (valid_index_ops ? ", (valid case)" : ", (invalid case)"));
    if (!valid_index_ops || !valid_cursor_ops)
        return;

    ASSERT_GT(n, 5);
    fpta_cursor *const cursor = cursor_guard.get();
    ASSERT_NE(nullptr, cursor);

    // переходим туда-сюда и к первой записи
    ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_first));
    ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 0));
    ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_last));
    ASSERT_NO_FATAL_FAILURE(CheckPosition(reorder.size() - 1, 0));
    ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_first));
    ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 0));

    // пробуем уйти дальше последней
    ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_last));
    ASSERT_NO_FATAL_FAILURE(CheckPosition(reorder.size() - 1, 0));
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

    // пробуем выйти за первую
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
    ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 0));
    ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_key_prev));
    ASSERT_NO_FATAL_FAILURE(CheckPosition(-2, 0));
    ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_key_next));
    ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 0));
    ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_key_next));
#if FPTA_ENABLE_RETURN_INTO_RANGE
    ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
#else
    ASSERT_EQ(FPTA_ECURSOR, fpta_cursor_move(cursor, fpta_prev));
    ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_last));
#endif
    ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 0));
    ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_key_prev));
    ASSERT_NO_FATAL_FAILURE(CheckPosition(-2, 0));
    ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_key_prev));
    ASSERT_NO_FATAL_FAILURE(CheckPosition(-3, 0));
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
    ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 0));
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
    ASSERT_NO_FATAL_FAILURE(CheckPosition(1, 0));
    ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_key_prev));
    ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 0));
    ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_key_prev));
}

INSTANTIATE_TEST_CASE_P(
    Combine, CursorPrimary,
    ::testing::Combine(
        ::testing::Values(fptu_null, fptu_uint16, fptu_int32, fptu_uint32,
                          fptu_fp32, fptu_int64, fptu_uint64, fptu_fp64,
                          fptu_96, fptu_128, fptu_160, fptu_192, fptu_256,
                          fptu_cstr, fptu_opaque
                          /*, fptu_nested, fptu_farray */),
        ::testing::Values(fpta_primary_unique, fpta_primary_unique_reversed,
                          fpta_primary_withdups,
                          fpta_primary_withdups_reversed,
                          fpta_primary_unique_unordered,
                          fpta_primary_withdups_unordered),
        ::testing::Values(fpta_unsorted, fpta_ascending, fpta_descending)));

//----------------------------------------------------------------------------

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

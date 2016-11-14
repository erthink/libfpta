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

static const char testdb_name[] = "ut_schema.fpta";
static const char testdb_name_lck[] = "ut_schema.fpta-lock";

TEST(Schema, Trivia)
{
    fpta_column_set def;
    fpta_column_set_init(&def);
    EXPECT_FALSE(fpta_column_set_validate(&def));

    EXPECT_EQ(FPTA_EINVAL,
              fpta_column_describe("", fptu_cstr, fpta_primary_unique, &def));
    EXPECT_EQ(FPTA_EINVAL,
              fpta_column_describe("column_a", fptu_cstr, fpta_primary_unique,
                                   nullptr));

    EXPECT_EQ(FPTA_EINVAL,
              fpta_column_describe("column_a", fptu_uint64,
                                   fpta_primary_unique_reversed, &def));
    EXPECT_EQ(FPTA_EINVAL, fpta_column_describe("column_a", fptu_null,
                                                fpta_primary_unique, &def));

    /* Валидны все комбинации, в которых установлен хотя-бы один из флажков
     * fpta_index_fordered или fpta_index_fobverse. Другим словами,
     * не может быть unordered индекса со сравнением ключей
     * в обратном порядке. Однако, также допустим fpta_index_none.
     *
     * Поэтому внутри диапазона остаются только две недопустимые комбинации,
     * которые и проверяем. */
    EXPECT_EQ(FPTA_EINVAL, fpta_column_describe("column_a", fptu_cstr,
                                                fpta_index_funique, &def));
    EXPECT_EQ(FPTA_EINVAL,
              fpta_column_describe("column_a", fptu_cstr,
                                   (fpta_index_type)(fpta_index_fsecondary |
                                                     fpta_index_funique),
                                   &def));
    EXPECT_EQ(FPTA_EINVAL, fpta_column_describe("column_a", fptu_cstr,
                                                (fpta_index_type)-1, &def));
    EXPECT_EQ(FPTA_EINVAL,
              fpta_column_describe(
                  "column_a", fptu_cstr,
                  (fpta_index_type)(fpta_index_funique + fpta_index_fordered +
                                    fpta_index_fobverse +
                                    fpta_index_fsecondary + 1),
                  &def));

    EXPECT_EQ(FPTA_OK, fpta_column_describe("column_a", fptu_cstr,
                                            fpta_primary_unique, &def));
    EXPECT_TRUE(fpta_column_set_validate(&def));

    EXPECT_EQ(EEXIST, fpta_column_describe("column_b", fptu_cstr,
                                           fpta_primary_unique, &def));
    EXPECT_EQ(EEXIST, fpta_column_describe("column_a", fptu_cstr,
                                           fpta_secondary, &def));
    EXPECT_TRUE(fpta_column_set_validate(&def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe("column_b", fptu_cstr,
                                            fpta_secondary, &def));
    EXPECT_TRUE(fpta_column_set_validate(&def));

    EXPECT_EQ(EEXIST, fpta_column_describe("column_b", fptu_fp64,
                                           fpta_secondary, &def));
    EXPECT_TRUE(fpta_column_set_validate(&def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe("column_c", fptu_uint16,
                                            fpta_secondary, &def));
    EXPECT_TRUE(fpta_column_set_validate(&def));
}

TEST(Schema, Base)
{
    ASSERT_TRUE(unlink(testdb_name) == 0 || errno == ENOENT);
    ASSERT_TRUE(unlink(testdb_name_lck) == 0 || errno == ENOENT);

    fpta_db *db = nullptr;
    EXPECT_EQ(FPTA_SUCCESS,
              fpta_db_open(testdb_name, fpta_async, 0644, 1, false, &db));
    ASSERT_NE(nullptr, db);

    fpta_column_set def;
    fpta_column_set_init(&def);

    EXPECT_EQ(FPTA_OK, fpta_column_describe("pk_str_uniq", fptu_cstr,
                                            fpta_primary_unique, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe("first_uint", fptu_uint64,
                                            fpta_index_none, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe("second_fp", fptu_fp64,
                                            fpta_index_none, &def));
    EXPECT_TRUE(fpta_column_set_validate(&def));

    //------------------------------------------------------------------------
    fpta_txn *txn = (fpta_txn *)&txn;
    EXPECT_EQ(FPTA_EINVAL, fpta_transaction_begin(db, fpta_read, nullptr));
    EXPECT_EQ(FPTA_EINVAL, fpta_transaction_begin(db, (fpta_level)0, &txn));
    EXPECT_EQ(nullptr, txn);
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
    ASSERT_NE(nullptr, txn);

    EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "table_1", &def));
    EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
    txn = nullptr;

    //------------------------------------------------------------------------
    fpta_name table, col_pk, col_a, col_b;
    EXPECT_EQ(FPTA_OK, fpta_name_init(&table, "tAbLe_1", fpta_table));
    EXPECT_EQ(FPTA_OK, fpta_name_init(&table, "table_1", fpta_table));
    EXPECT_EQ(FPTA_OK, fpta_name_init(&col_pk, "pk_str_uniq", fpta_column));
    EXPECT_EQ(FPTA_OK, fpta_name_init(&col_a, "First_Uint", fpta_column));
    EXPECT_EQ(FPTA_OK, fpta_name_init(&col_b, "second_FP", fpta_column));

    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_read, &txn));
    ASSERT_NE(nullptr, txn);

    EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &table, &col_pk));
    EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &table, &col_a));
    EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &table, &col_b));

    EXPECT_EQ(fptu_cstr, fpta_shove2type(col_pk.internal));
    EXPECT_EQ(fpta_primary_unique, fpta_shove2index(col_pk.internal));
    EXPECT_EQ(fptu_cstr, col_pk.column.type);
    EXPECT_EQ(FPTA_OK, col_pk.column.order);

    EXPECT_EQ(fptu_uint64, fpta_shove2type(col_a.internal));
    EXPECT_EQ(fpta_index_none, fpta_shove2index(col_a.internal));
    EXPECT_EQ(fptu_uint64, col_a.column.type);
    EXPECT_EQ(1, col_a.column.order);

    EXPECT_EQ(fptu_fp64, fpta_shove2type(col_b.internal));
    EXPECT_EQ(fpta_index_none, fpta_shove2index(col_b.internal));
    EXPECT_EQ(fptu_fp64, col_b.column.type);
    EXPECT_EQ(2, col_b.column.order);

    EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
    txn = nullptr;

    fpta_name_destroy(&table);
    fpta_name_destroy(&col_pk);

    //------------------------------------------------------------------------
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
    ASSERT_NE(nullptr, txn);
    EXPECT_EQ(FPTA_OK, fpta_table_drop(txn, "Table_1"));
    EXPECT_EQ(MDB_NOTFOUND, fpta_table_drop(txn, "table_xyz"));
    EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));

    //------------------------------------------------------------------------
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

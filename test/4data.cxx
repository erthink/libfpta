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

static const char testdb_name[] = "ut_data.fpta";
static const char testdb_name_lck[] = "ut_data.fpta-lock";

static const uint8_t pattern[256] = {
    /* clang-format off */
    23,  8,   6,   151, 184, 195, 17,  16,  140, 191, 131, 156, 61,  239, 127,
    181, 94,  176, 27,  81,  235, 141, 69,  47,  170, 74,  168, 88,  56,  193,
    68,  209, 104, 143, 52,  53,  46,  115, 158, 100, 243, 213, 247, 34,  62,
    238, 203, 232, 92,  49,  54,  42,  245, 171, 227, 123, 24,  186, 63,  112,
    200, 125, 93,  55,  84,  105, 89,  215, 161, 211, 154, 86,  39,  145, 77,
    190, 147, 136, 108, 132, 107, 172, 229, 83,  187, 226, 160, 155, 242, 133,
    166, 70,  71,  11,  189, 50,  234, 218, 30,  0,   134, 32,  152, 90,  19,
    135, 183, 254, 5,   198, 13,  216, 73,  219, 173, 255, 121, 79,  137, 150,
    12,  162, 41,  206, 217, 231, 120, 59,  128, 101, 51,  201, 253, 35,  194,
    177, 85,  188, 146, 222, 148, 10,  7,   241, 57,  199, 43,  106, 240, 124,
    237, 220, 230, 197, 76,  116, 153, 205, 221, 28,  2,   31,  233, 58,  60,
    159, 228, 109, 20,  66,  214, 111, 15,  18,  44,  208, 72,  249, 210, 113,
    212, 165, 1,   225, 174, 164, 204, 45,  130, 82,  80,  99,  138, 48,  167,
    78,  14,  149, 207, 103, 178, 223, 25,  163, 118, 139, 122, 37,  119, 182,
    97,  4,   236, 96,  64,  196, 75,  29,  95,  252, 33,  185, 87,  110, 202,
    224, 3,   250, 98,  169, 102, 38,  142, 91,  117, 180, 175, 246, 9,   129,
    114, 244, 67,  157, 21,  144, 126, 40,  179, 36,  192, 248, 22,  65,  251,
    26
    /* clang-format on */
};

TEST(Data, Field2Value) {
  /* Проверка конвертации полей кортежа в соответствующие значения fpta_value.
   *
   * Сценарий:
   *  1. Создаем и заполняем кортеж полями всяческих типов.
   *  2. Читаем каждое добавленное поле и конвертируем в fpta_value.
   *  3. Сравниваем значения в форме fpta_value с исходно добавленными.
   *
   * Тест НЕ перебирает комбинации и диапазоны значений. Некий относительно
   * полный перебор происходит при тестировании индексов и курсоров.
   */

  // формируем кортеж с полями всяческих типов
  fptu_rw *pt = fptu_alloc(15, 39 * 4 + sizeof(pattern));
  ASSERT_NE(nullptr, pt);
  ASSERT_STREQ(nullptr, fptu_check(pt));

  EXPECT_EQ(FPTU_OK, fptu_upsert_uint16(pt, 0, 0x8001));
  EXPECT_EQ(FPTU_OK, fptu_upsert_uint32(pt, 1, 1354824703));
  EXPECT_EQ(FPTU_OK, fptu_upsert_int32(pt, 42, -8782211));
  EXPECT_EQ(FPTU_OK, fptu_upsert_uint64(pt, 111, 15047220096467327));
  EXPECT_EQ(FPTU_OK, fptu_upsert_int64(pt, 3, -60585001468255361));
  EXPECT_EQ(FPTU_OK, fptu_upsert_fp64(pt, 2, 3.14159265358979323846));
  EXPECT_EQ(FPTU_OK, fptu_upsert_fp32(pt, 4, 2.7182818284590452354));
  EXPECT_EQ(FPTU_OK, fptu_upsert_null(pt, 0));

  static const uint8_t *_96 = pattern;
  static const uint8_t *_128 = _96 + 12;
  static const uint8_t *_160 = _128 + 16;
  static const uint8_t *_256 = _160 + 24;
  ASSERT_LT(32, pattern + sizeof(pattern) - _256);
  ASSERT_LT(fpta_max_keylen, sizeof(pattern) / 2);
  const fptu_time now = fptu_now_coarse();

  EXPECT_EQ(FPTU_OK, fptu_upsert_96(pt, fptu_max_cols / 2, _96));
  EXPECT_EQ(FPTU_OK, fptu_upsert_128(pt, 257, _128));
  EXPECT_EQ(FPTU_OK, fptu_upsert_160(pt, 7, _160));
  EXPECT_EQ(FPTU_OK, fptu_upsert_datetime(pt, 8, now));
  EXPECT_EQ(FPTU_OK, fptu_upsert_256(pt, fptu_max_cols - 2, _256));
  EXPECT_EQ(FPTU_OK, fptu_upsert_cstr(pt, fptu_max_cols - 1, "abc-string"));
  EXPECT_EQ(FPTU_OK, fptu_upsert_opaque(pt, 43, pattern, sizeof(pattern)));

  ASSERT_STREQ(nullptr, fptu_check(pt));
  // теперь получаем и проверяем все значения в форме fpta_value
  fpta_value value;

  value = fpta_field2value(fptu_lookup(pt, 0, fptu_uint16));
  EXPECT_EQ(fpta_unsigned_int, value.type);
  EXPECT_EQ(0x8001, value.uint);

  value = fpta_field2value(fptu_lookup(pt, 1, fptu_uint32));
  EXPECT_EQ(fpta_unsigned_int, value.type);
  EXPECT_EQ(1354824703, value.uint);

  value = fpta_field2value(fptu_lookup(pt, 42, fptu_int32));
  EXPECT_EQ(fpta_signed_int, value.type);
  EXPECT_EQ(-8782211, value.sint);

  value = fpta_field2value(fptu_lookup(pt, 111, fptu_uint64));
  EXPECT_EQ(fpta_unsigned_int, value.type);
  EXPECT_EQ(15047220096467327, value.sint);

  value = fpta_field2value(fptu_lookup(pt, 3, fptu_int64));
  EXPECT_EQ(fpta_signed_int, value.type);
  EXPECT_EQ(-60585001468255361, value.sint);

  value = fpta_field2value(fptu_lookup(pt, 2, fptu_fp64));
  EXPECT_EQ(fpta_float_point, value.type);
  EXPECT_EQ(3.14159265358979323846, value.fp);

  value = fpta_field2value(fptu_lookup(pt, 4, fptu_fp32));
  EXPECT_EQ(fpta_float_point, value.type);
  EXPECT_EQ((float)2.7182818284590452354, value.fp);

  value = fpta_field2value(fptu_lookup(pt, fptu_max_cols / 2, fptu_96));
  EXPECT_EQ(fpta_binary, value.type);
  EXPECT_EQ(96 / 8, value.binary_length);
  EXPECT_EQ(0, memcmp(value.binary_data, _96, value.binary_length));

  value = fpta_field2value(fptu_lookup(pt, 257, fptu_128));
  EXPECT_EQ(fpta_binary, value.type);
  EXPECT_EQ(128 / 8, value.binary_length);
  EXPECT_EQ(0, memcmp(value.binary_data, _128, value.binary_length));

  value = fpta_field2value(fptu_lookup(pt, 7, fptu_160));
  EXPECT_EQ(fpta_binary, value.type);
  EXPECT_EQ(160 / 8, value.binary_length);
  EXPECT_EQ(0, memcmp(value.binary_data, _160, value.binary_length));

  value = fpta_field2value(fptu_lookup(pt, 8, fptu_datetime));
  EXPECT_EQ(fpta_datetime, value.type);
  EXPECT_EQ(now.fixedpoint, value.datetime.fixedpoint);

  value = fpta_field2value(fptu_lookup(pt, fptu_max_cols - 2, fptu_256));
  EXPECT_EQ(fpta_binary, value.type);
  EXPECT_EQ(256 / 8, value.binary_length);
  EXPECT_EQ(0, memcmp(value.binary_data, _256, value.binary_length));

  value = fpta_field2value(fptu_lookup(pt, fptu_max_cols - 1, fptu_cstr));
  EXPECT_EQ(fpta_string, value.type);
  EXPECT_STREQ("abc-string", value.str);

  value = fpta_field2value(fptu_lookup(pt, 43, fptu_opaque));
  EXPECT_EQ(fpta_binary, value.type);
  EXPECT_EQ(sizeof(pattern), value.binary_length);
  EXPECT_EQ(0, memcmp(value.binary_data, pattern, value.binary_length));

  value = fpta_field2value(fptu_lookup(pt, 0, fptu_null));
  EXPECT_EQ(fpta_null, value.type);
  EXPECT_EQ(0, value.binary_length);
  EXPECT_EQ(nullptr, value.binary_data);

  // TODO: fptu_nested

  // TODO: fptu_farray

  // разрушаем кортеж
  ASSERT_STREQ(nullptr, fptu_check(pt));
  free(pt);
}

TEST(Data, UpsertColumn) {
  /* Проверка добавления/обновления полей кортежа (значений колонок) на
   * уровне libfpta, через представление значений в fpta_value
   * и с использованием схемы таблицы (описания колонок).
   *
   * Тест не добавляет данных в таблицу, но использует схему для контроля
   * типов и преобразования fpta-значений в поля libfptu.
   *
   * Сценарий:
   *  1. Создаем временную базу с таблицей, в которой есть колонка для
         каждого fptu-типа.
   *  2. Проверяем обслуживание каждого fptu-типа, посредством
   *     добавления/обновления значений для соответствующей колонки.
   *     - При этом также проверяем обработку как неверных типов,
   *       так и недопустимых значений конвертируемых типов.
   *  3. Завершаем операции и освобождаем ресурсы.
   */

  // для проверки требуются полноценные идентификаторы колонок,
  // поэтому необходимо открыть базу, создать таблицу и т.д.
  ASSERT_TRUE(unlink(testdb_name) == 0 || errno == ENOENT);
  ASSERT_TRUE(unlink(testdb_name_lck) == 0 || errno == ENOENT);
  fpta_db *db = nullptr;
  EXPECT_EQ(FPTA_SUCCESS,
            fpta_db_open(testdb_name, fpta_async, 0644, 1, true, &db));
  ASSERT_NE(nullptr, db);

  // создаем набор колонок разных типов
  fpta_column_set def;
  fpta_column_set_init(&def);

  EXPECT_EQ(FPTA_EINVAL,
            fpta_column_describe("null", fptu_null, fpta_index_none, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("uint16", fptu_uint16, fpta_index_none, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("int32", fptu_int32, fpta_index_none, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("uint32", fptu_uint32, fpta_index_none, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("fp32", fptu_fp32, fpta_index_none, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("int64", fptu_int64, fpta_index_none, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("uint64", fptu_uint64, fpta_index_none, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("fp64", fptu_fp64, fpta_index_none, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("o96", fptu_96, fpta_index_none, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("o128", fptu_128, fpta_index_none, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("o160", fptu_160, fpta_index_none, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe("datetime", fptu_datetime,
                                          fpta_index_none, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("o256", fptu_256, fpta_index_none, &def));

  EXPECT_EQ(FPTA_OK, fpta_column_describe("string", fptu_cstr,
                                          fpta_primary_unique, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("opaque", fptu_opaque, fpta_index_none, &def));

  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("nested", fptu_nested, fpta_index_none, &def));
  EXPECT_EQ(FPTA_EINVAL,
            fpta_column_describe("farray", fptu_farray, fpta_index_none, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def));

  // создаем таблицу
  fpta_txn *txn = (fpta_txn *)&txn;
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
  ASSERT_NE(nullptr, txn);
  ASSERT_EQ(FPTA_OK, fpta_table_create(txn, "table", &def));
  ASSERT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  // инициализируем идентификаторы колонок
  fpta_name table, col_uint16, col_uint32, col_int32, col_fp32, col_int64,
      col_uint64, col_fp64, col_96, col_128, col_160, col_datetime, col_256,
      col_str, col_opaque;

  EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "table"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_uint16, "uint16"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_uint32, "uint32"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_int32, "int32"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_fp32, "fp32"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_uint64, "uint64"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_int64, "int64"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_fp64, "fp64"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_96, "o96"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_128, "o128"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_160, "o160"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_datetime, "datetime"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_256, "o256"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_str, "string"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_opaque, "opaque"));
  // TODO: fptu_nested
  // TODO: fptu_farray

  // начинаем транзакцию чтения
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_read, &txn));
  ASSERT_NE(nullptr, txn);

  // связываем идентификаторы с ранее созданной схемой
  ASSERT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &col_uint16));
  ASSERT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_uint32));
  ASSERT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_int32));
  ASSERT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_fp32));
  ASSERT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_uint64));
  ASSERT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_int64));
  ASSERT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_fp64));
  ASSERT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_96));
  ASSERT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_128));
  ASSERT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_160));
  ASSERT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_datetime));
  ASSERT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_256));
  ASSERT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_str));
  ASSERT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_opaque));

  // теперь формируем кортеж с полями всяческих типов
  fptu_rw *pt = fptu_alloc(15, 39 * 4 + sizeof(pattern));
  ASSERT_NE(nullptr, pt);
  ASSERT_STREQ(nullptr, fptu_check(pt));

  // колонка uint16
  // пробуем плохие типы
  EXPECT_EQ(FPTA_ETYPE,
            fpta_upsert_column(pt, &col_uint16, fpta_value_cstr("dummy")));
  EXPECT_EQ(FPTA_ETYPE,
            fpta_upsert_column(pt, &col_uint16, fpta_value_float(12.34)));
  EXPECT_EQ(FPTA_ETYPE,
            fpta_upsert_column(pt, &col_uint16, fpta_value_float(-12.34)));
  EXPECT_EQ(FPTA_ETYPE,
            fpta_upsert_column(pt, &col_uint16,
                               fpta_value_datetime(fptu_now_coarse())));
  // теперь плохие значения
  EXPECT_EQ(FPTA_EVALUE, fpta_upsert_column(pt, &col_uint16,
                                            fpta_value_uint(UINT16_MAX + 1l)));
  EXPECT_EQ(FPTA_EVALUE,
            fpta_upsert_column(pt, &col_uint16, fpta_value_sint(-1)));
  // теперь подходящие значения, последнее должно остаться в силе
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_uint16, fpta_value_sint(0)));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_uint16, fpta_value_sint(UINT16_MAX)));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_uint16, fpta_value_uint(0x8001)));

  // колонка uint32
  // пробуем плохие типы
  EXPECT_EQ(FPTA_ETYPE,
            fpta_upsert_column(pt, &col_uint32, fpta_value_cstr("dummy")));
  EXPECT_EQ(FPTA_ETYPE,
            fpta_upsert_column(pt, &col_uint32, fpta_value_float(12.34)));
  EXPECT_EQ(FPTA_ETYPE,
            fpta_upsert_column(pt, &col_uint32, fpta_value_float(-12.34)));
  EXPECT_EQ(FPTA_ETYPE,
            fpta_upsert_column(pt, &col_uint32,
                               fpta_value_datetime(fptu_now_coarse())));
  // теперь плохие значения
  EXPECT_EQ(FPTA_EVALUE, fpta_upsert_column(pt, &col_uint32,
                                            fpta_value_uint(UINT32_MAX + 1l)));
  EXPECT_EQ(FPTA_EVALUE,
            fpta_upsert_column(pt, &col_uint32, fpta_value_sint(-1)));
  // теперь подходящие значения, последнее должно остаться в силе
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_uint32, fpta_value_sint(0)));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_uint32, fpta_value_sint(UINT32_MAX)));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_uint32, fpta_value_uint(1354824703)));

  // колонка int32
  // пробуем плохие типы
  EXPECT_EQ(FPTA_ETYPE,
            fpta_upsert_column(pt, &col_int32, fpta_value_cstr("dummy")));
  EXPECT_EQ(FPTA_ETYPE,
            fpta_upsert_column(pt, &col_int32, fpta_value_float(12.34)));
  EXPECT_EQ(FPTA_ETYPE,
            fpta_upsert_column(pt, &col_int32, fpta_value_float(-12.34)));
  EXPECT_EQ(FPTA_ETYPE,
            fpta_upsert_column(pt, &col_int32,
                               fpta_value_datetime(fptu_now_coarse())));
  // теперь плохие значения
  EXPECT_EQ(FPTA_EVALUE, fpta_upsert_column(pt, &col_int32,
                                            fpta_value_uint(UINT32_MAX - 1l)));
  EXPECT_EQ(FPTA_EVALUE, fpta_upsert_column(pt, &col_int32,
                                            fpta_value_uint(UINT32_MAX + 1l)));
  EXPECT_EQ(FPTA_EVALUE, fpta_upsert_column(pt, &col_int32,
                                            fpta_value_uint(INT32_MAX + 1l)));
  EXPECT_EQ(FPTA_EVALUE, fpta_upsert_column(pt, &col_int32,
                                            fpta_value_sint(UINT32_MAX - 1l)));
  EXPECT_EQ(FPTA_EVALUE, fpta_upsert_column(pt, &col_int32,
                                            fpta_value_sint(UINT32_MAX + 1l)));
  EXPECT_EQ(FPTA_EVALUE, fpta_upsert_column(pt, &col_int32,
                                            fpta_value_sint(INT32_MIN - 1l)));
  // теперь подходящие значения, последнее должно остаться в силе
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_int32, fpta_value_sint(INT32_MIN)));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_int32, fpta_value_sint(INT32_MIN + 1)));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_int32, fpta_value_sint(INT32_MAX)));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_int32,
                                        fpta_value_sint(INT32_MAX - 1l)));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_int32, fpta_value_sint(-8782211)));

  // колонка uint64
  // пробуем плохие типы
  EXPECT_EQ(FPTA_ETYPE,
            fpta_upsert_column(pt, &col_uint64, fpta_value_cstr("dummy")));
  EXPECT_EQ(FPTA_ETYPE,
            fpta_upsert_column(pt, &col_uint64, fpta_value_float(12.34)));
  EXPECT_EQ(FPTA_ETYPE,
            fpta_upsert_column(pt, &col_uint64, fpta_value_float(-12.34)));
  EXPECT_EQ(FPTA_ETYPE,
            fpta_upsert_column(pt, &col_uint64,
                               fpta_value_datetime(fptu_now_coarse())));
  // теперь плохие значения
  EXPECT_EQ(FPTA_EVALUE,
            fpta_upsert_column(pt, &col_uint64, fpta_value_sint(-1l)));
  EXPECT_EQ(FPTA_EVALUE,
            fpta_upsert_column(pt, &col_uint64, fpta_value_sint(INT32_MIN)));
  EXPECT_EQ(FPTA_EVALUE,
            fpta_upsert_column(pt, &col_uint64, fpta_value_sint(INT64_MIN)));
  // теперь подходящие значения, последнее должно остаться в силе
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_uint64, fpta_value_sint(0)));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_uint64, fpta_value_sint(INT64_MAX)));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_uint64, fpta_value_uint(UINT64_MAX)));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_uint64,
                                        fpta_value_uint(15047220096467327)));

  // колонка int64
  // пробуем плохие типы
  EXPECT_EQ(FPTA_ETYPE,
            fpta_upsert_column(pt, &col_int64, fpta_value_cstr("dummy")));
  EXPECT_EQ(FPTA_ETYPE,
            fpta_upsert_column(pt, &col_int64, fpta_value_float(12.34)));
  EXPECT_EQ(FPTA_ETYPE,
            fpta_upsert_column(pt, &col_int64, fpta_value_float(-12.34)));
  EXPECT_EQ(FPTA_ETYPE,
            fpta_upsert_column(pt, &col_int64,
                               fpta_value_datetime(fptu_now_coarse())));
  // теперь плохие значения
  EXPECT_EQ(FPTA_EVALUE, fpta_upsert_column(pt, &col_int64,
                                            fpta_value_uint(INT64_MAX + 1ull)));
  EXPECT_EQ(FPTA_EVALUE,
            fpta_upsert_column(pt, &col_int64, fpta_value_uint(UINT64_MAX)));
  // теперь подходящие значения, последнее должно остаться в силе
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_int64, fpta_value_sint(0)));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_int64, fpta_value_sint(INT64_MIN)));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_int64, fpta_value_sint(INT64_MIN + 1)));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_int64, fpta_value_sint(INT64_MAX)));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_int64, fpta_value_sint(INT64_MAX - 1)));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_int64,
                                        fpta_value_sint(-60585001468255361)));

  // колонка fp64
  // пробуем плохие типы
  EXPECT_EQ(FPTA_ETYPE,
            fpta_upsert_column(pt, &col_fp64, fpta_value_cstr("dummy")));
  EXPECT_EQ(FPTA_ETYPE, fpta_upsert_column(pt, &col_fp64, fpta_value_sint(0)));
  EXPECT_EQ(FPTA_ETYPE,
            fpta_upsert_column(pt, &col_fp64,
                               fpta_value_datetime(fptu_now_coarse())));
  // теперь плохие значения
  EXPECT_EQ(FPTA_EVALUE,
            fpta_upsert_column(pt, &col_fp64, fpta_value_float(std::nan(""))));
  // теперь подходящие значения, последнее должно остаться в силе
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_fp64, fpta_value_float(HUGE_VAL)));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_fp64, fpta_value_float(DBL_MAX)));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_fp64, fpta_value_float(-HUGE_VAL)));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_fp64, fpta_value_float(-DBL_MAX)));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_fp64, fpta_value_float(DBL_MIN)));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_fp64, fpta_value_float(-DBL_MIN)));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_fp64,
                               fpta_value_float(-3.14159265358979323846)));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_fp64,
                               fpta_value_float(3.14159265358979323846)));

  // колонка fp32
  // пробуем плохие типы
  EXPECT_EQ(FPTA_ETYPE,
            fpta_upsert_column(pt, &col_fp32, fpta_value_cstr("dummy")));
  EXPECT_EQ(FPTA_ETYPE, fpta_upsert_column(pt, &col_fp32, fpta_value_sint(0)));
  EXPECT_EQ(FPTA_ETYPE,
            fpta_upsert_column(pt, &col_fp32,
                               fpta_value_datetime(fptu_now_coarse())));
  // теперь плохие значения
  EXPECT_EQ(FPTA_EVALUE,
            fpta_upsert_column(pt, &col_fp32, fpta_value_float(std::nan(""))));
  EXPECT_EQ(FPTA_EVALUE,
            fpta_upsert_column(pt, &col_fp32, fpta_value_float(DBL_MAX)));
  EXPECT_EQ(FPTA_EVALUE,
            fpta_upsert_column(pt, &col_fp32, fpta_value_float(-DBL_MAX)));
  EXPECT_EQ(FPTA_EVALUE,
            fpta_upsert_column(
                pt, &col_fp32,
                fpta_value_float(FLT_MAX + (double)FLT_MAX * FLT_EPSILON)));
  EXPECT_EQ(FPTA_EVALUE,
            fpta_upsert_column(
                pt, &col_fp32,
                fpta_value_float(-FLT_MAX - (double)FLT_MAX * FLT_EPSILON)));
  // теперь подходящие значения, последнее должно остаться в силе
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_fp32, fpta_value_float(DBL_MIN)));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_fp32, fpta_value_float(-DBL_MIN)));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_fp32, fpta_value_float(FLT_MAX)));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_fp32, fpta_value_float(HUGE_VAL)));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_fp32, fpta_value_float(-FLT_MAX)));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_fp32, fpta_value_float(-HUGE_VAL)));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_fp32, fpta_value_float(FLT_MIN)));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_fp32, fpta_value_float(-FLT_MIN)));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_fp32,
                               fpta_value_float(2.7182818284590452354)));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_fp32,
                               fpta_value_float(-2.7182818284590452354)));

  // колонка datetime
  // пробуем плохие типы
  EXPECT_EQ(FPTA_ETYPE,
            fpta_upsert_column(pt, &col_datetime, fpta_value_cstr("dummy")));
  EXPECT_EQ(FPTA_ETYPE,
            fpta_upsert_column(pt, &col_datetime, fpta_value_sint(0)));
  EXPECT_EQ(FPTA_ETYPE,
            fpta_upsert_column(pt, &col_datetime, fpta_value_uint(0)));
  EXPECT_EQ(FPTA_ETYPE,
            fpta_upsert_column(pt, &col_datetime, fpta_value_float(0)));
  // для datetime нет плохих значений
  // теперь подходящие значения, последнее должно остаться в силе
  fptu_time datetime;
  datetime.fixedpoint = 0;
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_datetime,
                                        fpta_value_datetime(datetime)));
  datetime.fixedpoint = 1;
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_datetime,
                                        fpta_value_datetime(datetime)));
  datetime.fixedpoint = INT32_MAX;
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_datetime,
                                        fpta_value_datetime(datetime)));
  datetime.fixedpoint = INT64_MAX;
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_datetime,
                                        fpta_value_datetime(datetime)));
  datetime.fixedpoint = 0x847DBA5E5EAF395F;
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_datetime,
                                        fpta_value_datetime(datetime)));

  static const uint8_t *_96 = pattern;
  static const uint8_t *_128 = _96 + 12;
  static const uint8_t *_160 = _128 + 16;
  static const uint8_t *_256 = _160 + 24;
  ASSERT_LT(32, pattern + sizeof(pattern) - _256);
  ASSERT_LT(fpta_max_keylen, sizeof(pattern) / 2);

  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_96, fpta_value_binary(_96, 96 / 8)));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_128, fpta_value_binary(_128, 128 / 8)));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_160, fpta_value_binary(_160, 160 / 8)));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_256, fpta_value_binary(_256, 256 / 8)));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_str, fpta_value_cstr("xyz-string")));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_opaque,
                               fpta_value_binary(pattern, sizeof(pattern))));

  ASSERT_STREQ(nullptr, fptu_check(pt));
  fptu_ro row = fptu_take_noshrink(pt);
  ASSERT_STREQ(nullptr, fptu_check_ro(row));

  // теперь сравниваем значения всех колонок
  int error = -1;
  EXPECT_EQ(1354824703, fptu_get_uint32(row, col_uint32.column.num, &error));
  EXPECT_EQ(FPTU_OK, error);
  EXPECT_EQ(-8782211, fptu_get_int32(row, col_int32.column.num, &error));
  EXPECT_EQ(FPTU_OK, error);
  EXPECT_EQ((float)-2.7182818284590452354,
            fptu_get_fp32(row, col_fp32.column.num, &error));
  EXPECT_EQ(FPTU_OK, error);

  EXPECT_EQ(15047220096467327,
            fptu_get_uint64(row, col_uint64.column.num, &error));
  EXPECT_EQ(FPTU_OK, error);
  EXPECT_EQ(-60585001468255361,
            fptu_get_int64(row, col_int64.column.num, &error));
  EXPECT_EQ(FPTU_OK, error);
  EXPECT_EQ(3.14159265358979323846,
            fptu_get_fp64(row, col_fp64.column.num, &error));
  EXPECT_EQ(FPTU_OK, error);

  EXPECT_EQ(0x8001, fptu_get_uint16(row, col_uint16.column.num, &error));
  EXPECT_EQ(FPTU_OK, error);
  EXPECT_STREQ("xyz-string", fptu_get_cstr(row, col_str.column.num, &error));
  EXPECT_EQ(FPTU_OK, error);

  EXPECT_EQ(fptu_eq, fptu_cmp_opaque(row, col_opaque.column.num, pattern,
                                     sizeof(pattern)));
  EXPECT_EQ(fptu_eq, fptu_cmp_96(row, col_96.column.num, _96));
  EXPECT_EQ(fptu_eq, fptu_cmp_128(row, col_128.column.num, _128));
  EXPECT_EQ(fptu_eq, fptu_cmp_160(row, col_160.column.num, _160));
  EXPECT_EQ(datetime.fixedpoint,
            fptu_get_datetime(row, col_datetime.column.num, &error).fixedpoint);
  EXPECT_EQ(FPTU_OK, error);
  EXPECT_EQ(fptu_eq, fptu_cmp_256(row, col_256.column.num, _256));

  // TODO: fptu_nested
  // TODO: fptu_farray

  // разрушаем кортеж
  ASSERT_STREQ(nullptr, fptu_check(pt));
  free(pt);

  // прерываем транзакцию
  ASSERT_EQ(FPTA_OK, fpta_transaction_end(txn, true));
  txn = nullptr;

  // разрушаем привязанные идентификаторы
  fpta_name_destroy(&table);
  fpta_name_destroy(&col_uint16);
  fpta_name_destroy(&col_uint32);
  fpta_name_destroy(&col_int32);
  fpta_name_destroy(&col_fp32);
  fpta_name_destroy(&col_uint64);
  fpta_name_destroy(&col_int64);
  fpta_name_destroy(&col_fp64);
  fpta_name_destroy(&col_96);
  fpta_name_destroy(&col_128);
  fpta_name_destroy(&col_160);
  fpta_name_destroy(&col_datetime);
  fpta_name_destroy(&col_256);
  fpta_name_destroy(&col_str);
  fpta_name_destroy(&col_opaque);

  // закрываем и удаляем базу
  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));
  ASSERT_TRUE(unlink(testdb_name) == 0);
  ASSERT_TRUE(unlink(testdb_name_lck) == 0);
}

//----------------------------------------------------------------------------

static fptu_lge filter_cmp(const fptu_field *pf, const fpta_value &right) {
  return __fpta_filter_cmp(pf, &right);
}

TEST(Data, Compare_null) {
  /* Проверка сравнения с fptu_null.
   *
   * Сценарий:
   *  - пробуем сравнить всяческие варианты fpta_value с fptu_null.
   *  - результат "равно" должен быть для fpta_null и пусто бинарной строки.
   *  - в остальных случаях должно быть "несравнимо".
   */
  fptu_rw *pt = fptu_alloc(1, 12);
  ASSERT_NE(nullptr, pt);
  ASSERT_STREQ(nullptr, fptu_check(pt));

  // сравнение с null
  fptu_field *pf = nullptr;
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_null()));
  EXPECT_EQ(FPTU_OK, fptu_upsert_null(pt, 0));
  pf = fptu_lookup(pt, 0, fptu_null);
  ASSERT_NE(nullptr, pf);
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_null()));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_binary(nullptr, 0)));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_uint(42)));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_sint(42)));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_float(42)));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_cstr("dummy")));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_datetime(fptu_now_coarse())));
  fptu_erase_field(pt, pf);

  ASSERT_STREQ(nullptr, fptu_check(pt));
  free(pt);
}

TEST(Data, Compare_uint16) {
  /* Проверка сравнения с fptu_uint16.
   *
   * Сценарий:
   *  - пробуем сравнить всяческие варианты fpta_value с fptu_uint16.
   *  - для всех чисел, включая signed/unsigned и float/double, должен быть
   *    корректный результат больше/меньше/равно.
   *  - в остальных случаях должно быть "несравнимо".
   */
  fptu_rw *pt = fptu_alloc(1, 12);
  ASSERT_NE(nullptr, pt);
  ASSERT_STREQ(nullptr, fptu_check(pt));

  EXPECT_EQ(FPTU_OK, fptu_upsert_uint16(pt, 0, 42));
  fptu_field *pf = fptu_lookup(pt, 0, fptu_uint16);
  ASSERT_NE(nullptr, pf);
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_uint(42)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_sint(42)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_float(42)));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_cstr("dummy")));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_null()));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_datetime(fptu_now_coarse())));
  EXPECT_EQ(fptu_ic,
            filter_cmp(pf, fpta_value_binary(pattern, sizeof(pattern))));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_uint(43)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_uint(UINT64_MAX)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_sint(43)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_sint(INT64_MAX)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(43)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(FLT_MAX)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(DBL_MAX)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(HUGE_VAL)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_uint(41)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_sint(41)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(41)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_sint(-42)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-42)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-FLT_MAX)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-DBL_MAX)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-HUGE_VAL)));
  EXPECT_EQ(FPTU_OK, fptu_upsert_uint16(pt, 0, 0));
  ASSERT_EQ(pf, fptu_lookup(pt, 0, fptu_uint16));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_sint(-1)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_sint(INT64_MIN)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-1)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-FLT_MAX)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-DBL_MAX)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-FLT_MIN)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-DBL_MIN)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(FLT_MIN)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(DBL_MIN)));
  fptu_erase_field(pt, pf);

  ASSERT_STREQ(nullptr, fptu_check(pt));
  free(pt);
}

TEST(Data, Compare_uint32) {
  /* Проверка сравнения с fptu_uint32.
   *
   * Сценарий:
   *  - пробуем сравнить всяческие варианты fpta_value с fptu_uint32.
   *  - для всех чисел, включая signed/unsigned и float/double, должен быть
   *    корректный результат больше/меньше/равно.
   *  - в остальных случаях должно быть "несравнимо".
   */
  fptu_rw *pt = fptu_alloc(1, 12);
  ASSERT_NE(nullptr, pt);
  ASSERT_STREQ(nullptr, fptu_check(pt));

  EXPECT_EQ(FPTU_OK, fptu_upsert_uint32(pt, 0, 42));
  fptu_field *pf = fptu_lookup(pt, 0, fptu_uint32);
  ASSERT_NE(nullptr, pf);
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_uint(42)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_sint(42)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_float(42)));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_cstr("dummy")));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_null()));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_datetime(fptu_now_coarse())));
  EXPECT_EQ(fptu_ic,
            filter_cmp(pf, fpta_value_binary(pattern, sizeof(pattern))));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_uint(43)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_uint(UINT64_MAX)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_sint(43)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_sint(INT64_MAX)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(43)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(FLT_MAX)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(DBL_MAX)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(HUGE_VAL)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_uint(41)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_sint(41)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(41)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_sint(-42)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-42)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-FLT_MAX)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-DBL_MAX)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-HUGE_VAL)));
  EXPECT_EQ(FPTU_OK, fptu_upsert_uint32(pt, 0, 0));
  ASSERT_EQ(pf, fptu_lookup(pt, 0, fptu_uint32));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_sint(-1)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_sint(INT64_MIN)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-1)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-FLT_MAX)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-DBL_MAX)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-FLT_MIN)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-DBL_MIN)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(FLT_MIN)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(DBL_MIN)));
  EXPECT_EQ(FPTU_OK, fptu_upsert_uint32(pt, 0, UINT32_MAX));
  ASSERT_EQ(pf, fptu_lookup(pt, 0, fptu_uint32));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_sint(UINT32_MAX)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_uint(UINT32_MAX)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_float(UINT32_MAX)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_sint(UINT32_MAX - 1l)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_uint(UINT32_MAX - 1l)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(UINT32_MAX - 1l)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_sint(UINT32_MAX + 1l)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_uint(UINT32_MAX + 1l)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(UINT32_MAX + 1l)));
  fptu_erase_field(pt, pf);

  ASSERT_STREQ(nullptr, fptu_check(pt));
  free(pt);
}

TEST(Data, Compare_uint64) {
  /* Проверка сравнения с fptu_uint64.
   *
   * Сценарий:
   *  - пробуем сравнить всяческие варианты fpta_value с fptu_uint64.
   *  - для всех чисел, включая signed/unsigned и float/double, должен быть
   *    корректный результат больше/меньше/равно.
   *  - в остальных случаях должно быть "несравнимо".
   */
  fptu_rw *pt = fptu_alloc(1, 12);
  ASSERT_NE(nullptr, pt);
  ASSERT_STREQ(nullptr, fptu_check(pt));

  EXPECT_EQ(FPTU_OK, fptu_upsert_uint64(pt, 0, 42));
  fptu_field *pf = fptu_lookup(pt, 0, fptu_uint64);
  ASSERT_NE(nullptr, pf);
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_uint(42)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_sint(42)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_float(42)));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_cstr("dummy")));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_null()));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_datetime(fptu_now_coarse())));
  EXPECT_EQ(fptu_ic,
            filter_cmp(pf, fpta_value_binary(pattern, sizeof(pattern))));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_uint(43)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_uint(UINT64_MAX)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_sint(43)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_sint(INT64_MAX)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(43)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(FLT_MAX)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(DBL_MAX)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(HUGE_VAL)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_uint(41)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_sint(41)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(41)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_sint(-42)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-42)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-FLT_MAX)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-DBL_MAX)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-HUGE_VAL)));
  EXPECT_EQ(FPTU_OK, fptu_upsert_uint64(pt, 0, 0));
  ASSERT_EQ(pf, fptu_lookup(pt, 0, fptu_uint64));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_sint(-1)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_sint(INT64_MIN)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-1)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-FLT_MAX)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-DBL_MAX)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-FLT_MIN)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-DBL_MIN)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(FLT_MIN)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(DBL_MIN)));
  EXPECT_EQ(FPTU_OK, fptu_upsert_uint64(pt, 0, INT64_MAX));
  ASSERT_EQ(pf, fptu_lookup(pt, 0, fptu_uint64));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_sint(INT64_MAX)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_uint(INT64_MAX)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_float(INT64_MAX)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_sint(INT64_MAX - 1l)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_uint(INT64_MAX - 1l)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_uint(INT64_MAX + 1ul)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_uint(UINT64_MAX)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(UINT64_MAX)));
  fptu_erase_field(pt, pf);

  ASSERT_STREQ(nullptr, fptu_check(pt));
  free(pt);
}

TEST(Data, Compare_int32) {
  /* Проверка сравнения с fptu_int32.
   *
   * Сценарий:
   *  - пробуем сравнить всяческие варианты fpta_value с fptu_int32.
   *  - для всех чисел, включая signed/unsigned и float/double, должен быть
   *    корректный результат больше/меньше/равно.
   *  - в остальных случаях должно быть "несравнимо".
   */
  fptu_rw *pt = fptu_alloc(1, 12);
  ASSERT_NE(nullptr, pt);
  ASSERT_STREQ(nullptr, fptu_check(pt));

  EXPECT_EQ(FPTU_OK, fptu_upsert_int32(pt, 0, 42));
  fptu_field *pf = fptu_lookup(pt, 0, fptu_int32);
  ASSERT_NE(nullptr, pf);
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_uint(42)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_sint(42)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_float(42)));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_cstr("dummy")));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_null()));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_datetime(fptu_now_coarse())));
  EXPECT_EQ(fptu_ic,
            filter_cmp(pf, fpta_value_binary(pattern, sizeof(pattern))));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_uint(43)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_uint(UINT64_MAX)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_sint(43)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_sint(INT64_MAX)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(43)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(FLT_MAX)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(DBL_MAX)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(HUGE_VAL)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_uint(41)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_sint(41)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(41)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_sint(-42)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-42)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-FLT_MAX)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-DBL_MAX)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-HUGE_VAL)));
  EXPECT_EQ(FPTU_OK, fptu_upsert_int32(pt, 0, 0));
  ASSERT_EQ(pf, fptu_lookup(pt, 0, fptu_int32));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_sint(-1)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_sint(INT64_MIN)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-1)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-FLT_MAX)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-DBL_MAX)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-FLT_MIN)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-DBL_MIN)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(FLT_MIN)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(DBL_MIN)));
  EXPECT_EQ(FPTU_OK, fptu_upsert_int32(pt, 0, INT32_MAX));
  ASSERT_EQ(pf, fptu_lookup(pt, 0, fptu_int32));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_sint(INT32_MAX)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_uint(INT32_MAX)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_float(INT32_MAX)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_sint(INT32_MAX - 1l)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_uint(INT32_MAX - 1l)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(INT32_MAX - 1l)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_sint(INT32_MAX + 1l)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_uint(INT32_MAX + 1l)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(INT32_MAX + 1l)));
  EXPECT_EQ(FPTU_OK, fptu_upsert_int32(pt, 0, INT32_MIN));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_sint(0)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_uint(0)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(0)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_sint(INT32_MIN)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_uint(UINT32_MAX)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_float(INT32_MIN)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_sint(INT32_MIN - 1l)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_uint(UINT32_MAX - 1l)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(INT32_MIN - 1l)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_sint(INT32_MIN + 1l)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_uint(UINT32_MAX + 1l)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(INT32_MIN + 1l)));
  fptu_erase_field(pt, pf);

  ASSERT_STREQ(nullptr, fptu_check(pt));
  free(pt);
}

TEST(Data, Compare_int64) {
  /* Проверка сравнения с fptu_int64.
   *
   * Сценарий:
   *  - пробуем сравнить всяческие варианты fpta_value с fptu_int64.
   *  - для всех чисел, включая signed/unsigned и float/double, должен быть
   *    корректный результат больше/меньше/равно.
   *  - в остальных случаях должно быть "несравнимо".
   */
  fptu_rw *pt = fptu_alloc(1, 12);
  ASSERT_NE(nullptr, pt);
  ASSERT_STREQ(nullptr, fptu_check(pt));

  EXPECT_EQ(FPTU_OK, fptu_upsert_int64(pt, 0, 42));
  fptu_field *pf = fptu_lookup(pt, 0, fptu_int64);
  ASSERT_NE(nullptr, pf);
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_uint(42)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_sint(42)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_float(42)));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_cstr("dummy")));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_null()));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_datetime(fptu_now_coarse())));
  EXPECT_EQ(fptu_ic,
            filter_cmp(pf, fpta_value_binary(pattern, sizeof(pattern))));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_uint(43)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_uint(UINT64_MAX)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_sint(43)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_sint(INT64_MAX)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(43)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(FLT_MAX)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(DBL_MAX)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(HUGE_VAL)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_uint(41)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_sint(41)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(41)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_sint(-42)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-42)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-FLT_MAX)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-DBL_MAX)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-HUGE_VAL)));
  EXPECT_EQ(FPTU_OK, fptu_upsert_int64(pt, 0, 0));
  ASSERT_EQ(pf, fptu_lookup(pt, 0, fptu_int64));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_sint(-1)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_sint(INT64_MIN)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-1)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-FLT_MAX)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-DBL_MAX)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-FLT_MIN)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-DBL_MIN)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(FLT_MIN)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(DBL_MIN)));
  EXPECT_EQ(FPTU_OK, fptu_upsert_int64(pt, 0, INT64_MAX));
  ASSERT_EQ(pf, fptu_lookup(pt, 0, fptu_int64));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_sint(INT64_MAX)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_uint(INT64_MAX)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_float(INT64_MAX)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_sint(INT64_MAX - 1l)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_uint(INT64_MAX - 1l)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_uint(INT64_MAX + 1ull)));
  EXPECT_EQ(FPTU_OK, fptu_upsert_int64(pt, 0, INT64_MIN));
  ASSERT_EQ(pf, fptu_lookup(pt, 0, fptu_int64));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_sint(0)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_uint(0)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(0)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_sint(INT64_MIN)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_uint(UINT64_MAX)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_float(INT64_MIN)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_uint(UINT64_MAX - 1l)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_sint(INT64_MIN + 1l)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_uint(UINT64_MAX)));
  fptu_erase_field(pt, pf);

  ASSERT_STREQ(nullptr, fptu_check(pt));
  free(pt);
}

TEST(Data, Compare_fp64) {
  /* Проверка сравнения с fptu_fp64 (double).
   *
   * Сценарий:
   *  - пробуем сравнить всяческие варианты fpta_value с fptu_fp64.
   *  - для всех чисел, включая signed/unsigned и float/double, должен быть
   *    корректный результат больше/меньше/равно.
   *  - в остальных случаях должно быть "несравнимо".
   */
  fptu_rw *pt = fptu_alloc(1, 12);
  ASSERT_NE(nullptr, pt);
  ASSERT_STREQ(nullptr, fptu_check(pt));

  EXPECT_EQ(FPTU_OK, fptu_upsert_fp64(pt, 0, 42));
  fptu_field *pf = fptu_lookup(pt, 0, fptu_fp64);
  ASSERT_NE(nullptr, pf);
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_uint(42)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_sint(42)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_float(42)));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_cstr("dummy")));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_null()));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_datetime(fptu_now_coarse())));
  EXPECT_EQ(fptu_ic,
            filter_cmp(pf, fpta_value_binary(pattern, sizeof(pattern))));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_uint(43)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_uint(UINT64_MAX)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_sint(43)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_sint(INT64_MAX)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(43)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(FLT_MAX)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(DBL_MAX)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(HUGE_VAL)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_uint(41)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_sint(41)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(41)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_sint(-42)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-42)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-FLT_MAX)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-DBL_MAX)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-HUGE_VAL)));
  EXPECT_EQ(FPTU_OK, fptu_upsert_fp64(pt, 0, 0));
  ASSERT_EQ(pf, fptu_lookup(pt, 0, fptu_fp64));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_sint(-1)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_sint(INT64_MIN)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-1)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-FLT_MAX)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-DBL_MAX)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-FLT_MIN)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-DBL_MIN)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(FLT_MIN)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(DBL_MIN)));
  EXPECT_EQ(FPTU_OK, fptu_upsert_fp64(pt, 0, INT64_MAX));
  ASSERT_EQ(pf, fptu_lookup(pt, 0, fptu_fp64));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_sint(INT64_MAX)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_uint(INT64_MAX)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_float(INT64_MAX)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_sint(INT64_MAX / 2.0)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_uint(INT64_MAX / 2.0)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_uint(INT64_MAX + 2048ull)));
  EXPECT_EQ(FPTU_OK, fptu_upsert_fp64(pt, 0, INT64_MIN));
  ASSERT_EQ(pf, fptu_lookup(pt, 0, fptu_fp64));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_sint(0)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_uint(0)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(0)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_sint(INT64_MIN)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_uint(UINT64_MAX)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_float(INT64_MIN)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_uint(UINT64_MAX - 1l)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_sint(INT64_MIN + 2048ull)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_uint(UINT64_MAX)));

  EXPECT_EQ(FPTU_OK, fptu_upsert_fp64(pt, 0, HUGE_VAL));
  ASSERT_EQ(pf, fptu_lookup(pt, 0, fptu_fp64));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_float(HUGE_VAL)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(DBL_MAX)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(FLT_MAX)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(0)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-HUGE_VAL)));

  EXPECT_EQ(FPTU_OK, fptu_upsert_fp64(pt, 0, -HUGE_VAL));
  ASSERT_EQ(pf, fptu_lookup(pt, 0, fptu_fp64));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_float(-HUGE_VAL)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(-DBL_MAX)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(-FLT_MAX)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(0)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(HUGE_VAL)));

  fptu_erase_field(pt, pf);
  ASSERT_STREQ(nullptr, fptu_check(pt));
  free(pt);
}

TEST(Data, Compare_fp32) {
  /* Проверка сравнения с fptu_fp32 (float).
   *
   * Сценарий:
   *  - пробуем сравнить всяческие варианты fpta_value с fptu_fp32.
   *  - для всех чисел, включая signed/unsigned и float/double, должен быть
   *    корректный результат больше/меньше/равно.
   *  - в остальных случаях должно быть "несравнимо".
   */
  fptu_rw *pt = fptu_alloc(1, 12);
  ASSERT_NE(nullptr, pt);
  ASSERT_STREQ(nullptr, fptu_check(pt));

  EXPECT_EQ(FPTU_OK, fptu_upsert_fp32(pt, 0, 42));
  fptu_field *pf = fptu_lookup(pt, 0, fptu_fp32);
  ASSERT_NE(nullptr, pf);
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_uint(42)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_sint(42)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_float(42)));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_cstr("dummy")));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_null()));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_datetime(fptu_now_coarse())));
  EXPECT_EQ(fptu_ic,
            filter_cmp(pf, fpta_value_binary(pattern, sizeof(pattern))));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_uint(43)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_uint(UINT64_MAX)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_sint(43)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_sint(INT64_MAX)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(43)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(FLT_MAX)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(DBL_MAX)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(HUGE_VAL)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_uint(41)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_sint(41)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(41)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_sint(-42)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-42)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-FLT_MAX)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-DBL_MAX)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-HUGE_VAL)));
  EXPECT_EQ(FPTU_OK, fptu_upsert_fp32(pt, 0, 0));
  ASSERT_EQ(pf, fptu_lookup(pt, 0, fptu_fp32));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_sint(-1)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_sint(INT64_MIN)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-1)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-FLT_MAX)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-DBL_MAX)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-FLT_MIN)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-DBL_MIN)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(FLT_MIN)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(DBL_MIN)));
  EXPECT_EQ(FPTU_OK, fptu_upsert_fp32(pt, 0, INT64_MAX));
  ASSERT_EQ(pf, fptu_lookup(pt, 0, fptu_fp32));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_sint(INT64_MAX)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_uint(INT64_MAX)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_float(INT64_MAX)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_sint(INT64_MAX / 2.0)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_uint(INT64_MAX / 2.0)));
  EXPECT_EQ(fptu_lt,
            filter_cmp(pf, fpta_value_uint((uint64_t)INT64_MAX + UINT32_MAX)));
  EXPECT_EQ(FPTU_OK, fptu_upsert_fp32(pt, 0, INT64_MIN));
  ASSERT_EQ(pf, fptu_lookup(pt, 0, fptu_fp32));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_sint(0)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_uint(0)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(0)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_sint(INT64_MIN)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_uint(UINT64_MAX)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_float(INT64_MIN)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_uint(UINT64_MAX - 1l)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_sint(INT64_MIN + UINT32_MAX)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_uint(UINT64_MAX)));

  EXPECT_EQ(FPTU_OK, fptu_upsert_fp32(pt, 0, HUGE_VAL));
  ASSERT_EQ(pf, fptu_lookup(pt, 0, fptu_fp32));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_float(HUGE_VAL)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(DBL_MAX)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(FLT_MAX)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(0)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-HUGE_VAL)));

  EXPECT_EQ(FPTU_OK, fptu_upsert_fp32(pt, 0, -HUGE_VAL));
  ASSERT_EQ(pf, fptu_lookup(pt, 0, fptu_fp32));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_float(-HUGE_VAL)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(-DBL_MAX)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(-FLT_MAX)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(0)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(HUGE_VAL)));

#if !FPTA_PROHIBIT_LOSS_PRECISION
  EXPECT_EQ(FPTU_OK, fptu_upsert_fp32(pt, 0, -DBL_MIN));
  ASSERT_EQ(pf, fptu_lookup(pt, 0, fptu_fp32));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_float(1)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_float(0)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-DBL_MIN)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_float(-FLT_MIN)));
#endif

  fptu_erase_field(pt, pf);
  ASSERT_STREQ(nullptr, fptu_check(pt));
  free(pt);
}

TEST(Data, Compare_datetime) {
  /* Проверка сравнения с fptu_datetime (структура fptu_time).
   *
   * Сценарий:
   *  - пробуем сравнить всяческие варианты fpta_value с fptu_datetime.
   *  - сравнение допустимо только с fpta_datetime и при этом должно давать
   *    корректный результат больше/меньше/равно.
   *  - в остальных случаях должно быть "несравнимо".
   */
  fptu_rw *pt = fptu_alloc(1, 12);
  ASSERT_NE(nullptr, pt);
  ASSERT_STREQ(nullptr, fptu_check(pt));

  fptu_time now = fptu_now_fine();
  fptu_time zero;
  zero.fixedpoint = 0;
  fptu_time ones;
  ones.fixedpoint = UINT64_MAX;
  fptu_time less;
  less.fixedpoint = now.fixedpoint - 42;
  fptu_time great;
  great.fixedpoint = now.fixedpoint + 42;

  EXPECT_EQ(FPTU_OK, fptu_upsert_datetime(pt, 0, now));
  fptu_field *pf = fptu_lookup(pt, 0, fptu_datetime);
  ASSERT_NE(nullptr, pf);
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_uint(42)));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_sint(42)));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_float(42)));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_cstr("dummy")));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_null()));
  EXPECT_EQ(fptu_ic,
            filter_cmp(pf, fpta_value_binary(pattern, sizeof(pattern))));

  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_datetime(zero)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_datetime(ones)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_datetime(less)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_datetime(great)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_datetime(now)));

  EXPECT_EQ(FPTU_OK, fptu_upsert_datetime(pt, 0, zero));
  ASSERT_EQ(pf, fptu_lookup(pt, 0, fptu_datetime));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_datetime(zero)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_datetime(ones)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_datetime(less)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_datetime(great)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_datetime(now)));

  EXPECT_EQ(FPTU_OK, fptu_upsert_datetime(pt, 0, ones));
  ASSERT_EQ(pf, fptu_lookup(pt, 0, fptu_datetime));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_datetime(zero)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_datetime(ones)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_datetime(less)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_datetime(great)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_datetime(now)));

  EXPECT_EQ(FPTU_OK, fptu_upsert_datetime(pt, 0, less));
  ASSERT_EQ(pf, fptu_lookup(pt, 0, fptu_datetime));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_datetime(zero)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_datetime(ones)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_datetime(less)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_datetime(great)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_datetime(now)));

  EXPECT_EQ(FPTU_OK, fptu_upsert_datetime(pt, 0, great));
  ASSERT_EQ(pf, fptu_lookup(pt, 0, fptu_datetime));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_datetime(zero)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_datetime(ones)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_datetime(less)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_datetime(great)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_datetime(now)));

  fptu_erase_field(pt, pf);
  ASSERT_STREQ(nullptr, fptu_check(pt));
  free(pt);
}

TEST(Data, Compare_string) {
  /* Проверка сравнения с fptu_string (С-строка).
   *
   * Сценарий:
   *  - пробуем сравнить всяческие варианты fpta_value с fptu_string.
   *  - сравнение допустимо с fpta_string и fpta_binary и при этом должно
   *    давать корректный результат больше/меньше/равно, с учетом полезной
   *    длины данных (без терминирующего нуля), пояснение внутри fpta_value.
   *  - в остальных случаях должно быть "несравнимо".
   */
  fptu_rw *pt = fptu_alloc(1, 12);
  ASSERT_NE(nullptr, pt);
  ASSERT_STREQ(nullptr, fptu_check(pt));

  EXPECT_EQ(FPTU_OK, fptu_upsert_cstr(pt, 0, "42"));
  fptu_field *pf = fptu_lookup(pt, 0, fptu_cstr);
  ASSERT_NE(nullptr, pf);
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_uint(42)));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_sint(42)));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_float(42)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_cstr("42")));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_null()));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_datetime(fptu_now_coarse())));

  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_cstr("43")));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_cstr("\xff")));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_cstr("42\1")));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_cstr("41")));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_cstr("41\xff")));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_cstr("")));

  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_binary("42", 2)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_binary("42", 3)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_binary("43", 2)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_binary("\xff", 1)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary("41", 2)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary("41\xff", 2)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary("", 1)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary("", 0)));
  fptu_erase_field(pt, pf);

  ASSERT_STREQ(nullptr, fptu_check(pt));
  free(pt);
}

TEST(Data, Compare_binary) {
  /* Проверка сравнения с fptu_opaque (бинарная строка вариативной длины),
   * а также с бинарными типами фиксированного размера fptu_96/128/160/256.
   *
   * Сценарий:
   *  - пробуем сравнить всяческие варианты fpta_value с fptu_opaque/fixbin.
   *  - по ходу теста добавляется, обновляется и удаляется поле для
   *    каждого из типов fptu_opaque, fptu_96/128/160/256.
   *  - сравнения допустимы с fpta_string, fpta_binary и при этом должно
   *    давать корректный результат больше/меньше/равно, с учетом полезной
   *    длины данных (без терминирующего нуля), пояснение внутри fpta_value.
   *  - в остальных случаях должно быть "несравнимо".
   */
  fptu_rw *pt = fptu_alloc(1, 32);
  ASSERT_NE(nullptr, pt);
  ASSERT_STREQ(nullptr, fptu_check(pt));

  uint8_t zeros[32], ones[32];
  memset(zeros, 0, sizeof(zeros));
  memset(ones, ~0u, sizeof(ones));

  // fptu_opaque
  EXPECT_EQ(FPTU_OK, fptu_upsert_opaque(pt, 0, nullptr, 0));
  fptu_field *pf = fptu_lookup(pt, 0, fptu_opaque);
  ASSERT_NE(nullptr, pf);
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_uint(42)));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_sint(42)));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_float(42)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_cstr("42")));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_null()));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_datetime(fptu_now_coarse())));

  EXPECT_EQ(FPTU_OK, fptu_upsert_opaque(pt, 0, "42", 2));
  pf = fptu_lookup(pt, 0, fptu_opaque);
  ASSERT_EQ(pf, fptu_lookup(pt, 0, fptu_opaque));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_uint(42)));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_sint(42)));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_float(42)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_cstr("42")));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_null()));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_datetime(fptu_now_coarse())));

  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_cstr("43")));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_cstr("\xff")));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_cstr("42\1")));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_cstr("41")));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_cstr("41\xff")));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_cstr("")));

  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_binary("42", 2)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_binary("42", 3)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_binary("43", 2)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_binary("\xff", 1)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary("41", 2)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary("41\xff", 2)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary("", 1)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary("", 0)));

  EXPECT_EQ(FPTU_OK, fptu_upsert_opaque(pt, 0, zeros, 3));
  ASSERT_EQ(pf, fptu_lookup(pt, 0, fptu_opaque));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary("", 0)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary("", 1)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary("\0", 2)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_binary(zeros, 3)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_binary(zeros, 4)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_binary(zeros, 5)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_binary("\xff", 1)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_binary("\xff", 2)));

  EXPECT_EQ(FPTU_OK, fptu_upsert_opaque(pt, 0, ones, 3));
  ASSERT_EQ(pf, fptu_lookup(pt, 0, fptu_opaque));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary("", 0)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary("", 1)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary("\0", 2)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary(zeros, 3)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary(zeros, 4)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary(zeros, 5)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary(ones, 2)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_binary(ones, 3)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_binary(ones, 4)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_binary(ones, 5)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary("\xff", 1)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary("\xff", 2)));
  fptu_erase_field(pt, pf);
  ASSERT_STREQ(nullptr, fptu_check(pt));

  static const uint8_t *_96 = pattern;
  static const uint8_t *_128 = _96 + 12;
  static const uint8_t *_160 = _128 + 16;
  static const uint8_t *_256 = _160 + 24;
  ASSERT_LT(32, pattern + sizeof(pattern) - _256);

  // fptu_96
  EXPECT_EQ(FPTU_OK, fptu_upsert_96(pt, 0, zeros));
  pf = fptu_lookup(pt, 0, fptu_96);
  ASSERT_NE(nullptr, pf);
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary("", 0)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary("", 1)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary(zeros, 96 / 8 - 1)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_binary(zeros, 96 / 8)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_binary(zeros, 96 / 8 + 1)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_binary("\xff", 1)));

  EXPECT_EQ(FPTU_OK, fptu_upsert_96(pt, 0, ones));
  ASSERT_EQ(pf, fptu_lookup(pt, 0, fptu_96));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary("", 0)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary("", 1)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary("\xff", 1)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary(ones, 96 / 8 - 1)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_binary(ones, 96 / 8)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_binary(ones, 96 / 8 + 1)));

  EXPECT_EQ(FPTU_OK, fptu_upsert_96(pt, 0, _96));
  ASSERT_EQ(pf, fptu_lookup(pt, 0, fptu_96));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary("", 0)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary("", 1)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary(zeros, 2)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary(zeros, 96 / 8)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary(_96, 96 / 8 - 1)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_binary(_96, 96 / 8)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_binary(_96, 96 / 8 + 1)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_binary(ones, 2)));

  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_uint(42)));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_sint(42)));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_float(42)));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_cstr("42")));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_null()));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_datetime(fptu_now_coarse())));

  fptu_erase_field(pt, pf);
  ASSERT_STREQ(nullptr, fptu_check(pt));

  // fptu_128
  EXPECT_EQ(FPTU_OK, fptu_upsert_128(pt, 0, zeros));
  pf = fptu_lookup(pt, 0, fptu_128);
  ASSERT_NE(nullptr, pf);
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary("", 0)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary("", 1)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary(zeros, 128 / 8 - 1)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_binary(zeros, 128 / 8)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_binary(zeros, 128 / 8 + 1)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_binary("\xff", 1)));

  EXPECT_EQ(FPTU_OK, fptu_upsert_128(pt, 0, ones));
  ASSERT_EQ(pf, fptu_lookup(pt, 0, fptu_128));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary("", 0)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary("", 1)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary("\xff", 1)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary(ones, 128 / 8 - 1)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_binary(ones, 128 / 8)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_binary(ones, 128 / 8 + 1)));

  EXPECT_EQ(FPTU_OK, fptu_upsert_128(pt, 0, _128));
  ASSERT_EQ(pf, fptu_lookup(pt, 0, fptu_128));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary("", 0)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary("", 1)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary(zeros, 2)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary(zeros, 128 / 8)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary(_128, 128 / 8 - 1)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_binary(_128, 128 / 8)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_binary(_128, 128 / 8 + 1)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_binary(ones, 2)));

  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_uint(42)));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_sint(42)));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_float(42)));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_cstr("42")));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_null()));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_datetime(fptu_now_coarse())));

  fptu_erase_field(pt, pf);
  ASSERT_STREQ(nullptr, fptu_check(pt));

  // fptu_160
  EXPECT_EQ(FPTU_OK, fptu_upsert_160(pt, 0, zeros));
  pf = fptu_lookup(pt, 0, fptu_160);
  ASSERT_NE(nullptr, pf);
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary("", 0)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary("", 1)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary(zeros, 160 / 8 - 1)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_binary(zeros, 160 / 8)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_binary(zeros, 160 / 8 + 1)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_binary("\xff", 1)));

  EXPECT_EQ(FPTU_OK, fptu_upsert_160(pt, 0, ones));
  ASSERT_EQ(pf, fptu_lookup(pt, 0, fptu_160));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary("", 0)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary("", 1)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary("\xff", 1)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary(ones, 160 / 8 - 1)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_binary(ones, 160 / 8)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_binary(ones, 160 / 8 + 1)));

  EXPECT_EQ(FPTU_OK, fptu_upsert_160(pt, 0, _160));
  ASSERT_EQ(pf, fptu_lookup(pt, 0, fptu_160));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary("", 0)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary("", 1)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary(zeros, 2)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary(zeros, 160 / 8)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary(_160, 160 / 8 - 1)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_binary(_160, 160 / 8)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_binary(_160, 160 / 8 + 1)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_binary(ones, 2)));

  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_uint(42)));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_sint(42)));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_float(42)));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_cstr("42")));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_null()));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_datetime(fptu_now_coarse())));

  fptu_erase_field(pt, pf);
  ASSERT_STREQ(nullptr, fptu_check(pt));

  // fptu_256
  EXPECT_EQ(FPTU_OK, fptu_upsert_256(pt, 0, zeros));
  pf = fptu_lookup(pt, 0, fptu_256);
  ASSERT_NE(nullptr, pf);
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary("", 0)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary("", 1)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary(zeros, 256 / 8 - 1)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_binary(zeros, 256 / 8)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_binary(zeros, 256 / 8 + 1)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_binary("\xff", 1)));

  EXPECT_EQ(FPTU_OK, fptu_upsert_256(pt, 0, ones));
  ASSERT_EQ(pf, fptu_lookup(pt, 0, fptu_256));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary("", 0)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary("", 1)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary("\xff", 1)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary(ones, 256 / 8 - 1)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_binary(ones, 256 / 8)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_binary(ones, 256 / 8 + 1)));

  EXPECT_EQ(FPTU_OK, fptu_upsert_256(pt, 0, _256));
  ASSERT_EQ(pf, fptu_lookup(pt, 0, fptu_256));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary("", 0)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary("", 1)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary(zeros, 2)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary(zeros, 256 / 8)));
  EXPECT_EQ(fptu_gt, filter_cmp(pf, fpta_value_binary(_256, 256 / 8 - 1)));
  EXPECT_EQ(fptu_eq, filter_cmp(pf, fpta_value_binary(_256, 256 / 8)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_binary(_256, 256 / 8 + 1)));
  EXPECT_EQ(fptu_lt, filter_cmp(pf, fpta_value_binary(ones, 2)));

  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_uint(42)));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_sint(42)));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_float(42)));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_cstr("42")));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_null()));
  EXPECT_EQ(fptu_ic, filter_cmp(pf, fpta_value_datetime(fptu_now_coarse())));

  fptu_erase_field(pt, pf);
  ASSERT_STREQ(nullptr, fptu_check(pt));

  free(pt);
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

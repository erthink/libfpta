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

TEST(Value2Key, Invalid) {
  /* Тривиальный тест преобразования недопустимых значений в "ключи индексов".
   *
   * Сценарий:
   *  - пробуем явно недопустимые комбинации.
   *  - пробуем явно недопустимые псевдо-значения.
   */
  static const fpta_index_type index_cases[] = {
      /* clang-format off */
        fpta_primary_unique, fpta_primary_unique_unordered,
        fpta_primary_unique_reversed
      /* clang-format on */
  };

  fpta_key key;
  EXPECT_EQ(FPTA_EOOPS,
            value2key(fpta_column_shove(0, fptu_cstr, fpta_index_none),
                      fpta_value_cstr("42"), key));
  EXPECT_EQ(FPTA_EOOPS, value2key(fpta_column_shove(0, fptu_null, fpta_primary),
                                  fpta_value_cstr("42"), key));

  for (auto type = fptu_null; type <= fptu_nested; type = fptu_type(type + 1)) {
    for (auto index : index_cases) {
      SCOPED_TRACE("type " + std::to_string(type));
      SCOPED_TRACE("index " + std::to_string(index));

      const auto valid = fpta_column_shove(0, type, index);
      EXPECT_EQ(FPTA_ETYPE, value2key(valid, fpta_value_null(), key));
      EXPECT_EQ(FPTA_ETYPE, value2key(valid, fpta_value_begin(), key));
      EXPECT_EQ(FPTA_ETYPE, value2key(valid, fpta_value_end(), key));
    }
  }
}

TEST(Value2Key, uint16) {
  /* Тест преобразования значений в "ключи индекса" для fptu_uint16,
   * включая проверку сравнения через индексный компаратор.
   *
   * Сценарий:
   *  1. Проверяем конвертацию значений:
   *    - конвертируем допустимые значения fpta_value, ожидая FPTA_OK.
   *    - пробуем несовместимые типы, ожидая FPTA_ETYPE.
   *    - пробуем конвертировать значения вне диапазона, ожидая FPTA_EVALUE.
   *    - всё перечисленное проверяем для ordered и unordered индексов.
   *  2. Проверяем конвертацию и сравнение через компараторы индексов:
   *    - в цикле добавляем в "пробник" несколько значений с краю от
   *      диапазона машинного типа.
   *    - при добавлении в "пробник" каждое значение снабжается ожидаемым
   *      порядковым номером.
   *    - внутри пробника несколько упорядоченных контейнеров (std::map),
   *      для каждого допустимого класса индексов obverse/reverse/unordered,
   *      которые используют соответствующий индексный компаратор.
   *    - в конце "пробник" проверяет кол-во элементов в каждом проверяющим
   *      индекс контейнере, и корректность порядка элементов.
   *    - также "пробник" самостоятельно пытается добавить дубликаты.
   *    - этим проверяется как корректность формирования ключей, так и
   *      верность результата соответствующих индексных компараторов.
   */
  const auto ordered = fpta_column_shove(0, fptu_uint16, fpta_primary_unique);
  const auto unordered =
      fpta_column_shove(0, fptu_uint16, fpta_primary_unique_unordered);
  fpta_key key;

  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_uint(42), key));
  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_uint(UINT16_MAX - 1), key));
  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_uint(UINT16_MAX), key));
  EXPECT_EQ(FPTA_EVALUE,
            value2key(ordered, fpta_value_uint(UINT16_MAX + 1), key));
  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_sint(42), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(ordered, fpta_value_float(42), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(ordered, fpta_value_cstr("42"), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(ordered, fpta_value_binary("42", 2), key));

  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_uint(42), key));
  EXPECT_EQ(FPTA_OK,
            value2key(unordered, fpta_value_uint(UINT16_MAX - 1), key));
  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_uint(UINT16_MAX), key));
  EXPECT_EQ(FPTA_EVALUE,
            value2key(unordered, fpta_value_uint(UINT16_MAX + 1), key));
  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_sint(42), key));
  EXPECT_EQ(FPTA_EVALUE, value2key(unordered, fpta_value_sint(-42), key));
  EXPECT_EQ(FPTA_EVALUE,
            value2key(unordered, fpta_value_sint(UINT16_MAX + 1), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(unordered, fpta_value_float(42), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(unordered, fpta_value_cstr("42"), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(unordered, fpta_value_binary("42", 2), key));

  // проверяем через компараторы индексов
  probe_triplet<fptu_uint16> probe;
  for (auto i = 0; i < 42; i++) {
    probe(fpta_value_uint(UINT16_MAX - i), 100 - i);
    probe(fpta_value_uint(i), i);
  }
  probe.check(42 * 2);
}

TEST(Value2Key, uint32) {
  /* Тест преобразования значений в "ключи индекса" для fptu_uint32,
   * включая проверку сравнения через индексный компаратор.
   *
   * Сценарий:
   *  1. Проверяем конвертацию значений:
   *    - конвертируем допустимые значения fpta_value, ожидая FPTA_OK.
   *    - пробуем несовместимые типы, ожидая FPTA_ETYPE.
   *    - пробуем конвертировать значения вне диапазона, ожидая FPTA_EVALUE.
   *    - всё перечисленное проверяем для ordered и unordered индексов.
   *  2. Проверяем конвертацию и сравнение через компараторы индексов:
   *    - в цикле добавляем в "пробник" несколько значений с краю от
   *      диапазона машинного типа.
   *    - при добавлении в "пробник" каждое значение снабжается ожидаемым
   *      порядковым номером.
   *    - внутри пробника несколько упорядоченных контейнеров (std::map),
   *      для каждого допустимого класса индексов obverse/reverse/unordered,
   *      которые используют соответствующий индексный компаратор.
   *    - в конце "пробник" проверяет кол-во элементов в каждом проверяющим
   *      индекс контейнере, и корректность порядка элементов.
   *    - также "пробник" самостоятельно пытается добавить дубликаты.
   *    - этим проверяется как корректность формирования ключей, так и
   *      верность результата соответствующих индексных компараторов.
   */
  const auto ordered = fpta_column_shove(0, fptu_uint32, fpta_primary_unique);
  const auto unordered =
      fpta_column_shove(0, fptu_uint32, fpta_primary_unique_unordered);
  fpta_key key;

  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_uint(42), key));
  EXPECT_EQ(FPTA_OK,
            value2key(ordered, fpta_value_uint(UINT32_MAX - UINT64_C(1)), key));
  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_uint(UINT32_MAX), key));
  EXPECT_EQ(FPTA_EVALUE,
            value2key(ordered, fpta_value_uint(UINT32_MAX + UINT64_C(1)), key));
  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_sint(42), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(ordered, fpta_value_float(42), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(ordered, fpta_value_cstr("42"), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(ordered, fpta_value_binary("42", 2), key));

  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_uint(42), key));
  EXPECT_EQ(FPTA_OK, value2key(unordered,
                               fpta_value_uint(UINT32_MAX - UINT64_C(1)), key));
  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_uint(UINT32_MAX), key));
  EXPECT_EQ(
      FPTA_EVALUE,
      value2key(unordered, fpta_value_uint(UINT32_MAX + UINT64_C(1)), key));
  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_sint(42), key));
  EXPECT_EQ(FPTA_EVALUE, value2key(unordered, fpta_value_sint(-42), key));
  EXPECT_EQ(
      FPTA_EVALUE,
      value2key(unordered, fpta_value_sint(UINT32_MAX + INT64_C(1)), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(unordered, fpta_value_float(42), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(unordered, fpta_value_cstr("42"), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(unordered, fpta_value_binary("42", 2), key));

  // проверяем через компараторы индексов
  probe_triplet<fptu_uint32> probe;
  for (auto i = 0; i < 42; i++) {
    probe(fpta_value_uint(UINT32_MAX - i), 100 - i);
    probe(fpta_value_uint(i), i);
  }
  probe.check(42 * 2);
}

TEST(Value2Key, uint64) {
  /* Тест преобразования значений в "ключи индекса" для fptu_uint64,
   * включая проверку сравнения через индексный компаратор.
   *
   * Сценарий:
   *  1. Проверяем конвертацию значений:
   *    - конвертируем допустимые значения fpta_value, ожидая FPTA_OK.
   *    - пробуем несовместимые типы, ожидая FPTA_ETYPE.
   *    - пробуем конвертировать значения вне диапазона, ожидая FPTA_EVALUE.
   *    - всё перечисленное проверяем для ordered и unordered индексов.
   *  2. Проверяем конвертацию и сравнение через компараторы индексов:
   *    - в цикле добавляем в "пробник" несколько значений с краю от
   *      диапазона машинного типа.
   *    - при добавлении в "пробник" каждое значение снабжается ожидаемым
   *      порядковым номером.
   *    - внутри пробника несколько упорядоченных контейнеров (std::map),
   *      для каждого допустимого класса индексов obverse/reverse/unordered,
   *      которые используют соответствующий индексный компаратор.
   *    - в конце "пробник" проверяет кол-во элементов в каждом проверяющим
   *      индекс контейнере, и корректность порядка элементов.
   *    - также "пробник" самостоятельно пытается добавить дубликаты.
   *    - этим проверяется как корректность формирования ключей, так и
   *      верность результата соответствующих индексных компараторов.
   */
  const auto ordered = fpta_column_shove(0, fptu_uint64, fpta_primary_unique);
  const auto unordered =
      fpta_column_shove(0, fptu_uint64, fpta_primary_unique_unordered);
  fpta_key key;

  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_uint(42), key));
  EXPECT_EQ(FPTA_OK,
            value2key(ordered, fpta_value_uint(UINT64_MAX - UINT64_C(1)), key));
  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_uint(UINT64_MAX), key));
  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_sint(42), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(ordered, fpta_value_float(42), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(ordered, fpta_value_cstr("42"), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(ordered, fpta_value_binary("42", 2), key));

  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_uint(42), key));
  EXPECT_EQ(FPTA_OK, value2key(unordered,
                               fpta_value_uint(UINT64_MAX - UINT64_C(1)), key));
  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_uint(UINT64_MAX), key));
  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_sint(42), key));
  EXPECT_EQ(FPTA_EVALUE, value2key(unordered, fpta_value_sint(-42), key));
  EXPECT_EQ(FPTA_EVALUE, value2key(unordered, fpta_value_sint(INT64_MIN), key));
  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_sint(INT64_MAX), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(unordered, fpta_value_float(42), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(unordered, fpta_value_cstr("42"), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(unordered, fpta_value_binary("42", 2), key));

  // проверяем через компараторы индексов
  probe_triplet<fptu_uint64> probe;
  for (auto i = 0; i < 42; i++) {
    probe(fpta_value_uint(UINT64_MAX - i), 100 - i);
    probe(fpta_value_uint(i), i);
  }
  probe.check(42 * 2);
}

TEST(Value2Key, int32) {
  /* Тест преобразования значений в "ключи индекса" для fptu_int32,
   * включая проверку сравнения через индексный компаратор.
   *
   * Сценарий:
   *  1. Проверяем конвертацию значений:
   *    - конвертируем допустимые значения fpta_value, ожидая FPTA_OK.
   *    - пробуем несовместимые типы, ожидая FPTA_ETYPE.
   *    - пробуем конвертировать значения вне диапазона, ожидая FPTA_EVALUE.
   *    - всё перечисленное проверяем для ordered и unordered индексов.
   *  2. Проверяем конвертацию и сравнение через компараторы индексов:
   *    - в цикле добавляем в "пробник" несколько значений с краю от
   *      диапазона машинного типа.
   *    - при добавлении в "пробник" каждое значение снабжается ожидаемым
   *      порядковым номером.
   *    - внутри пробника несколько упорядоченных контейнеров (std::map),
   *      для каждого допустимого класса индексов obverse/reverse/unordered,
   *      которые используют соответствующий индексный компаратор.
   *    - в конце "пробник" проверяет кол-во элементов в каждом проверяющим
   *      индекс контейнере, и корректность порядка элементов.
   *    - также "пробник" самостоятельно пытается добавить дубликаты.
   *    - этим проверяется как корректность формирования ключей, так и
   *      верность результата соответствующих индексных компараторов.
   */
  const auto ordered = fpta_column_shove(0, fptu_int32, fpta_primary_unique);
  const auto unordered =
      fpta_column_shove(0, fptu_int32, fpta_primary_unique_unordered);
  fpta_key key;

  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_sint(42), key));
  EXPECT_EQ(FPTA_EVALUE,
            value2key(ordered, fpta_value_sint(INT32_MIN - INT64_C(1)), key));
  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_sint(INT32_MIN), key));
  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_sint(INT32_MIN + 1), key));
  EXPECT_EQ(FPTA_OK,
            value2key(ordered, fpta_value_sint(INT32_MAX - INT64_C(1)), key));
  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_sint(INT32_MAX), key));
  EXPECT_EQ(FPTA_EVALUE,
            value2key(ordered, fpta_value_sint(INT32_MAX + INT64_C(1)), key));
  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_uint(42), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(ordered, fpta_value_float(42), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(ordered, fpta_value_cstr("42"), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(ordered, fpta_value_binary("42", 2), key));

  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_sint(42), key));
  EXPECT_EQ(FPTA_EVALUE,
            value2key(unordered, fpta_value_sint(INT32_MIN - INT64_C(1)), key));
  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_sint(INT32_MIN), key));
  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_sint(INT32_MIN + 1), key));
  EXPECT_EQ(FPTA_OK,
            value2key(unordered, fpta_value_sint(INT32_MAX - INT64_C(1)), key));
  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_sint(INT32_MAX), key));
  EXPECT_EQ(FPTA_EVALUE,
            value2key(unordered, fpta_value_sint(INT32_MAX + INT64_C(1)), key));
  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_uint(42), key));
  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_uint(INT32_MAX), key));
  EXPECT_EQ(
      FPTA_EVALUE,
      value2key(unordered, fpta_value_uint(INT32_MAX + UINT64_C(1)), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(unordered, fpta_value_float(42), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(unordered, fpta_value_cstr("42"), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(unordered, fpta_value_binary("42", 2), key));

  // проверяем через компараторы индексов
  probe_triplet<fptu_int32> probe;
  for (auto i = 0; i < 42; i++) {
    probe(fpta_value_sint(INT32_MIN + i), i);
    probe(fpta_value_sint(INT32_MAX - i), 200 - i);
    probe(fpta_value_sint(i - 21), 100 + i);
  }
  probe.check(42 * 3);
}

TEST(Value2Key, int64) {
  /* Тест преобразования значений в "ключи индекса" для fptu_int64,
   * включая проверку сравнения через индексный компаратор.
   *
   * Сценарий:
   *  1. Проверяем конвертацию значений:
   *    - конвертируем допустимые значения fpta_value, ожидая FPTA_OK.
   *    - пробуем несовместимые типы, ожидая FPTA_ETYPE.
   *    - пробуем конвертировать значения вне диапазона, ожидая FPTA_EVALUE.
   *    - всё перечисленное проверяем для ordered и unordered индексов.
   *  2. Проверяем конвертацию и сравнение через компараторы индексов:
   *    - в цикле добавляем в "пробник" несколько значений с краю от
   *      диапазона машинного типа.
   *    - при добавлении в "пробник" каждое значение снабжается ожидаемым
   *      порядковым номером.
   *    - внутри пробника несколько упорядоченных контейнеров (std::map),
   *      для каждого допустимого класса индексов obverse/reverse/unordered,
   *      которые используют соответствующий индексный компаратор.
   *    - в конце "пробник" проверяет кол-во элементов в каждом проверяющим
   *      индекс контейнере, и корректность порядка элементов.
   *    - также "пробник" самостоятельно пытается добавить дубликаты.
   *    - этим проверяется как корректность формирования ключей, так и
   *      верность результата соответствующих индексных компараторов.
   */
  const auto ordered = fpta_column_shove(0, fptu_int64, fpta_primary_unique);
  const auto unordered =
      fpta_column_shove(0, fptu_int64, fpta_primary_unique_unordered);
  fpta_key key;

  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_sint(42), key));
  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_sint(INT64_MIN), key));
  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_sint(INT64_MAX), key));
  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_uint(42), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(ordered, fpta_value_float(42), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(ordered, fpta_value_cstr("42"), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(ordered, fpta_value_binary("42", 2), key));

  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_sint(42), key));
  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_sint(INT64_MIN), key));
  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_sint(INT64_MAX), key));
  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_uint(42), key));
  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_uint(INT64_MAX), key));
  EXPECT_EQ(FPTA_EVALUE,
            value2key(unordered, fpta_value_uint(INT64_MAX + 1ull), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(unordered, fpta_value_float(42), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(unordered, fpta_value_cstr("42"), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(unordered, fpta_value_binary("42", 2), key));

  // проверяем через компараторы индексов
  probe_triplet<fptu_int64> probe;
  for (auto i = 0; i < 42; i++) {
    probe(fpta_value_sint(INT64_MIN + i), i);
    probe(fpta_value_sint(INT64_MAX - i), 200 - i);
    probe(fpta_value_sint(i - 21), 100 + i);
  }
  probe.check(42 * 3);
}

TEST(Value2Key, fp64) {
  /* Тест преобразования значений в "ключи индекса" для fptu_fp64 (double),
   * включая проверку сравнения через индексный компаратор.
   *
   * Сценарий:
   *  1. Проверяем конвертацию значений:
   *    - конвертируем допустимые значения fpta_value, ожидая FPTA_OK.
   *    - пробуем несовместимые типы, ожидая FPTA_ETYPE.
   *    - пробуем конвертировать значения вне диапазона, ожидая FPTA_EVALUE.
   *    - проверяются также специальные для double/float значения:
   *      бесконечности, эпсилоны, переполнения и потеря значимости для float.
   *    - всё перечисленное проверяем для ordered и unordered индексов.
   *  2. Проверяем конвертацию и сравнение через компараторы индексов:
   *    - в цикле добавляем в "пробник" несколько значений с краю от
   *      диапазона машинного типа.
   *    - при добавлении в "пробник" каждое значение снабжается ожидаемым
   *      порядковым номером.
   *    - внутри пробника несколько упорядоченных контейнеров (std::map),
   *      для каждого допустимого класса индексов obverse/reverse/unordered,
   *      которые используют соответствующий индексный компаратор.
   *    - в конце "пробник" проверяет кол-во элементов в каждом проверяющим
   *      индекс контейнере, и корректность порядка элементов.
   *    - также "пробник" самостоятельно пытается добавить дубликаты.
   *    - этим проверяется как корректность формирования ключей, так и
   *      верность результата соответствующих индексных компараторов.
   */
  const auto ordered = fpta_column_shove(0, fptu_fp64, fpta_primary_unique);
  const auto unordered =
      fpta_column_shove(0, fptu_fp64, fpta_primary_unique_unordered);
  fpta_key key;

  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_float(42), key));
  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_float(HUGE_VAL), key));
  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_float(-HUGE_VAL), key));
  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_float(INT64_MIN), key));
  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_float(INT64_MAX), key));
  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_float(FLT_MIN), key));
  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_float(FLT_MAX), key));
  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_float(flt_pos_over), key));
  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_float(-FLT_MIN), key));
  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_float(-FLT_MAX), key));
  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_float(flt_neg_over), key));
  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_float(DBL_MIN), key));
  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_float(DBL_MAX), key));
  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_float(-DBL_MIN), key));
  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_float(-DBL_MAX), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(ordered, fpta_value_uint(42), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(ordered, fpta_value_sint(42), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(ordered, fpta_value_cstr("42"), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(ordered, fpta_value_binary("42", 2), key));

  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_float(42), key));
  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_float(HUGE_VAL), key));
  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_float(-HUGE_VAL), key));
  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_float(INT64_MIN), key));
  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_float(INT64_MAX), key));
  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_float(FLT_MIN), key));
  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_float(FLT_MAX), key));
  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_float(flt_pos_over), key));
  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_float(-FLT_MIN), key));
  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_float(-FLT_MAX), key));
  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_float(flt_neg_over), key));
  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_float(DBL_MIN), key));
  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_float(DBL_MAX), key));
  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_float(-DBL_MIN), key));
  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_float(-DBL_MAX), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(unordered, fpta_value_uint(42), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(unordered, fpta_value_sint(42), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(unordered, fpta_value_cstr("42"), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(unordered, fpta_value_binary("42", 2), key));

  // проверяем через компараторы индексов
  probe_triplet<fptu_fp64> probe;

  int i = 0;
  probe(fpta_value_float(-HUGE_VAL), ++i);
  probe(fpta_value_float(-DBL_MAX), ++i);
  probe(fpta_value_float(flt_neg_over), ++i);
  probe(fpta_value_float(-FLT_MAX), ++i);
  probe(fpta_value_float(flt_neg_below), ++i);
  probe(fpta_value_float(INT64_MIN), ++i);
  probe(fpta_value_float(INT32_MIN), ++i);
  probe(fpta_value_float(INT16_MIN), ++i);
  probe(fpta_value_float(-42), ++i);
  probe(fpta_value_float(-1 - FLT_EPSILON), ++i);
  probe(fpta_value_float(-1 - DBL_EPSILON), ++i);
  probe(fpta_value_float(-1), ++i);
  probe(fpta_value_float(-FLT_MIN), ++i);
  probe(fpta_value_float(-DBL_MIN), ++i);
  probe(fpta_value_float(0), ++i);
  probe(fpta_value_float(DBL_MIN), ++i);
  probe(fpta_value_float(FLT_MIN), ++i);
  probe(fpta_value_float(1), ++i);
  probe(fpta_value_float(1 + DBL_EPSILON), ++i);
  probe(fpta_value_float(1 + FLT_EPSILON), ++i);
  probe(fpta_value_float(42), ++i);
  probe(fpta_value_float(INT16_MAX), ++i);
  probe(fpta_value_float(INT32_MAX), ++i);
  probe(fpta_value_float(INT64_MAX), ++i);
  probe(fpta_value_float(UINT64_MAX), ++i);
  probe(fpta_value_float(flt_pos_below), ++i);
  probe(fpta_value_float(FLT_MAX), ++i);
  probe(fpta_value_float(flt_pos_over), ++i);
  probe(fpta_value_float(DBL_MAX), ++i);
  probe(fpta_value_float(HUGE_VAL), ++i);
  probe.check();
}

TEST(Value2Key, fp32) {
  /* Тест преобразования значений в "ключи индекса" для fptu_fp32 (float),
   * включая проверку сравнения через индексный компаратор.
   *
   * Сценарий:
   *  1. Проверяем конвертацию значений:
   *    - конвертируем допустимые значения fpta_value, ожидая FPTA_OK.
   *    - пробуем несовместимые типы, ожидая FPTA_ETYPE.
   *    - пробуем конвертировать значения вне диапазона, ожидая FPTA_EVALUE.
   *    - проверяются также специальные для double/float значения:
   *      бесконечности, эпсилоны, переполнения и потеря значимости для float.
   *    - всё перечисленное проверяем для ordered и unordered индексов.
   *  2. Проверяем конвертацию и сравнение через компараторы индексов:
   *    - в цикле добавляем в "пробник" несколько значений с краю от
   *      диапазона машинного типа.
   *    - при добавлении в "пробник" каждое значение снабжается ожидаемым
   *      порядковым номером.
   *    - внутри пробника несколько упорядоченных контейнеров (std::map),
   *      для каждого допустимого класса индексов obverse/reverse/unordered,
   *      которые используют соответствующий индексный компаратор.
   *    - в конце "пробник" проверяет кол-во элементов в каждом проверяющим
   *      индекс контейнере, и корректность порядка элементов.
   *    - также "пробник" самостоятельно пытается добавить дубликаты.
   *    - этим проверяется как корректность формирования ключей, так и
   *      верность результата соответствующих индексных компараторов.
   */
  const auto ordered = fpta_column_shove(0, fptu_fp32, fpta_primary_unique);
  const auto unordered =
      fpta_column_shove(0, fptu_fp32, fpta_primary_unique_unordered);
  fpta_key key;

  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_float(42), key));
  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_float(HUGE_VAL), key));
  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_float(-HUGE_VAL), key));
  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_float(INT64_MIN), key));
  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_float(INT64_MAX), key));
  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_float(INT16_MIN), key));
  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_float(INT16_MAX), key));
  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_float(FLT_MIN), key));
  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_float(FLT_MAX), key));
  EXPECT_EQ(FPTA_EVALUE,
            value2key(ordered, fpta_value_float(flt_pos_over), key));
  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_float(-FLT_MIN), key));
  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_float(-FLT_MAX), key));
  EXPECT_EQ(FPTA_EVALUE,
            value2key(ordered, fpta_value_float(flt_neg_over), key));
  EXPECT_EQ(FPTA_EVALUE, value2key(ordered, fpta_value_float(DBL_MAX), key));
  EXPECT_EQ(FPTA_EVALUE, value2key(ordered, fpta_value_float(-DBL_MAX), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(ordered, fpta_value_uint(42), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(ordered, fpta_value_sint(42), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(ordered, fpta_value_cstr("42"), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(ordered, fpta_value_binary("42", 2), key));
  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_float(flt_pos_below), key));
  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_float(flt_neg_below), key));
#if FPTA_PROHIBIT_LOSS_PRECISION
  EXPECT_EQ(FPTA_EVALUE, value2key(ordered, fpta_value_float(DBL_MIN), key));
  EXPECT_EQ(FPTA_EVALUE, value2key(ordered, fpta_value_float(-DBL_MIN), key));
  EXPECT_EQ(FPTA_EVALUE, value2key(ordered, fpta_value_float(INT32_MAX), key));
#else
  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_float(DBL_MIN), key));
  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_float(-DBL_MIN), key));
  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_float(INT32_MAX), key));
#endif
  EXPECT_EQ(FPTA_OK, value2key(ordered, fpta_value_float(INT32_MIN), key));

  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_float(42), key));
  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_float(HUGE_VAL), key));
  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_float(-HUGE_VAL), key));
  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_float(INT64_MIN), key));
  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_float(INT64_MAX), key));
  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_float(FLT_MIN), key));
  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_float(FLT_MAX), key));
  EXPECT_EQ(FPTA_EVALUE,
            value2key(unordered, fpta_value_float(flt_pos_over), key));
  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_float(-FLT_MIN), key));
  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_float(-FLT_MAX), key));
  EXPECT_EQ(FPTA_EVALUE,
            value2key(ordered, fpta_value_float(flt_neg_over), key));
  EXPECT_EQ(FPTA_EVALUE, value2key(unordered, fpta_value_float(DBL_MAX), key));
  EXPECT_EQ(FPTA_EVALUE, value2key(unordered, fpta_value_float(-DBL_MAX), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(unordered, fpta_value_uint(42), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(unordered, fpta_value_sint(42), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(unordered, fpta_value_cstr("42"), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(unordered, fpta_value_binary("42", 2), key));
  EXPECT_EQ(FPTA_OK,
            value2key(unordered, fpta_value_float(flt_pos_below), key));
  EXPECT_EQ(FPTA_OK,
            value2key(unordered, fpta_value_float(flt_neg_below), key));
#if FPTA_PROHIBIT_LOSS_PRECISION
  EXPECT_EQ(FPTA_EVALUE, value2key(unordered, fpta_value_float(DBL_MIN), key));
  EXPECT_EQ(FPTA_EVALUE, value2key(unordered, fpta_value_float(-DBL_MIN), key));
  EXPECT_EQ(FPTA_EVALUE,
            value2key(unordered, fpta_value_float(INT32_MAX), key));
#else
  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_float(DBL_MIN), key));
  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_float(-DBL_MIN), key));
  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_float(INT32_MAX), key));
#endif
  EXPECT_EQ(FPTA_OK, value2key(unordered, fpta_value_float(INT32_MIN), key));

  // проверяем через компараторы индексов
  probe_triplet<fptu_fp32> probe;

  int i = 0;
  probe(fpta_value_float(-HUGE_VAL), ++i);
  probe(fpta_value_float(-FLT_MAX), ++i);
  probe(fpta_value_float(flt_neg_below), ++i);
  probe(fpta_value_float(INT64_MIN), ++i);
  probe(fpta_value_float(INT32_MIN), ++i);
  probe(fpta_value_float(INT16_MIN), ++i);
  probe(fpta_value_float(-42), ++i);
  probe(fpta_value_float(-1 - FLT_EPSILON), ++i);
  probe(fpta_value_float(-1), ++i);
#if !FPTA_PROHIBIT_LOSS_PRECISION
  probe(fpta_value_float(-1 - DBL_EPSILON), i, true);
#endif
  probe(fpta_value_float(-FLT_MIN), ++i);
  probe(fpta_value_float(0), ++i);
#if !FPTA_PROHIBIT_LOSS_PRECISION
  probe(fpta_value_float(-DBL_MIN), i, true);
  probe(fpta_value_float(DBL_MIN), i, true);
#endif
  probe(fpta_value_float(FLT_MIN), ++i);
  probe(fpta_value_float(1), ++i);
#if !FPTA_PROHIBIT_LOSS_PRECISION
  probe(fpta_value_float(1 + DBL_EPSILON), i, true);
#endif
  probe(fpta_value_float(1 + FLT_EPSILON), ++i);
  probe(fpta_value_float(42), ++i);
  probe(fpta_value_float(INT16_MAX), ++i);
#if !FPTA_PROHIBIT_LOSS_PRECISION
  probe(fpta_value_float(INT32_MAX), ++i);
#endif
  probe(fpta_value_float(INT64_MAX), ++i);
  probe(fpta_value_float(UINT64_MAX), ++i);
  probe(fpta_value_float(flt_pos_below), ++i);
  probe(fpta_value_float(FLT_MAX), ++i);
  probe(fpta_value_float(HUGE_VAL), ++i);

  probe.check();
}

TEST(Value2Key, datetime) {
  /* Тест преобразования значений в "ключи индекса" для fptu_datetime,
   * включая проверку сравнения через индексный компаратор.
   *
   * Сценарий:
   *  1. Проверяем конвертацию значений:
   *    - конвертируем допустимые значения fpta_value, ожидая FPTA_OK.
   *    - пробуем несовместимые типы, ожидая FPTA_ETYPE.
   *    - всё перечисленное проверяем для ordered и unordered индексов.
   *  2. Проверяем конвертацию и сравнение через компараторы индексов:
   *    - в цикле добавляем в "пробник" несколько значений с краю от
   *      диапазона машинного типа.
   *    - при добавлении в "пробник" каждое значение снабжается ожидаемым
   *      порядковым номером.
   *    - внутри пробника несколько упорядоченных контейнеров (std::map),
   *      для каждого допустимого класса индексов obverse/reverse/unordered,
   *      которые используют соответствующий индексный компаратор.
   *    - в конце "пробник" проверяет кол-во элементов в каждом проверяющим
   *      индекс контейнере, и корректность порядка элементов.
   *    - также "пробник" самостоятельно пытается добавить дубликаты.
   *    - этим проверяется как корректность формирования ключей, так и
   *      верность результата соответствующих индексных компараторов.
   */
  const auto ordered = fpta_column_shove(0, fptu_datetime, fpta_primary_unique);
  const auto unordered =
      fpta_column_shove(0, fptu_datetime, fpta_primary_unique_unordered);
  fpta_key key;

  EXPECT_EQ(FPTA_OK,
            value2key(ordered, fpta_value_datetime(fptu_now_coarse()), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(ordered, fpta_value_null(), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(ordered, fpta_value_uint(42), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(ordered, fpta_value_sint(42), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(ordered, fpta_value_float(42), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(ordered, fpta_value_cstr("42"), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(ordered, fpta_value_binary("42", 2), key));

  EXPECT_EQ(FPTA_OK,
            value2key(unordered, fpta_value_datetime(fptu_now_coarse()), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(unordered, fpta_value_null(), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(unordered, fpta_value_uint(42), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(unordered, fpta_value_sint(42), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(unordered, fpta_value_float(42), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(unordered, fpta_value_cstr("42"), key));
  EXPECT_EQ(FPTA_ETYPE, value2key(unordered, fpta_value_binary("42", 2), key));

  // проверяем через компараторы индексов
  probe_triplet<fptu_datetime> probe;
  for (auto i = 0; i < 42; i++) {
    fptu_time datetime;
    datetime.fixedpoint = UINT64_MAX - i;
    probe(fpta_value_datetime(datetime), 100 - i);
    datetime.fixedpoint = i;
    probe(fpta_value_datetime(datetime), i);
  }
  probe.check(42 * 2);
}

//----------------------------------------------------------------------------

template <fptu_type _type> struct glue {
  static constexpr fptu_type type = _type;
};

typedef ::testing::Types<glue<fptu_cstr>, glue<fptu_opaque>>
    VariableStringTypes;

typedef ::testing::Types<glue<fptu_96>, glue<fptu_cstr>, glue<fptu_opaque>,
                         glue<fptu_128>, glue<fptu_160>, glue<fptu_256>>
    AllStringTypes;

template <typename TypeParam>
struct Value2Key_AllString : public ::testing::Test {};
TYPED_TEST_CASE(Value2Key_AllString, AllStringTypes);

template <typename TypeParam>
struct Value2Key_VariableString : public ::testing::Test {};
TYPED_TEST_CASE(Value2Key_VariableString, VariableStringTypes);

template <fptu_type data_type, fpta_index_type index_type>
void expect_lt(const fpta_value &left, const fpta_value &right) {
  struct probe_key<data_type, index_type> kv_left(left);
  struct probe_key<data_type, index_type> kv_right(right);
  EXPECT_LT(kv_left, kv_right);
}

template <fptu_type data_type, fpta_index_type index_type>
void expect_gt(const fpta_value &left, const fpta_value &right) {
  struct probe_key<data_type, index_type> kv_left(left);
  struct probe_key<data_type, index_type> kv_right(right);
  EXPECT_GT(kv_left, kv_right);
}

template <fptu_type data_type, fpta_index_type index_type>
void expect_ne(const fpta_value &left, const fpta_value &right) {
  struct probe_key<data_type, index_type> kv_left(left);
  struct probe_key<data_type, index_type> kv_right(right);
  EXPECT_NE(kv_left, kv_right);
}

template <fptu_type data_type, fpta_index_type index_type>
void expect_eq(const fpta_value &left, const fpta_value &right) {
  struct probe_key<data_type, index_type> kv_left(left);
  struct probe_key<data_type, index_type> kv_right(right);
  EXPECT_EQ(kv_left, kv_right);
}

TYPED_TEST(Value2Key_AllString, basic) {
  /* Базовый тест преобразования строковых значений в "ключи индекса" для
   * всех "строковых" типов: fptu_cstr, fptu_opaque и fptu_96/128/160/256.
   * Важной составляющей теста является проверка сравнения ключей равной
   * длины индексными компараторами для obverse/reversed индексов.
   *
   * Сценарий:
   *  1. Проверяем конвертацию значений:
   *    - конвертируем допустимые значения fpta_value, ожидая FPTA_OK.
   *    - пробуем несовместимые типы, ожидая FPTA_ETYPE.
   *    - пробуем конвертировать значения вне диапазона, ожидая FPTA_EVALUE
   *      или FPTA_DATALEN_MISMATCH.
   *    - всё перечисленное проверяем для obverse/unordered/reverse индексов.
   *  2. Проверяем конвертацию и сравнение через компараторы индексов:
   *    - конвертируем и сравниваем "крайние" значения: все нули, все 0xFF,
   *      нулевая длина (если допустима).
   *    - для строк вариативной длины в цикле перебираются допустимые
   *      размеры ключей.
   *    - для проверки компараторов для каждого итерируемого размера ключа
   *      конвертируются и сравниваются пары значений, полученные движением
   *      "скользящего окна" по возрастающей последовательности байтов.
   *    - всё перечисленное выполняется для obverse/reversed индексов.
   *    - этим проверяется как корректность формирования ключей, так и
   *      верность результата соответствующих индексных компараторов.
   */
  constexpr fptu_type type = TypeParam::type;
  SCOPED_TRACE("type " + std::to_string(type));

  static const fpta_index_type index_cases[] = {
      /* clang-format off */
        fpta_primary_unique, fpta_primary_unique_unordered,
        fpta_primary_unique_reversed
      /* clang-format on */
  };

  constexpr bool is_string = (type == fptu_cstr);
  static const size_t keylen_min =
      ct_is_fixedsize(type) ? ct_elem_size(type) : 1;
  static const size_t keylen_max =
      ct_is_fixedsize(type) ? ct_elem_size(type) : (size_t)fpta_max_keylen;

#ifdef _MSC_VER /* FIXME: mustdie */
  uint8_t *const zeros = (uint8_t *)_alloca(keylen_max);
  uint8_t *const ones = (uint8_t *)_alloca(keylen_max);
  memset(zeros, is_string ? ' ' : 0, keylen_max);
  memset(ones, ~0u, keylen_max);
#else
  char zeros[keylen_max], ones[keylen_max];
  memset(zeros, is_string ? ' ' : 0, sizeof(zeros));
  memset(ones, ~0u, sizeof(ones));
#endif

  for (auto index : index_cases) {
    SCOPED_TRACE("index " + std::to_string(index));
    const auto shove = fpta_column_shove(0, type, index);
    fpta_key key;

    EXPECT_EQ(FPTA_ETYPE, value2key(shove, fpta_value_uint(42), key));
    EXPECT_EQ(FPTA_ETYPE, value2key(shove, fpta_value_sint(42), key));
    EXPECT_EQ(FPTA_ETYPE, value2key(shove, fpta_value_float(42), key));
    EXPECT_EQ(FPTA_ETYPE, value2key(shove, fpta_value_begin(), key));
    EXPECT_EQ(FPTA_ETYPE, value2key(shove, fpta_value_end(), key));
    EXPECT_EQ(FPTA_ETYPE, value2key(shove, fpta_value_null(), key));

    if (ct_is_fixedsize(type)) {
      EXPECT_EQ(
          FPTA_DATALEN_MISMATCH,
          value2key(shove, fpta_value_binary(nullptr, keylen_min - 1), key));
      EXPECT_EQ(
          FPTA_DATALEN_MISMATCH,
          value2key(shove, fpta_value_binary(nullptr, keylen_max + 1), key));
      EXPECT_EQ(FPTA_ETYPE, value2key(shove, fpta_value_cstr(nullptr), key));
      EXPECT_EQ(FPTA_ETYPE, value2key(shove, fpta_value_cstr("42"), key));
    } else if (is_string) {
      EXPECT_EQ(FPTA_OK, value2key(shove, fpta_value_cstr(nullptr), key));
      EXPECT_EQ(FPTA_ETYPE,
                value2key(shove, fpta_value_binary(nullptr, 0), key));
      auto hack = fpta_value_cstr(nullptr);
      hack.binary_length = 1;
      EXPECT_EQ(FPTA_EINVAL, value2key(shove, hack, key));
      EXPECT_EQ(FPTA_OK, value2key(shove, fpta_value_cstr("42"), key));
    } else {
      EXPECT_EQ(FPTA_ETYPE, value2key(shove, fpta_value_cstr(nullptr), key));
      EXPECT_EQ(FPTA_OK, value2key(shove, fpta_value_binary(nullptr, 0), key));
      EXPECT_EQ(FPTA_EINVAL,
                value2key(shove, fpta_value_binary(nullptr, 1), key));
      EXPECT_EQ(FPTA_ETYPE, value2key(shove, fpta_value_cstr("42"), key));
      EXPECT_EQ(FPTA_OK, value2key(shove, fpta_value_binary("42", 2), key));
    }
  }

  fpta_value value_left, value_right;
  const size_t keybuf_len = keylen_max + keylen_min;
#ifdef _MSC_VER /* FIXME: mustdie */
  uint8_t *const obverse = (uint8_t *)_alloca(keybuf_len);
  uint8_t *const reverse = (uint8_t *)_alloca(keybuf_len);
#else
  uint8_t obverse[keybuf_len], reverse[keybuf_len];
#endif
  for (size_t i = 0; i < keybuf_len; ++i) {
    obverse[i] = i + 1 + (is_string ? ' ' : 0);
    reverse[i] = keybuf_len - i + (is_string ? ' ' : 0);
  }
  ASSERT_TRUE(memcmp(zeros, obverse, keybuf_len) < 0);
  ASSERT_TRUE(memcmp(ones, obverse, keybuf_len) > 0);

  if (!ct_is_fixedsize(type)) {
    value_left = fpta_value_binstr<type>(nullptr, 0);
    value_right = fpta_value_binstr<type>(zeros, keylen_max);
    expect_lt<type, fpta_primary_unique>(value_left, value_right);
    expect_lt<type, fpta_primary_unique_reversed>(value_left, value_right);
    expect_ne<type, fpta_primary_unique_unordered>(value_left, value_right);
  }

  value_left = fpta_value_binstr<type>(zeros, keylen_max);
  value_right = fpta_value_binstr<type>(ones, keylen_max);
  expect_lt<type, fpta_primary_unique>(value_left, value_right);
  expect_lt<type, fpta_primary_unique_reversed>(value_left, value_right);
  expect_ne<type, fpta_primary_unique_unordered>(value_left, value_right);

  expect_eq<type, fpta_primary_unique>(value_left, value_left);
  expect_eq<type, fpta_primary_unique>(value_right, value_right);
  expect_eq<type, fpta_primary_unique_reversed>(value_left, value_left);
  expect_eq<type, fpta_primary_unique_reversed>(value_right, value_right);
  expect_eq<type, fpta_primary_unique_unordered>(value_left, value_left);
  expect_eq<type, fpta_primary_unique_unordered>(value_right, value_right);

  for (auto keylen = keylen_min; keylen <= keylen_max; ++keylen) {
    SCOPED_TRACE("keylen " + std::to_string(keylen));

    value_left = fpta_value_binstr<type>(zeros, keylen);
    value_right = fpta_value_binstr<type>(obverse, keylen);

    expect_lt<type, fpta_primary_unique>(value_left, value_right);
    expect_lt<type, fpta_primary_unique_reversed>(value_left, value_right);
    expect_ne<type, fpta_primary_unique_unordered>(value_left, value_right);

    value_left = fpta_value_binstr<type>(ones, keylen);
    expect_gt<type, fpta_primary_unique>(value_left, value_right);
    expect_gt<type, fpta_primary_unique_reversed>(value_left, value_right);
    expect_ne<type, fpta_primary_unique_unordered>(value_left, value_right);

    for (size_t offset = 1; offset < keybuf_len - keylen; ++offset) {
      SCOPED_TRACE("offset " + std::to_string(offset));

      value_left = fpta_value_binstr<type>(obverse, keylen);
      value_right = fpta_value_binstr<type>(obverse + offset, keylen);
      expect_lt<type, fpta_primary_unique>(value_left, value_right);
      expect_ne<type, fpta_primary_unique_unordered>(value_left, value_right);

      expect_eq<type, fpta_primary_unique>(value_left, value_left);
      expect_eq<type, fpta_primary_unique>(value_right, value_right);
      expect_eq<type, fpta_primary_unique_reversed>(value_left, value_left);
      expect_eq<type, fpta_primary_unique_reversed>(value_right, value_right);
      expect_eq<type, fpta_primary_unique_unordered>(value_left, value_left);
      expect_eq<type, fpta_primary_unique_unordered>(value_right, value_right);

      value_left = fpta_value_binstr<type>(reverse, keylen);
      value_right = fpta_value_binstr<type>(reverse + offset, keylen);
      expect_gt<type, fpta_primary_unique_reversed>(value_left, value_right);
      expect_ne<type, fpta_primary_unique_unordered>(value_left, value_right);

      expect_eq<type, fpta_primary_unique>(value_left, value_left);
      expect_eq<type, fpta_primary_unique>(value_right, value_right);
      expect_eq<type, fpta_primary_unique_reversed>(value_left, value_left);
      expect_eq<type, fpta_primary_unique_reversed>(value_right, value_right);
      expect_eq<type, fpta_primary_unique_unordered>(value_left, value_left);
      expect_eq<type, fpta_primary_unique_unordered>(value_right, value_right);
    }
  }
}

TYPED_TEST(Value2Key_AllString, normal_keys) {
  /* Тест преобразования не-длинных значений в "ключи индекса" для
   * всех "строковых" типов: fptu_cstr, fptu_opaque и fptu_96/128/160/256.
   * Также выполняется проверка сравнения полученных ключей через индексные
   * компараторы, в том числе разно-упорядоченные ключи разной длины.
   *
   * Сценарий:
   *  1. Тремя вложенными циклами добавляем в "пробник" достаточно большое
   *     ключевых значений.
   *      - string_keygen и три вложенные цикла обеспечивают перебор
   *        допустимых размеров ключа.
   *      - Важно: для строк вариативной длины обеспечивается генерация
   *        ключей в нелинейном порядке. Так чтобы часть, как более коротких
   *        ключей, так и более длинных, была как "больше", так и "меньше",
   *        соответственно более длинных и более коротких.
   *      - Для типов, допускающих нулевой размер, также добавляется ключ
   *        нулевой длины.
   *      - При добавлении в "пробник" каждое значение снабжается ожидаемым
   *        порядковым номером.
   *      - Также "пробник" самостоятельно пытается добавить дубликаты.
   *  2. Внутри пробника несколько упорядоченных контейнеров (std::map),
   *     для каждого допустимого класса индексов obverse/reverse/unordered,
   *     которые используют соответствующий индексный компаратор.
   *  3. В конце "пробник" проверяет кол-во элементов в каждом проверяющим
   *     индекс контейнере, и корректность порядка элементов.
   *
   *  В целом этим проверяется как корректность формирования ключей, так и
   *  верность результата соответствующих индексных компараторов для
   *  разно-упорядоченных ключей разного размера.
   */
  constexpr fptu_type type = TypeParam::type;
  constexpr bool is_string = (type == fptu_cstr);
  static const size_t keylen_min =
      ct_is_fixedsize(type) ? ct_elem_size(type) : 1;
  static const size_t keylen_max =
      ct_is_fixedsize(type) ? ct_elem_size(type) : (size_t)fpta_max_keylen;
  SCOPED_TRACE("type " + std::to_string(type));

#ifdef _MSC_VER /* FIXME: mustdie */
  uint8_t *const keybuf = (uint8_t *)_alloca(keylen_max);
#else
  uint8_t keybuf[keylen_max];
#endif
  probe_triplet<type> probe;

  if (!ct_is_fixedsize(type))
    probe(fpta_value_binstr<type>(nullptr, 0), -1);

  for (unsigned order_lopart = 0; order_lopart < 111;) {
    for (auto keylen = keylen_min; keylen <= keylen_max; ++keylen) {
      unsigned order_hipart = 0;
      do {
        unsigned order = order_lopart + order_hipart;
        bool key_is_too_short = string_keygen<is_string>(keylen, order, keybuf);
        if (key_is_too_short)
          break;

        probe(fpta_value_binstr<type>(keybuf, keylen), order);
        order_hipart += order_hipart / 3 + 12345;
      } while (order_hipart < INT_MAX / 2);
      ++order_lopart;
    }
  }
  probe.check();
}

//----------------------------------------------------------------------------

TYPED_TEST(Value2Key_VariableString, long_keys) {
  /* Тест преобразования ДЛИННЫХ строковых значений в "ключи индекса" для
   * fptu_cstr (C-строки) и fptu_opaque (бинарные строки). Также выполняется
   * проверка сравнения полученных ключей через индексные компараторы.
   *
   * Прежде всего тест проверят индексирование длинных строковых значений,
   * длина которых превышает fpta_max_keylen. Для таких значений libfpta
   * генерирует упорядоченные (ordered) индексные ключи следующим образом:
   *  - ключ составляется из двух частей: префикса и суффикса.
   *  - в первую часть (префикс) попадает начало исходной строки, размером
   *    fpta_max_keylen - 8 (размер суффикса).
   *  - во вторую часть (суффикс) попадает хэш-значение от хвоста исходной
   *    строки, который не поместился в префикс.
   *  - таким образом (это общепринятый подход) выполняется "сжатие" исходных
   *    длинных значений в ключи ограниченной длины.
   *  - для reversed индексов данные рассматриваются и записываются в обратном
   *    порядке.
   *
   * Соответственно, тест проверяет корректность всех описанных преобразований
   * и соответствующих сравнений, в том числе с "обычными" НЕ-длинными
   * ключами.
   *
   * Сценарий:
   *  1. Двумя вложенными циклами добавляем в "пробник" достаточно большое
   *     ключевых значений.
   *      - string_keygen и два вложенных цикла обеспечивают перебор размера
   *        исходных строк от половины fpta_max_keylen, до его многократного
   *        превышения.
   *      - Важно: обеспечивается генерация ключей в нелинейном порядке.
   *        Так чтобы часть, как более коротких строк, так и более длинных,
   *        была как "больше", так и "меньше", соответственно более длинных
   *        и более коротких.
   *      - Также добавляется строки нулевой длины и "максимальные" (из 0xFF).
   *      - При добавлении в "пробник" каждое значение снабжается ожидаемым
   *        порядковым номером.
   *      - Также "пробник" самостоятельно пытается добавить дубликаты.
   *  2. Внутри пробника несколько упорядоченных контейнеров (std::map),
   *     для каждого допустимого класса индексов obverse/reverse/unordered,
   *     которые используют соответствующий индексный компаратор.
   *  3. В конце "пробник" проверяет кол-во элементов в каждом проверяющим
   *     индекс контейнере, и корректность порядка элементов.
   *
   *  В целом этим проверяется как корректность формирования ключей, так и
   *  верность результата соответствующих индексных компараторов для
   *  разно-упорядоченных строк разного размера, в том числе длинных.
   */
  const size_t keylen_min = fpta_max_keylen / 2;
  const size_t keylen_max = fpta_max_keylen * 42;
  const size_t keylen_step = 11;

  constexpr fptu_type type = TypeParam::type;
  constexpr bool is_string = (type == fptu_cstr);
  SCOPED_TRACE("type " + std::to_string(type));

  uint8_t ones[keylen_max];
  memset(ones, ~0u, sizeof(ones));

  uint8_t keybuf[keylen_max];
  probe_triplet<type> probe;

  probe(fpta_value_binstr<type>(nullptr, 0), -1);
  probe(fpta_value_binstr<type>(ones, keylen_max), INT_MAX);

  unsigned order_lopart = 0;
  for (auto keylen = keylen_min; keylen <= keylen_max; keylen += keylen_step) {
    ++order_lopart;
    for (unsigned order_hipart = 0; order_hipart < INT_MAX / 2;) {
      order_hipart += order_hipart / 3 + 1234567;
      unsigned order = order_lopart + order_hipart + keylen;
      bool key_is_too_short = string_keygen<is_string>(keylen, order, keybuf);
      ASSERT_FALSE(key_is_too_short);
      probe(fpta_value_binstr<type>(keybuf, keylen), order);
    }
  }
  probe.check();
}

//----------------------------------------------------------------------------

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

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

/* test conformance for C mode */
#include <fast_positive/tables.h>

#ifdef _MSC_VER
#pragma warning(                                                               \
    disable : 4710 /* sprintf_s(char *const, const std::size_t, const char *const, ...): функция не является встроенной */)
#pragma warning(                                                               \
    disable : 4711 /* function 'fptu_init' selected for automatic inline expansion*/)
#pragma warning(push, 1)
#endif /* _MSC_VER (warnings) */

#include <math.h>
#include <stdio.h>

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#define print_value(comment, value)                                            \
  printf("%-24s = %7ld  // %s\n", #value, (long)value, comment)

#define print_mask(comment, value)                                             \
  printf("%-24s = 0x%-5lx  // %s\n", #value, (long)value, comment)

int main(int argc, char *argv[]) {
  (void)argc;
  (void)argv;

  printf("// основные ограничения и константы:\n");
  print_value("максимальное кол-во таблиц", fpta_tables_max);
  print_value("максимальное кол-во колонок", fptu_max_cols);
  print_value("максимальное кол-во индексов для одной таблице",
              fpta_max_indexes);
  print_value("максимальное суммарное кол-во таблиц и индексов", fpta_max_dbi);

  print_value("максимальная длина строки/записи в байтах", fpta_max_row_bytes);
  print_value("максимальная длина значения колонки в байтах",
              fpta_max_col_bytes);
  print_value("максимальное кол-во элементов в массиве", fpta_max_array_len);

  print_value("минимальная длина имени", fpta_name_len_min);
  print_value("максимальная длина имени", fpta_name_len_max);
  print_value("максимальная длина ключа (дополняется t1ha при превышении)",
              fpta_max_keylen);

  printf("// внутренние технические детали:\n");
  print_value("размер буфера для ключа", fpta_keybuf_len);

  print_value("ширина идентификатора в битах", fpta_id_bits);

  print_value("ширина типа колонки в битах", fpta_column_typeid_bits);
  print_value("сдвиг для получения типа колонки", fpta_column_typeid_shift);
  print_mask("маска для получения типа колонки", fpta_column_typeid_mask);

  print_value("ширина типа индекса в битах", fpta_column_index_bits);
  print_value("сдвиг для получения типа индекса", fpta_column_index_shift);
  print_mask("маска для получения типа индекса", fpta_column_index_mask);

  print_value("ширина хэша имени в битах", fpta_name_hash_bits);
  print_value("сдвиг для получения хэша имени", fpta_name_hash_shift);

  const double_t fpta_name_clash_probab = pow(2.0, -fpta_name_hash_bits / 2.0);
  printf("%-24s = %.2g  // %s\n", "fpta_name_clash_prob",
         fpta_name_clash_probab, "вероятность коллизии в именах");

  printf("\nless Windows, no Java, no Problems ;)\n");
  return 0;
}

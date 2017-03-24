/*
 * Copyright 2016-2017 libfptu authors: please see AUTHORS file.
 *
 * This file is part of libfptu, aka "Fast Positive Tuples".
 *
 * libfptu is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * libfptu is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with libfptu.  If not, see <http://www.gnu.org/licenses/>.
 */

/* test conformance for C mode */
#include <fast_positive/tuples.h>

#ifdef _MSC_VER
#pragma warning(disable : 4710) /* 'xyz': function not inlined */
#pragma warning(disable : 4711) /* function 'xyz' selected for                 \
                                   automatic inline expansion */
#endif                          /* windows mustdie */

#include <stdio.h>

static void print_value(const char *caption, const char *comment, long value) {
  printf("%-20s = %ld\t// %s\n", caption, value, comment);
}

static void print_mask(const char *caption, const char *comment, long value) {
  printf("%-20s = 0x%lx\t// %s\n", caption, value, comment);
}

int main(int argc, char *argv[]) {
  (void)argc;
  (void)argv;

  printf("// базовые лимиты и параметры:\n");
  print_value("fptu_bits", "ширина счетчиков", fptu_bits);
  print_value("fptu_unit_size", "размер одного юнита", fptu_unit_size);
  print_value("fptu_typeid_bits", "ширина типа в идентификаторе поля",
              fptu_typeid_bits);
  print_value("fptu_ct_reserve_bits", "резерв в идентификаторе поля",
              fptu_ct_reserve_bits);
  print_value("fptu_lx_bits", "резерв под флаги в заголовке кортежа",
              fptu_lx_bits);
  printf("\n");

  printf("// производные константы и параметры:\n");
  print_value("fptu_unit_shift", "log2(fptu_unit_size)", fptu_unit_shift);
  print_value("fptu_limit", "базовый лимит значений", fptu_limit);
  print_value("fptu_co_bits", "ширина тега-номера поля/колонки", fptu_co_bits);
  print_mask("fptu_ty_mask",
             "маска для получения типа из идентификатора поля/колонки",
             fptu_ty_mask);
  print_mask("fptu_fr_mask",
             "маска резервных битов в идентификаторе поля/колонки",
             fptu_fr_mask);
  print_value("fptu_co_shift",
              "сдвиг для получения тега-номера из идентификатора поля/колонки",
              fptu_co_shift);
  print_value("fptu_co_dead",
              "значение тега-номера для удаленных полей/колонок", fptu_co_dead);
  print_value("fptu_lt_bits", "кол-во бит доступных для хранения размера "
                              "массива дескрипторов полей",
              fptu_lt_bits);
  print_mask("fptu_lx_mask",
             "маска для выделения служебных бит из заголовка кортежа",
             fptu_lx_mask);
  print_mask("fptu_lt_mask", "маска для получения размера массива "
                             "дескрипторов из заголовка кортежа",
             fptu_lt_mask);
  printf("\n");

  printf("// итоговые ограничения:\n");
  print_value("fptu_max_tuple_bytes", "максимальный суммарный размер "
                                      "сериализованного представления "
                                      "кортежа",
              fptu_max_tuple_bytes);
  print_value("fptu_max_cols", "максимальный тег-номер поля/колонки",
              fptu_max_cols);
  print_value("fptu_max_fields",
              "максимальное кол-во полей/колонок в одном кортеже",
              fptu_max_fields);
  print_value("fptu_max_field_bytes", "максимальный размер поля/колонки",
              fptu_max_field_bytes);
  print_value("fptu_max_opaque_bytes",
              "максимальный размер произвольной последовательности байт",
              fptu_max_opaque_bytes);
  print_value("fptu_max_array", "максимальное кол-во элементов в массиве",
              fptu_max_array);
  printf("\n");

  printf("// максимальные размеры буферов:\n");
  print_value("fptu_buffer_enought",
              "буфер достаточного размера для любого кортежа",
              fptu_buffer_enought);
  print_value("fptu_buffer_limit",
              "предельный размер, превышение которого считается ошибкой",
              fptu_buffer_limit);

  printf("\nno Windows, no Java, no Problems ;)\n");
  return 0;
}

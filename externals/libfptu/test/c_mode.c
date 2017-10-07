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
#pragma warning(push, 1)
#endif /* _MSC_VER (warnings) */

#include <stdio.h>

#if defined(_WIN32) || defined(_WIN64) || defined(_WINDOWS)
#include <fcntl.h>
#include <io.h>
#include <windows.h>
#define print_value(comment, value)                                            \
  wprintf(L"%-20S = %ld\t// %S\n", #value, (long)value, comment)
#define print_mask(comment, value)                                             \
  wprintf(L"%-20S = 0x%lx\t// %S\n", #value, (long)value, comment)
#define print(text) wprintf(L"%S", text)
#else
#define print_value(comment, value)                                            \
  printf("%-20s = %ld\t// %s\n", #value, (long)value, comment)
#define print_mask(comment, value)                                             \
  printf("%-20s = 0x%lx\t// %s\n", #value, (long)value, comment)
#define print(text) puts(text)
#endif /* WINDOWS */

#ifdef _MSC_VER
#pragma warning(pop)
#endif

int main(int argc, char *argv[]) {
  (void)argc;
  (void)argv;
#if defined(_WIN32) || defined(_WIN64) || defined(_WINDOWS)
  SetConsoleOutputCP(CP_UTF8);
#endif /* WINDOWS */

  print("// базовые лимиты и параметры:\n");
  print_value("ширина счетчиков в битах", fptu_bits);
  print_value("размер одного юнита в байтах", fptu_unit_size);
  print_value("ширина типа в идентификаторе поля", fptu_typeid_bits);
  print_value("резерв в идентификаторе поля", fptu_ct_reserve_bits);
  print_value("резерв под флаги в заголовке кортежа", fptu_lx_bits);

  print("\n// производные константы и параметры:\n");
  print_value("log2(fptu_unit_size)", fptu_unit_shift);
  print_value("основной внутренний лимит", fptu_limit);
  print_value("ширина тега-номера поля/колонки", fptu_co_bits);
  print_mask("маска для получения типа из идентификатора поля/колонки",
             fptu_ty_mask);
  print_mask("маска резервных битов в идентификаторе поля/колонки",
             fptu_fr_mask);
  print_value("сдвиг для получения тега-номера из идентификатора поля/колонки",
              fptu_co_shift);
  print_value("значение тега-номера для удаленных полей/колонок", fptu_co_dead);
  print_value("кол-во бит доступных для хранения размера "
              "массива дескрипторов полей",
              fptu_lt_bits);
  print_mask("маска для выделения служебных бит из заголовка кортежа",
             fptu_lx_mask);
  print_mask("маска для получения размера массива "
             "дескрипторов из заголовка кортежа",
             fptu_lt_mask);

  print("\n// итоговые ограничения:\n");
  print_value("максимальный суммарный размер "
              "сериализованного представления "
              "кортежа",
              fptu_max_tuple_bytes);
  print_value("максимальный тег-номер поля/колонки", fptu_max_cols);
  print_value("максимальное кол-во полей/колонок в одном кортеже",
              fptu_max_fields);
  print_value("максимальный размер поля/колонки", fptu_max_field_bytes);
  print_value("максимальный размер произвольной последовательности байт",
              fptu_max_opaque_bytes);
  print_value("максимальное кол-во элементов в массиве", fptu_max_array_len);

  print("\n// максимальные размеры буферов:\n");
  print_value("буфер достаточного размера для любого кортежа",
              fptu_buffer_enough);
  print_value("предельный размер для резервирования, "
              "превышение которого считается ошибкой",
              fptu_buffer_limit);

#if HAVE_FPTU_VERSIONINFO
  printf("\n libfptu version %s: %s, %d.%d.%d.%d,\n\tcommit %s, tree %s\n",
         fptu_version.git.describe, fptu_version.git.datetime,
         fptu_version.major, fptu_version.minor, fptu_version.release,
         fptu_version.revision, fptu_version.git.commit, fptu_version.git.tree);
#endif /* HAVE_FPTU_VERSIONINFO */

  printf("\n libfptu build %s: %s, %s,\n\t%s,\n\t%s\n", fptu_build.datetime,
         fptu_build.target, fptu_build.compiler, fptu_build.cmake_options,
         fptu_build.compile_flags);

  print("\nless Windows, no Java, no Problems ;)\n");
  return 0;
}

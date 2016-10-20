/*
 * Copyright 2016 libfptu AUTHORS: please see AUTHORS file.
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
#include <stdio.h>

int main(int argc, char* argv[]) {
    (void) argc;
    (void) argv;

    printf ("// базовые лимиты и параметры:\n");
    printf("fpt_bits = %ld // ширина счетчиков\n", (long) fpt_bits);
    printf("fpt_unit_size = %ld // размер одного юнита\n", (long) fpt_unit_size);
    printf("fpt_typeid_bits = %ld // ширина типа в идентификаторе поля\n", (long) fpt_typeid_bits);
    printf("fpt_ct_reserve_bits = %ld // резерв в идентификаторе поля\n", (long) fpt_ct_reserve_bits);
    printf("fpt_lx_bits = %ld // резерв под флаги в заголовке кортежа\n", (long) fpt_lx_bits);
    printf("\n");

    // базовый лимит значений
    printf("fpt_limit = %ld\n", (long) fpt_limit);

    // ширина тега-номера поля/колонки
    printf("fpt_co_bits = %ld\n", (long) fpt_co_bits);
    // маска для получения типа из идентификатора поля/колонки
    printf("fpt_ty_mask  = 0x%lx\n", (long) fpt_ty_mask );
    // маска ресервных битов в идентификаторе поля/колонки
    printf("fpt_fr_mask = 0x%lx\n", (long) fpt_fr_mask);

    // сдвиг для получения тега-номера из идентификатора поля/колонки
    printf("fpt_co_shift = %ld\n", (long) fpt_co_shift);
    // значение тега-номера для удаленных полей/колонок
    printf("fpt_co_dead  = %ld\n", (long) fpt_co_dead);

    // кол-во бит доступных для хранения размера массива дескрипторов полей
    printf("fpt_lt_bits = %ld\n", (long) fpt_lt_bits);
    // маска для выделения служебных бит из заголовка кортежа
    printf("fpt_lx_mask = 0x%lx\n", (long) fpt_lx_mask);
    // маска для получения размера массива дескрипторов из заголовка кортежа
    printf("fpt_lt_mask = 0x%lx\n", (long) fpt_lt_mask);
    printf("\n");

    // максимальный суммарный размер сериализованного представления кортежа,
    printf("fpt_max_tuple_bytes = %ld\n", (long) fpt_max_tuple_bytes);
    // максимальный тег-номер поля/колонки
    printf("fpt_max_cols = %ld\n", (long) fpt_max_cols);
    // максимальное кол-во полей/колонок в одном кортеже
    printf("fpt_max_fields = %ld\n", (long) fpt_max_fields);
    // максимальный размер поля/колонки
    printf("fpt_max_field_bytes = %ld\n", (long) fpt_max_field_bytes);
    // максимальный размер произвольной последовательности байт
    printf("fpt_max_opaque_bytes = %ld\n", (long) fpt_max_opaque_bytes);
    // максимальное кол-во элементов в массиве,
    // так чтобы при любом базовом типе не превышались другие лимиты
    printf("fpt_max_array = %ld\n", (long) fpt_max_array);
    printf("\n");

    return 0;
}

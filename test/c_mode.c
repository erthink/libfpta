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

static
void print_value(const char* caption, const char* comment, long value) {
	printf("%-20s = %ld\t// %s\n", caption, value, comment);
}

static
void print_mask(const char* caption, const char* comment, long value) {
	printf("%-20s = 0x%lx\t// %s\n", caption, value, comment);
}

int main(int argc, char* argv[]) {
	(void) argc;
	(void) argv;

	printf ("// базовые лимиты и параметры:\n");
	print_value("fpt_bits", "ширина счетчиков", fpt_bits);
	print_value("fpt_unit_size", "размер одного юнита", fpt_unit_size);
	print_value("fpt_typeid_bits", "ширина типа в идентификаторе поля", fpt_typeid_bits);
	print_value("fpt_ct_reserve_bits", "резерв в идентификаторе поля", fpt_ct_reserve_bits);
	print_value("fpt_lx_bits", "резерв под флаги в заголовке кортежа", fpt_lx_bits);
	printf("\n");

	printf("// производные константы и параметры:\n");
	print_value("fpt_unit_shift", "log2(fpt_unit_size)", fpt_unit_shift);
	print_value("fpt_limit", "базовый лимит значений", fpt_limit);
	print_value("fpt_co_bits", "ширина тега-номера поля/колонки", fpt_co_bits);
	print_mask("fpt_ty_mask", "маска для получения типа из идентификатора поля/колонки", fpt_ty_mask );
	print_mask("fpt_fr_mask", "маска ресервных битов в идентификаторе поля/колонки", fpt_fr_mask);
	print_value("fpt_co_shift", "сдвиг для получения тега-номера из идентификатора поля/колонки", fpt_co_shift);
	print_value("fpt_co_dead", "значение тега-номера для удаленных полей/колонок", fpt_co_dead);
	print_value("fpt_lt_bits", "кол-во бит доступных для хранения размера массива дескрипторов полей", fpt_lt_bits);
	print_mask("fpt_lx_mask", "маска для выделения служебных бит из заголовка кортежа", fpt_lx_mask);
	print_mask("fpt_lt_mask", "маска для получения размера массива дескрипторов из заголовка кортежа", fpt_lt_mask);
	printf("\n");

	printf("// итоговые ограничения:\n");
	print_value("fpt_max_tuple_bytes", "максимальный суммарный размер сериализованного представления кортежа", fpt_max_tuple_bytes);
	print_value("fpt_max_cols", "максимальный тег-номер поля/колонки", fpt_max_cols);
	print_value("fpt_max_fields", "максимальное кол-во полей/колонок в одном кортеже", fpt_max_fields);
	print_value("fpt_max_field_bytes", "максимальный размер поля/колонки", fpt_max_field_bytes);
	print_value("fpt_max_opaque_bytes", "максимальный размер произвольной последовательности байт", fpt_max_opaque_bytes);
	print_value("fpt_max_array", "максимальное кол-во элементов в массиве", fpt_max_array);
	printf("\n");

	printf("// максимальные размеры буферов:\n");
	print_value("fpt_buffer_enought", "буфер достаточного размера для любого кортежа", fpt_buffer_enought);
	print_value("fpt_buffer_limit", "предельный размер, превышение которого считается ошибкой", fpt_buffer_limit);
	printf("\n");

	return 0;
}

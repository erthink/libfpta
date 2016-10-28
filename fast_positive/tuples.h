/*
 * Copyright 2016 libfptu authors: please see AUTHORS file.
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

/*
 * libfptu = { Fast Positive Tuples, aka Позитивные Кортежи }
 * Please see README.md
 */

#pragma once
#ifndef FAST_POSITIVE_TUPLES_H
#define FAST_POSITIVE_TUPLES_H

#include "fast_positive/defs.h"

#include <sys/uio.h> // for struct iovec

#ifdef __cplusplus
extern "C" {
#endif

typedef union fpt_varlen {
	struct __packed {
		uint16_t brutto;
		union {
			uint16_t opaque_bytes;
			uint16_t array_length;
			uint16_t tuple_items;
		};
	};
	uint32_t flat;
} fpt_varlen;

typedef union fpt_field {
	struct __packed {
		uint16_t ct;
		uint16_t offset;
	};
	uint32_t header;
	uint32_t body[1];
} fpt_field;

typedef union fpt_unit {
	fpt_field field;
	fpt_varlen varlen;
	uint32_t data;
} fpt_unit;

typedef union fpt_ro {
	struct {
		const fpt_unit *units;
		size_t total_bytes;
	};
	struct iovec sys;
} fpt_ro;

typedef struct fpt_rw {
	unsigned head;  ///< Индекс дозаписи дескрипторов, растет к началу буфера, указывает на первый занятый элемент.
	unsigned tail;  ///< Индекс для дозаписи данных, растет к концу буфера, указываент на первый не занятый элемент.
	unsigned junk;  ///< Счетчик мусорных 32-битных элементов, которые образовались при удалении/обновлении.
	unsigned pivot; ///< Индекс опорной точки, от которой растут "голова" и "хвоcт", указывает на терминатор заголовка.
	unsigned end;   ///< Конец выделенного буфера, т.е. units[end] не наше.
	fpt_unit units[1];
} fpt_rw;

enum fpt_bits {
	// базовые лимиты и параметры
	fpt_bits = 16,           // ширина счетчиков
	fpt_unit_size = 4,       // размер одного юнита
	fpt_typeid_bits = 5,     // ширина типа в идентификаторе поля
	fpt_ct_reserve_bits = 1, // резерв в идентификаторе поля
	// количество служебных (зарезервированных) бит в заголовке кортежа,
	// для признаков сортированности и отсутствия повторяющихся полей
	fpt_lx_bits = 2,

	// производные константы и параметры
	// log2(fpt_unit_size)
	fpt_unit_shift = 2,

	// базовый лимит значений
	fpt_limit = ((size_t) 1 << fpt_bits) - 1,
	// максимальный суммарный размер сериализованного представления кортежа,
	fpt_max_tuple_bytes = fpt_limit * fpt_unit_size,

	// ширина тега-номера поля/колонки
	fpt_co_bits = fpt_bits - fpt_typeid_bits - fpt_ct_reserve_bits,
	// маска для получения типа из идентификатора поля/колонки
	fpt_ty_mask  = ((size_t) 1 << fpt_typeid_bits) - 1,
	// маска ресервных битов в идентификаторе поля/колонки
	fpt_fr_mask = (((size_t) 1 << fpt_ct_reserve_bits) - 1) << fpt_typeid_bits,

	// сдвиг для получения тега-номера из идентификатора поля/колонки
	fpt_co_shift = fpt_typeid_bits + fpt_ct_reserve_bits,
	// значение тега-номера для удаленных полей/колонок
	fpt_co_dead  = ((size_t) 1 << fpt_co_bits) - 1,
	// максимальный тег-номер поля/колонки
	fpt_max_cols = fpt_co_dead - 1,

	// кол-во бит доступных для хранения размера массива дескрипторов полей
	fpt_lt_bits = fpt_bits - fpt_lx_bits,
	// маска для выделения служебных бит из заголовка кортежа
	fpt_lx_mask = (((size_t) 1 << fpt_lx_bits) - 1) << fpt_lt_bits,
	// маска для получения размера массива дескрипторов из заголовка кортежа
	fpt_lt_mask = ((size_t) 1 << fpt_lt_bits) - 1,
	// максимальное кол-во полей/колонок в одном кортеже
	fpt_max_fields = fpt_lt_mask,

	// максимальный размер поля/колонки
	fpt_max_field_bytes = fpt_limit,
	// максимальный размер произвольной последовательности байт
	fpt_max_opaque_bytes = fpt_max_field_bytes - fpt_unit_size,
	// максимальное кол-во элементов в массиве,
	// так чтобы при любом базовом типе не превышались другие лимиты
	fpt_max_array = fpt_max_opaque_bytes / 32,
	// буфер достаточного размера для любого кортежа
	fpt_buffer_enought = sizeof(fpt_rw)
		+ fpt_max_tuple_bytes + fpt_max_fields * fpt_unit_size,
	// предельный размер, превышение которого считается ошибкой
	fpt_buffer_limit = fpt_max_tuple_bytes * 2
};

enum fpt_type {
	// fixed length, without ex-data (descriptor only)
	fpt_null     =  0,
	fpt_uint16   =  1,

	// fixed length with ex-data (at least 4 byte after the pivot)
	fpt_int32    =  2,
	fpt_uint32   =  3,
	fpt_fp32     =  4,

	fpt_int64    =  5,
	fpt_uint64   =  6,
	fpt_fp64     =  7,

	fpt_96       =  8, // opaque 12-bytes.
	fpt_128      =  9, // opaque 16-bytes (uuid, ipv6, etc).
	fpt_160      = 10, // opaque 20-bytes (sha1).
	fpt_192      = 11, // opaque 24-bytes
	fpt_256      = 12, // opaque 32-bytes (sha256).

	// variable length, e.g. length and payload inside ex-data
	fpt_string   = 13, // utf-8 с-string, zero terminated

	// with additional length
	fpt_opaque   = 14, // opaque octet string
	fpt_nested   = 15, // nested tuple
	fpt_farray   = 16, // flag

	// pseudo types for lookup and filtering
	fpt_filter   = 1 << (fpt_null | fpt_farray),
	fpt_any      = -1,       // match any type
	fpt_any_int  = fpt_filter
		| (1 << fpt_int32)
		| (1 << fpt_int64),  // match int32/int64
	fpt_any_uint = fpt_filter
		| (1 << fpt_uint32)
		| (1 << fpt_uint64), // match uin32/uin64
	fpt_any_fp   = fpt_filter
		| (1 << fpt_fp32)
		| (1 << fpt_fp64),   // match fp32/fp64

	// aliases
	fpt_16       = fpt_uint16,
	fpt_32       = fpt_uint32,
	fpt_64       = fpt_uint64,
	fpt_bool     = fpt_uint16,
	fpt_enum     = fpt_uint16,
	fpt_char     = fpt_uint16,
	fpt_wchar    = fpt_uint16,
	fpt_ipv4     = fpt_uint32,
	fpt_uuid     = fpt_128,
	fpt_ipv6     = fpt_128,
	fpt_md5      = fpt_128,
	fpt_sha1     = fpt_160,
	fpt_sha256   = fpt_256,
	fpt_wstring  = fpt_opaque
};

//----------------------------------------------------------------------

/* Возвращает минимальный размер буфера, который необходим для размещения
 * кортежа с указанным кол-вом полей и данных. */
size_t fpt_space(size_t items, size_t data_bytes);

/* Инициализирует в буфере кортеж, резервируя (отступая) место достаточное
 * для добавления до items_limit полей. Оставшееся место будет
 * использоваться для размещения данных.
 *
 * Возвращает указатель на созданный в буфере объект, либо nullptr, если
 * заданы неверные параметры или размер буфера недостаточен. */
fpt_rw* fpt_init(void* buffer_space, size_t buffer_bytes, size_t items_limit);

/* Выделяет через malloc() и инициализирует кортеж достаточный для
 * размещения заданного кол-ва полей и данных.
 *
 * Возвращает адрес объекта, который необходимо free() по окончании
 * использования. Либо nullptr при неверных параметрах или нехватке памяти. */
fpt_rw* fpt_alloc(size_t items_limit, size_t data_bytes);

/* Возвращает кол-во свободных слотов для добавления дескрипторов
 * полей в заголовок кортежа. */
size_t fpt_space4items(const fpt_rw* pt);

/* Возвращает остаток места доступного для размещения данных. */
size_t fpt_space4data(const fpt_rw* pt);

/* Возвращает объем мусора в байтах, который станет доступным для
 * добавления полей и данных после fpt_shrink(). */
size_t fpt_junkspace(const fpt_rw* pt);

/* Проверяет сериализованную форму кортежа на корректность.
 *
 * Возвращает nullptr если ошибок не обнаружено, либо указатель на константную
 * строку с краткой информацией о проблеме (нарушенное условие). */
const char* fpt_check_ro(fpt_ro ro);

/* Проверяет модифицируемую форму кортежа на корректность.
 *
 * Возвращает nullptr если ошибок не обнаружено, либо указатель на константную
 * строку с краткой информацией о проблеме (нарушенное условие). */
const char* fpt_check(fpt_rw *pt);

/* Возвращает сериализованную форму кортежа, которая находится внутри
 * модифицируемой. Дефрагментация не выполняется, поэтому сериализованная
 * форма может содержать лишний мусор, см fpt_junkspace().
 *
 * Возвращаемый результат валиден до изменения или разрушения исходной
 * модифицируемой формы кортежа. */
fpt_ro fpt_take_noshrink(fpt_rw* pt);

/* Строит в указанном буфере модифицируемую форму кортежа из сериализованной.
 * Проверка корректности данных в сериализованной форме не производится.
 * Сериализованная форма не модифицируется и не требуется после возврата из
 * функции.
 * Аргумент more_items резервирует кол-во полей, которые можно будет добавить
 * в создаваемую модифицируемую форму.
 *
 * Возвращает указатель на созданный в буфере объект, либо nullptr, если
 * заданы неверные параметры или размер буфера недостаточен. */
fpt_rw* fpt_fetch(fpt_ro ro, void* buffer_space, size_t buffer_bytes,
	unsigned more_items);

/* Проверяет содержимое сериализованной формы на корректность. Одновременно
 * возвращает размер буфера, который потребуется для модифицируемой формы,
 * с учетом добавляемых more_items полей и more_payload данных.
 *
 * При неверных параметрах или некорректных данных возвращает 0 и записывает
 * в error информацию об ошибке (указатель на константную строку
 * с краткой информацией о проблеме). */
size_t fpt_check_and_get_buffer_size(fpt_ro ro, unsigned more_items,
	unsigned more_payload, const char** error);

/* Производит дефрагментацию модифицируемой формы кортежа. */
void fpt_shrink(fpt_rw* pt);

/* Возвращает сериализованную форму кортежа, которая находится внутри
 * модифицируемой. При необходимости автоматически производится дефрагментация.
 * Возвращаемый результат валиден до изменения или разрушения исходной
 * модифицируемой формы кортежа. */
static __inline fpt_ro fpt_take(fpt_rw* pt) {
	if (pt->junk)
		fpt_shrink(pt);
	return fpt_take_noshrink(pt);
}

/* Если в аргументе type_or_filter взведен бит fpt_filter,
 * то type_or_filter интерпретируется как битовая маска типов.
 * Соответственно, будут удалены все поля с заданным column и попадающие
 * в маску типов. Например, если type_or_filter равен fpt_any_fp,
 * то удаляются все fpt_fp32 и fpt_fp64.
 *
 * Возвращается кол-во удаленных полей (больше либо равно нулю),
 * либо отрицательный код ошибки. */
int fpt_erase(fpt_rw* pt, unsigned column, int type_or_filter);
void fpt_erase_field(fpt_rw* pt, fpt_field *pf);

//----------------------------------------------------------------------

// Вставка или обновление существующего поля (первого найденного для коллекций).
int fpt_upsert_null(fpt_rw* pt, unsigned column);
int fpt_upsert_uint16(fpt_rw* pt, unsigned column, unsigned value);
int fpt_upsert_int32(fpt_rw* pt, unsigned column, int32_t value);
int fpt_upsert_uint32(fpt_rw* pt, unsigned column, uint32_t value);
int fpt_upsert_int64(fpt_rw* pt, unsigned column, int64_t value);
int fpt_upsert_uint64(fpt_rw* pt, unsigned column, uint64_t value);
int fpt_upsert_fp64(fpt_rw* pt, unsigned column, double value);
int fpt_upsert_fp32(fpt_rw* pt, unsigned column, float value);

int fpt_upsert_96(fpt_rw* pt, unsigned column, const void* data);
int fpt_upsert_128(fpt_rw* pt, unsigned column, const void* data);
int fpt_upsert_160(fpt_rw* pt, unsigned column, const void* data);
int fpt_upsert_192(fpt_rw* pt, unsigned column, const void* data);
int fpt_upsert_256(fpt_rw* pt, unsigned column, const void* data);

int fpt_upsert_cstr(fpt_rw* pt, unsigned column, const char* value);
int fpt_upsert_opaque(fpt_rw* pt, unsigned column, const void* value, size_t bytes);
int fpt_upsert_opaque_iov(fpt_rw* pt, unsigned column, const struct iovec value);
// TODO
//int fpt_upsert_nested(fpt_rw* pt, unsigned column, fpt_ro ro);

// TODO
//int fpt_upsert_array_uint16(fpt_rw* pt, unsigned ct, size_t array_length, const uint16_t* array_data);
//int fpt_upsert_array_int32(fpt_rw* pt, unsigned ct, size_t array_length, const int32_t* array_data);
//int fpt_upsert_array_uint32(fpt_rw* pt, unsigned ct, size_t array_length, const uint32_t* array_data);
//int fpt_upsert_array_int64(fpt_rw* pt, unsigned ct, size_t array_length, const int64_t* array_data);
//int fpt_upsert_array_uint64(fpt_rw* pt, unsigned ct, size_t array_length, const uint64_t* array_data);
//int fpt_upsert_array_str(fpt_rw* pt, unsigned ct, size_t array_length, const char* array_data[]);
//int fpt_upsert_array_nested(fpt_rw* pt, unsigned ct, size_t array_length, const char* array_data[]);

//----------------------------------------------------------------------

// Добавление ещё одного поля, для поддержки коллекций.
int fpt_insert_uint16(fpt_rw* pt, unsigned column, unsigned value);
int fpt_insert_int32(fpt_rw* pt, unsigned column, int32_t value);
int fpt_insert_uint32(fpt_rw* pt, unsigned column, uint32_t value);
int fpt_insert_int64(fpt_rw* pt, unsigned column, int64_t value);
int fpt_insert_uint64(fpt_rw* pt, unsigned column, uint64_t value);
int fpt_insert_fp64(fpt_rw* pt, unsigned column, double value);
int fpt_insert_fp32(fpt_rw* pt, unsigned column, float value);

int fpt_insert_96(fpt_rw* pt, unsigned column, const void* data);
int fpt_insert_128(fpt_rw* pt, unsigned column, const void* data);
int fpt_insert_160(fpt_rw* pt, unsigned column, const void* data);
int fpt_insert_192(fpt_rw* pt, unsigned column, const void* data);
int fpt_insert_256(fpt_rw* pt, unsigned column, const void* data);

int fpt_insert_cstr(fpt_rw* pt, unsigned column, const char* value);
int fpt_insert_opaque(fpt_rw* pt, unsigned column, const void* value, size_t bytes);
int fpt_insert_opaque_iov(fpt_rw* pt, unsigned column, const struct iovec value);
// TODO
//int fpt_insert_nested(fpt_rw* pt, unsigned column, fpt_ro ro);

// TODO
//int fpt_insert_array_uint16(fpt_rw* pt, unsigned ct, size_t array_length, const uint16_t* array_data);
//int fpt_insert_array_int32(fpt_rw* pt, unsigned ct, size_t array_length, const int32_t* array_data);
//int fpt_insert_array_uint32(fpt_rw* pt, unsigned ct, size_t array_length, const uint32_t* array_data);
//int fpt_insert_array_int64(fpt_rw* pt, unsigned ct, size_t array_length, const int64_t* array_data);
//int fpt_insert_array_uint64(fpt_rw* pt, unsigned ct, size_t array_length, const uint64_t* array_data);
//int fpt_insert_array_str(fpt_rw* pt, unsigned ct, size_t array_length, const char* array_data[]);

//----------------------------------------------------------------------

// Обновление существующего поля (первого найденного для коллекций).
int fpt_update_uint16(fpt_rw* pt, unsigned column, unsigned value);
int fpt_update_int32(fpt_rw* pt, unsigned column, int32_t value);
int fpt_update_uint32(fpt_rw* pt, unsigned column, uint32_t value);
int fpt_update_int64(fpt_rw* pt, unsigned column, int64_t value);
int fpt_update_uint64(fpt_rw* pt, unsigned column, uint64_t value);
int fpt_update_fp64(fpt_rw* pt, unsigned column, double value);
int fpt_update_fp32(fpt_rw* pt, unsigned column, float value);

int fpt_update_96(fpt_rw* pt, unsigned column, const void* data);
int fpt_update_128(fpt_rw* pt, unsigned column, const void* data);
int fpt_update_160(fpt_rw* pt, unsigned column, const void* data);
int fpt_update_192(fpt_rw* pt, unsigned column, const void* data);
int fpt_update_256(fpt_rw* pt, unsigned column, const void* data);

int fpt_update_cstr(fpt_rw* pt, unsigned column, const char* value);
int fpt_update_opaque(fpt_rw* pt, unsigned column, const void* value, size_t bytes);
int fpt_update_opaque_iov(fpt_rw* pt, unsigned column, const struct iovec value);
// TODO
//int fpt_update_nested(fpt_rw* pt, unsigned column, fpt_ro ro);

// TODO
//int fpt_update_array_uint16(fpt_rw* pt, unsigned ct, size_t array_length, const uint16_t* array_data);
//int fpt_update_array_int32(fpt_rw* pt, unsigned ct, size_t array_length, const int32_t* array_data);
//int fpt_update_array_uint32(fpt_rw* pt, unsigned ct, size_t array_length, const uint32_t* array_data);
//int fpt_update_array_int64(fpt_rw* pt, unsigned ct, size_t array_length, const int64_t* array_data);
//int fpt_update_array_uint64(fpt_rw* pt, unsigned ct, size_t array_length, const uint64_t* array_data);
//int fpt_update_array_str(fpt_rw* pt, unsigned ct, size_t array_length, const char* array_data[]);

//----------------------------------------------------------------------

/* Возвращает первое поле попадающее под критерий выбора, либо nullptr.
 * Семантика type_or_filter указана в описании fpt_erase(). */
const fpt_field* fpt_lookup_ro(fpt_ro ro, unsigned column, int type_or_filter);
fpt_field* fpt_lookup(fpt_rw* pt, unsigned column, int type_or_filter);

/* Возвращает "итераторы" по кортежу, в виде указателей.
 * Гарантируется что begin меньше, либо равно end.
 * В возвращаемом диапазоне могут буть удаленные поля,
 * для которых fpt_field_column() возвращает отрицательное значение. */
const fpt_field* fpt_begin_ro(fpt_ro ro);
const fpt_field* fpt_end_ro(fpt_ro ro);
const fpt_field* fpt_begin(const fpt_rw* pt);
const fpt_field* fpt_end(const fpt_rw* pt);

/* Итерация полей кортежа с заданным условие отбора, при этом
 * удаленные поля пропускаются.
 * Семантика type_or_filter указана в описании fpt_erase(). */
const fpt_field* fpt_first(const fpt_field* begin, const fpt_field* end, unsigned column, int type_or_filter);
const fpt_field* fpt_next(const fpt_field* from, const fpt_field* end, unsigned column, int type_or_filter);

/* Итерация полей кортежа с заданным внешним фильтром, при этом
 * удаленные поля пропускаются.
 * Семантика type_or_filter указана в описании fpt_erase(). */
typedef bool fpt_field_filter(const fpt_field*, void *context, void *param);
const fpt_field* fpt_first_ex(const fpt_field* begin, const fpt_field* end,
							  fpt_field_filter filter, void* context, void *param);
const fpt_field* fpt_next_ex(const fpt_field* begin, const fpt_field* end,
							  fpt_field_filter filter, void* context, void *param);

size_t fpt_field_count(const fpt_rw* pt, unsigned column, int type_or_filter);
size_t fpt_field_count_ro(fpt_ro ro, unsigned column, int type_or_filter);

size_t fpt_field_count_ex(const fpt_rw* pt, fpt_field_filter filter, void* context, void *param);
size_t fpt_field_count_ro_ex(fpt_ro ro, fpt_field_filter filter, void* context, void *param);

int fpt_field_type(const fpt_field* pf);
int fpt_field_column(const fpt_field* pf);

uint16_t fpt_field_uint16(const fpt_field* pf);
int32_t fpt_field_int32(const fpt_field* pf);
uint32_t fpt_field_uint32(const fpt_field* pf);
int64_t fpt_field_int64(const fpt_field* pf);
uint64_t fpt_field_uint64(const fpt_field* pf);
double fpt_field_fp64(const fpt_field* pf);
float fpt_field_fp32(const fpt_field* pf);
const uint8_t* fpt_field_96(const fpt_field* pf);
const uint8_t* fpt_field_128(const fpt_field* pf);
const uint8_t* fpt_field_160(const fpt_field* pf);
const uint8_t* fpt_field_192(const fpt_field* pf);
const uint8_t* fpt_field_256(const fpt_field* pf);
const char* fpt_field_cstr(const fpt_field* pf);
struct iovec fpt_field_opaque(const fpt_field* pf);
fpt_ro fpt_field_nested(const fpt_field* pf);

uint16_t fpt_get_uint16(fpt_ro ro, unsigned column, int *error);
int32_t fpt_get_int32(fpt_ro ro, unsigned column, int *error);
uint32_t fpt_get_uint32(fpt_ro ro, unsigned column, int *error);
int64_t fpt_get_int64(fpt_ro ro, unsigned column, int *error);
uint64_t fpt_get_uint64(fpt_ro ro, unsigned column, int *error);
double fpt_get_fp64(fpt_ro ro, unsigned column, int *error);
float fpt_get_fp32(fpt_ro ro, unsigned column, int *error);
const uint8_t* fpt_get_96(fpt_ro ro, unsigned column, int *error);
const uint8_t* fpt_get_128(fpt_ro ro, unsigned column, int *error);
const uint8_t* fpt_get_160(fpt_ro ro, unsigned column, int *error);
const uint8_t* fpt_get_192(fpt_ro ro, unsigned column, int *error);
const uint8_t* fpt_get_256(fpt_ro ro, unsigned column, int *error);
const char* fpt_get_cstr(fpt_ro ro, unsigned column, int *error);
struct iovec fpt_get_opaque(fpt_ro ro, unsigned column, int *error);
fpt_ro fpt_get_nested(fpt_ro ro, unsigned column, int *error);

//TODO: fpt_field_array(), fpt_get_array()
typedef struct fpt_array {
	size_t size;
	union {
		uint16_t    uint16[2];
		int32_t     int32[1];
		uint32_t    uint32[1];
		int64_t     int64[1];
		uint64_t    uint64[1];
		double      fp64[1];
		float       fp32[1];
		const char  *cstr[1];
		fpt_ro   nested[1];
		struct iovec opaque[1];
	};
} fpt_array;

//----------------------------------------------------------------------

typedef enum fpt_cmp {
	fpt_ic   = 1, // incomparable
	fpt_eq   = 2, // left == right
	fpt_lt   = 4, // left < right
	fpt_gt   = 8, // left > right
	fpt_ne   = fpt_lt | fpt_gt | fpt_ic, // left != right
	fpt_le   = fpt_lt | fpt_eq,          // left <= right
	fpt_ge   = fpt_gt | fpt_eq,          // left >= right
} fpt_cmp;

int fpt_cmp_96(fpt_ro ro, unsigned column, const uint8_t* value);
int fpt_cmp_128(fpt_ro ro, unsigned column, const uint8_t* value);
int fpt_cmp_160(fpt_ro ro, unsigned column, const uint8_t* value);
int fpt_cmp_192(fpt_ro ro, unsigned column, const uint8_t* value);
int fpt_cmp_256(fpt_ro ro, unsigned column, const uint8_t* value);
int fpt_cmp_opaque(fpt_ro ro, unsigned column, const void* value, size_t bytes);
int fpt_cmp_opaque_iov(fpt_ro ro, unsigned column, const struct iovec value);

//----------------------------------------------------------------------

enum fpt_error {
	fpt_ok       = 0,
	fpt_noent    = -1,
	fpt_einval   = -2,
	fpt_enospc   = -3,
	fpt_etm      = -4,
};

#ifdef __cplusplus
}
#endif

#endif /* FAST_POSITIVE_TUPLES_H */

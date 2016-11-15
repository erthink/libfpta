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

typedef union fptu_varlen {
    struct __packed {
        uint16_t brutto;
        union {
            uint16_t opaque_bytes;
            uint16_t array_length;
            uint16_t tuple_items;
        };
    };
    uint32_t flat;
} fptu_varlen;

typedef union fptu_field {
    struct __packed {
        uint16_t ct;
        uint16_t offset;
    };
    uint32_t header;
    uint32_t body[1];
} fptu_field;

typedef union fptu_unit {
    fptu_field field;
    fptu_varlen varlen;
    uint32_t data;
} fptu_unit;

typedef union fptu_ro {
    struct {
        const fptu_unit *units;
        size_t total_bytes;
    };
    struct iovec sys;
} fptu_ro;

typedef struct fptu_rw {
    unsigned head;  /* Индекс дозаписи дескрипторов, растет к началу буфера,
                       указывает на первый занятый элемент. */
    unsigned tail;  /* Индекс для дозаписи данных, растет к концу буфера,
                       указываент на первый не занятый элемент. */
    unsigned junk;  /* Счетчик мусорных 32-битных элементов, которые
                       образовались при удалении/обновлении. */
    unsigned pivot; /* Индекс опорной точки, от которой растут "голова" и
                       "хвоcт", указывает на терминатор заголовка. */
    unsigned end;   /* Конец выделенного буфера, т.е. units[end] не наше. */
    fptu_unit units[1];
} fptu_rw;

enum fptu_bits {
    // базовые лимиты и параметры
    fptu_bits = 16,     // ширина счетчиков
    fptu_unit_size = 4, // размер одного юнита
    fptu_typeid_bits = 5, // ширина типа в идентификаторе поля
    fptu_ct_reserve_bits = 1, // резерв в идентификаторе поля
    // количество служебных (зарезервированных) бит в заголовке кортежа,
    // для признаков сортированности и отсутствия повторяющихся полей
    fptu_lx_bits = 2,

    // производные константы и параметры
    // log2(fptu_unit_size)
    fptu_unit_shift = 2,

    // базовый лимит значений
    fptu_limit = ((size_t)1 << fptu_bits) - 1,
    // максимальный суммарный размер сериализованного представления кортежа,
    fptu_max_tuple_bytes = fptu_limit * fptu_unit_size,

    // ширина тега-номера поля/колонки
    fptu_co_bits = fptu_bits - fptu_typeid_bits - fptu_ct_reserve_bits,
    // маска для получения типа из идентификатора поля/колонки
    fptu_ty_mask = ((size_t)1 << fptu_typeid_bits) - 1,
    // маска ресервных битов в идентификаторе поля/колонки
    fptu_fr_mask = (((size_t)1 << fptu_ct_reserve_bits) - 1)
                   << fptu_typeid_bits,

    // сдвиг для получения тега-номера из идентификатора поля/колонки
    fptu_co_shift = fptu_typeid_bits + fptu_ct_reserve_bits,
    // значение тега-номера для удаленных полей/колонок
    fptu_co_dead = ((size_t)1 << fptu_co_bits) - 1,
    // максимальный тег-номер поля/колонки
    fptu_max_cols = fptu_co_dead - 1,

    // кол-во бит доступных для хранения размера массива дескрипторов полей
    fptu_lt_bits = fptu_bits - fptu_lx_bits,
    // маска для выделения служебных бит из заголовка кортежа
    fptu_lx_mask = (((size_t)1 << fptu_lx_bits) - 1) << fptu_lt_bits,
    // маска для получения размера массива дескрипторов из заголовка кортежа
    fptu_lt_mask = ((size_t)1 << fptu_lt_bits) - 1,
    // максимальное кол-во полей/колонок в одном кортеже
    fptu_max_fields = fptu_lt_mask,

    // максимальный размер поля/колонки
    fptu_max_field_bytes = fptu_limit,
    // максимальный размер произвольной последовательности байт
    fptu_max_opaque_bytes = fptu_max_field_bytes - fptu_unit_size,
    // максимальное кол-во элементов в массиве,
    // так чтобы при любом базовом типе не превышались другие лимиты
    fptu_max_array = fptu_max_opaque_bytes / 32,
    // буфер достаточного размера для любого кортежа
    fptu_buffer_enought = sizeof(fptu_rw) + fptu_max_tuple_bytes +
                          fptu_max_fields * fptu_unit_size,
    // предельный размер, превышение которого считается ошибкой
    fptu_buffer_limit = fptu_max_tuple_bytes * 2
};

enum fptu_type {
    // fixed length, without ex-data (descriptor only)
    fptu_null = 0,
    fptu_uint16 = 1,

    // fixed length with ex-data (at least 4 byte after the pivot)
    fptu_int32 = 2,
    fptu_uint32 = 3,
    fptu_fp32 = 4,

    fptu_int64 = 5,
    fptu_uint64 = 6,
    fptu_fp64 = 7,

    fptu_96 = 8,   // opaque 12-bytes.
    fptu_128 = 9,  // opaque 16-bytes (uuid, ipv6, etc).
    fptu_160 = 10, // opaque 20-bytes (sha1).
    fptu_192 = 11, // opaque 24-bytes
    fptu_256 = 12, // opaque 32-bytes (sha256).

    // variable length, e.g. length and payload inside ex-data
    fptu_string = 13, // utf-8 с-string, zero terminated

    // with additional length
    fptu_opaque = 14, // opaque octet string
    fptu_nested = 15, // nested tuple
    fptu_farray = 16, // flag

    fptu_typeid_max = (1 << fptu_typeid_bits) - 1,

    // pseudo types for lookup and filtering
    fptu_filter = 1 << (fptu_null | fptu_farray),
    fptu_any = -1, // match any type
    fptu_any_int = fptu_filter | (1 << fptu_int32) |
                   (1 << fptu_int64), // match int32/int64
    fptu_any_uint = fptu_filter | (1 << fptu_uint32) |
                    (1 << fptu_uint64), // match uin32/uin64
    fptu_any_fp =
        fptu_filter | (1 << fptu_fp32) | (1 << fptu_fp64), // match fp32/fp64

    // aliases
    fptu_16 = fptu_uint16,
    fptu_32 = fptu_uint32,
    fptu_64 = fptu_uint64,
    fptu_bool = fptu_uint16,
    fptu_enum = fptu_uint16,
    fptu_char = fptu_uint16,
    fptu_wchar = fptu_uint16,
    fptu_ipv4 = fptu_uint32,
    fptu_uuid = fptu_128,
    fptu_ipv6 = fptu_128,
    fptu_md5 = fptu_128,
    fptu_sha1 = fptu_160,
    fptu_sha256 = fptu_256,
    fptu_wstring = fptu_opaque
};

//----------------------------------------------------------------------------

/* Возвращает минимальный размер буфера, который необходим для размещения
 * кортежа с указанным кол-вом полей и данных. */
size_t fptu_space(size_t items, size_t data_bytes);

/* Инициализирует в буфере кортеж, резервируя (отступая) место достаточное
 * для добавления до items_limit полей. Оставшееся место будет
 * использоваться для размещения данных.
 *
 * Возвращает указатель на созданный в буфере объект, либо nullptr, если
 * заданы неверные параметры или размер буфера недостаточен. */
fptu_rw *fptu_init(void *buffer_space, size_t buffer_bytes,
                   size_t items_limit);

/* Выделяет через malloc() и инициализирует кортеж достаточный для
 * размещения заданного кол-ва полей и данных.
 *
 * Возвращает адрес объекта, который необходимо free() по окончании
 * использования. Либо nullptr при неверных параметрах или нехватке памяти. */
fptu_rw *fptu_alloc(size_t items_limit, size_t data_bytes);

/* Возвращает кол-во свободных слотов для добавления дескрипторов
 * полей в заголовок кортежа. */
size_t fptu_space4items(const fptu_rw *pt);

/* Возвращает остаток места доступного для размещения данных. */
size_t fptu_space4data(const fptu_rw *pt);

/* Возвращает объем мусора в байтах, который станет доступным для
 * добавления полей и данных после fptu_shrink(). */
size_t fptu_junkspace(const fptu_rw *pt);

/* Проверяет сериализованную форму кортежа на корректность.
 *
 * Возвращает nullptr если ошибок не обнаружено, либо указатель на константную
 * строку с краткой информацией о проблеме (нарушенное условие). */
const char *fptu_check_ro(fptu_ro ro);

/* Проверяет модифицируемую форму кортежа на корректность.
 *
 * Возвращает nullptr если ошибок не обнаружено, либо указатель на константную
 * строку с краткой информацией о проблеме (нарушенное условие). */
const char *fptu_check(fptu_rw *pt);

/* Возвращает сериализованную форму кортежа, которая находится внутри
 * модифицируемой. Дефрагментация не выполняется, поэтому сериализованная
 * форма может содержать лишний мусор, см fptu_junkspace().
 *
 * Возвращаемый результат валиден до изменения или разрушения исходной
 * модифицируемой формы кортежа. */
fptu_ro fptu_take_noshrink(fptu_rw *pt);

/* Строит в указанном буфере модифицируемую форму кортежа из сериализованной.
 * Проверка корректности данных в сериализованной форме не производится.
 * Сериализованная форма не модифицируется и не требуется после возврата из
 * функции.
 * Аргумент more_items резервирует кол-во полей, которые можно будет добавить
 * в создаваемую модифицируемую форму.
 *
 * Возвращает указатель на созданный в буфере объект, либо nullptr, если
 * заданы неверные параметры или размер буфера недостаточен. */
fptu_rw *fptu_fetch(fptu_ro ro, void *buffer_space, size_t buffer_bytes,
                    unsigned more_items);

/* Проверяет содержимое сериализованной формы на корректность. Одновременно
 * возвращает размер буфера, который потребуется для модифицируемой формы,
 * с учетом добавляемых more_items полей и more_payload данных.
 *
 * При неверных параметрах или некорректных данных возвращает 0 и записывает
 * в error информацию об ошибке (указатель на константную строку
 * с краткой информацией о проблеме). */
size_t fptu_check_and_get_buffer_size(fptu_ro ro, unsigned more_items,
                                      unsigned more_payload,
                                      const char **error);

/* Производит дефрагментацию модифицируемой формы кортежа. */
void fptu_shrink(fptu_rw *pt);

/* Возвращает сериализованную форму кортежа, которая находится внутри
 * модифицируемой. При необходимости автоматически производится
 * дефрагментация.
 * Возвращаемый результат валиден до изменения или разрушения исходной
 * модифицируемой формы кортежа. */
static __inline fptu_ro fptu_take(fptu_rw *pt)
{
    if (pt->junk)
        fptu_shrink(pt);
    return fptu_take_noshrink(pt);
}

/* Если в аргументе type_or_filter взведен бит fptu_filter,
 * то type_or_filter интерпретируется как битовая маска типов.
 * Соответственно, будут удалены все поля с заданным column и попадающие
 * в маску типов. Например, если type_or_filter равен fptu_any_fp,
 * то удаляются все fptu_fp32 и fptu_fp64.
 *
 * Возвращается кол-во удаленных полей (больше либо равно нулю),
 * либо отрицательный код ошибки. */
int fptu_erase(fptu_rw *pt, unsigned column, int type_or_filter);
void fptu_erase_field(fptu_rw *pt, fptu_field *pf);

//----------------------------------------------------------------------------

// Вставка или обновление существующего поля (первого найденного для
// коллекций).
int fptu_upsert_null(fptu_rw *pt, unsigned column);
int fptu_upsert_uint16(fptu_rw *pt, unsigned column, unsigned value);
int fptu_upsert_int32(fptu_rw *pt, unsigned column, int32_t value);
int fptu_upsert_uint32(fptu_rw *pt, unsigned column, uint32_t value);
int fptu_upsert_int64(fptu_rw *pt, unsigned column, int64_t value);
int fptu_upsert_uint64(fptu_rw *pt, unsigned column, uint64_t value);
int fptu_upsert_fp64(fptu_rw *pt, unsigned column, double value);
int fptu_upsert_fp32(fptu_rw *pt, unsigned column, float value);

int fptu_upsert_96(fptu_rw *pt, unsigned column, const void *data);
int fptu_upsert_128(fptu_rw *pt, unsigned column, const void *data);
int fptu_upsert_160(fptu_rw *pt, unsigned column, const void *data);
int fptu_upsert_192(fptu_rw *pt, unsigned column, const void *data);
int fptu_upsert_256(fptu_rw *pt, unsigned column, const void *data);

int fptu_upsert_cstr(fptu_rw *pt, unsigned column, const char *value);
int fptu_upsert_opaque(fptu_rw *pt, unsigned column, const void *value,
                       size_t bytes);
int fptu_upsert_opaque_iov(fptu_rw *pt, unsigned column,
                           const struct iovec value);
// TODO
// int fptu_upsert_nested(fptu_rw* pt, unsigned column, fptu_ro ro);

// TODO
// int fptu_upsert_array_uint16(fptu_rw* pt, unsigned ct, size_t array_length,
// const uint16_t* array_data);
// int fptu_upsert_array_int32(fptu_rw* pt, unsigned ct, size_t array_length,
// const int32_t* array_data);
// int fptu_upsert_array_uint32(fptu_rw* pt, unsigned ct, size_t array_length,
// const uint32_t* array_data);
// int fptu_upsert_array_int64(fptu_rw* pt, unsigned ct, size_t array_length,
// const int64_t* array_data);
// int fptu_upsert_array_uint64(fptu_rw* pt, unsigned ct, size_t array_length,
// const uint64_t* array_data);
// int fptu_upsert_array_str(fptu_rw* pt, unsigned ct, size_t array_length,
// const char* array_data[]);
// int fptu_upsert_array_nested(fptu_rw* pt, unsigned ct, size_t array_length,
// const char* array_data[]);

//----------------------------------------------------------------------------

// Добавление ещё одного поля, для поддержки коллекций.
int fptu_insert_uint16(fptu_rw *pt, unsigned column, unsigned value);
int fptu_insert_int32(fptu_rw *pt, unsigned column, int32_t value);
int fptu_insert_uint32(fptu_rw *pt, unsigned column, uint32_t value);
int fptu_insert_int64(fptu_rw *pt, unsigned column, int64_t value);
int fptu_insert_uint64(fptu_rw *pt, unsigned column, uint64_t value);
int fptu_insert_fp64(fptu_rw *pt, unsigned column, double value);
int fptu_insert_fp32(fptu_rw *pt, unsigned column, float value);

int fptu_insert_96(fptu_rw *pt, unsigned column, const void *data);
int fptu_insert_128(fptu_rw *pt, unsigned column, const void *data);
int fptu_insert_160(fptu_rw *pt, unsigned column, const void *data);
int fptu_insert_192(fptu_rw *pt, unsigned column, const void *data);
int fptu_insert_256(fptu_rw *pt, unsigned column, const void *data);

int fptu_insert_cstr(fptu_rw *pt, unsigned column, const char *value);
int fptu_insert_opaque(fptu_rw *pt, unsigned column, const void *value,
                       size_t bytes);
int fptu_insert_opaque_iov(fptu_rw *pt, unsigned column,
                           const struct iovec value);
// TODO
// int fptu_insert_nested(fptu_rw* pt, unsigned column, fptu_ro ro);

// TODO
// int fptu_insert_array_uint16(fptu_rw* pt, unsigned ct, size_t array_length,
// const uint16_t* array_data);
// int fptu_insert_array_int32(fptu_rw* pt, unsigned ct, size_t array_length,
// const int32_t* array_data);
// int fptu_insert_array_uint32(fptu_rw* pt, unsigned ct, size_t array_length,
// const uint32_t* array_data);
// int fptu_insert_array_int64(fptu_rw* pt, unsigned ct, size_t array_length,
// const int64_t* array_data);
// int fptu_insert_array_uint64(fptu_rw* pt, unsigned ct, size_t array_length,
// const uint64_t* array_data);
// int fptu_insert_array_str(fptu_rw* pt, unsigned ct, size_t array_length,
// const char* array_data[]);

//----------------------------------------------------------------------------

// Обновление существующего поля (первого найденного для коллекций).
int fptu_update_uint16(fptu_rw *pt, unsigned column, unsigned value);
int fptu_update_int32(fptu_rw *pt, unsigned column, int32_t value);
int fptu_update_uint32(fptu_rw *pt, unsigned column, uint32_t value);
int fptu_update_int64(fptu_rw *pt, unsigned column, int64_t value);
int fptu_update_uint64(fptu_rw *pt, unsigned column, uint64_t value);
int fptu_update_fp64(fptu_rw *pt, unsigned column, double value);
int fptu_update_fp32(fptu_rw *pt, unsigned column, float value);

int fptu_update_96(fptu_rw *pt, unsigned column, const void *data);
int fptu_update_128(fptu_rw *pt, unsigned column, const void *data);
int fptu_update_160(fptu_rw *pt, unsigned column, const void *data);
int fptu_update_192(fptu_rw *pt, unsigned column, const void *data);
int fptu_update_256(fptu_rw *pt, unsigned column, const void *data);

int fptu_update_cstr(fptu_rw *pt, unsigned column, const char *value);
int fptu_update_opaque(fptu_rw *pt, unsigned column, const void *value,
                       size_t bytes);
int fptu_update_opaque_iov(fptu_rw *pt, unsigned column,
                           const struct iovec value);
// TODO
// int fptu_update_nested(fptu_rw* pt, unsigned column, fptu_ro ro);

// TODO
// int fptu_update_array_uint16(fptu_rw* pt, unsigned ct, size_t array_length,
// const uint16_t* array_data);
// int fptu_update_array_int32(fptu_rw* pt, unsigned ct, size_t array_length,
// const int32_t* array_data);
// int fptu_update_array_uint32(fptu_rw* pt, unsigned ct, size_t array_length,
// const uint32_t* array_data);
// int fptu_update_array_int64(fptu_rw* pt, unsigned ct, size_t array_length,
// const int64_t* array_data);
// int fptu_update_array_uint64(fptu_rw* pt, unsigned ct, size_t array_length,
// const uint64_t* array_data);
// int fptu_update_array_str(fptu_rw* pt, unsigned ct, size_t array_length,
// const char* array_data[]);

//----------------------------------------------------------------------------

/* Возвращает первое поле попадающее под критерий выбора, либо nullptr.
 * Семантика type_or_filter указана в описании fptu_erase(). */
const fptu_field *fptu_lookup_ro(fptu_ro ro, unsigned column,
                                 int type_or_filter);
fptu_field *fptu_lookup(fptu_rw *pt, unsigned column, int type_or_filter);

/* Возвращает "итераторы" по кортежу, в виде указателей.
 * Гарантируется что begin меньше, либо равно end.
 * В возвращаемом диапазоне могут буть удаленные поля,
 * для которых fptu_field_column() возвращает отрицательное значение. */
const fptu_field *fptu_begin_ro(fptu_ro ro);
const fptu_field *fptu_end_ro(fptu_ro ro);
const fptu_field *fptu_begin(const fptu_rw *pt);
const fptu_field *fptu_end(const fptu_rw *pt);

/* Итерация полей кортежа с заданным условие отбора, при этом
 * удаленные поля пропускаются.
 * Семантика type_or_filter указана в описании fptu_erase(). */
const fptu_field *fptu_first(const fptu_field *begin, const fptu_field *end,
                             unsigned column, int type_or_filter);
const fptu_field *fptu_next(const fptu_field *from, const fptu_field *end,
                            unsigned column, int type_or_filter);

/* Итерация полей кортежа с заданным внешним фильтром, при этом
 * удаленные поля пропускаются.
 * Семантика type_or_filter указана в описании fptu_erase(). */
typedef bool fptu_field_filter(const fptu_field *, void *context,
                               void *param);
const fptu_field *fptu_first_ex(const fptu_field *begin,
                                const fptu_field *end,
                                fptu_field_filter filter, void *context,
                                void *param);
const fptu_field *fptu_next_ex(const fptu_field *begin, const fptu_field *end,
                               fptu_field_filter filter, void *context,
                               void *param);

size_t fptu_field_count(const fptu_rw *pt, unsigned column,
                        int type_or_filter);
size_t fptu_field_count_ro(fptu_ro ro, unsigned column, int type_or_filter);

size_t fptu_field_count_ex(const fptu_rw *pt, fptu_field_filter filter,
                           void *context, void *param);
size_t fptu_field_count_ro_ex(fptu_ro ro, fptu_field_filter filter,
                              void *context, void *param);

int fptu_field_type(const fptu_field *pf);
int fptu_field_column(const fptu_field *pf);

uint16_t fptu_field_uint16(const fptu_field *pf);
int32_t fptu_field_int32(const fptu_field *pf);
uint32_t fptu_field_uint32(const fptu_field *pf);
int64_t fptu_field_int64(const fptu_field *pf);
uint64_t fptu_field_uint64(const fptu_field *pf);
double fptu_field_fp64(const fptu_field *pf);
float fptu_field_fp32(const fptu_field *pf);
const uint8_t *fptu_field_96(const fptu_field *pf);
const uint8_t *fptu_field_128(const fptu_field *pf);
const uint8_t *fptu_field_160(const fptu_field *pf);
const uint8_t *fptu_field_192(const fptu_field *pf);
const uint8_t *fptu_field_256(const fptu_field *pf);
const char *fptu_field_cstr(const fptu_field *pf);
struct iovec fptu_field_opaque(const fptu_field *pf);
fptu_ro fptu_field_nested(const fptu_field *pf);

uint16_t fptu_get_uint16(fptu_ro ro, unsigned column, int *error);
int32_t fptu_get_int32(fptu_ro ro, unsigned column, int *error);
uint32_t fptu_get_uint32(fptu_ro ro, unsigned column, int *error);
int64_t fptu_get_int64(fptu_ro ro, unsigned column, int *error);
uint64_t fptu_get_uint64(fptu_ro ro, unsigned column, int *error);
double fptu_get_fp64(fptu_ro ro, unsigned column, int *error);
float fptu_get_fp32(fptu_ro ro, unsigned column, int *error);
const uint8_t *fptu_get_96(fptu_ro ro, unsigned column, int *error);
const uint8_t *fptu_get_128(fptu_ro ro, unsigned column, int *error);
const uint8_t *fptu_get_160(fptu_ro ro, unsigned column, int *error);
const uint8_t *fptu_get_192(fptu_ro ro, unsigned column, int *error);
const uint8_t *fptu_get_256(fptu_ro ro, unsigned column, int *error);
const char *fptu_get_cstr(fptu_ro ro, unsigned column, int *error);
struct iovec fptu_get_opaque(fptu_ro ro, unsigned column, int *error);
fptu_ro fptu_get_nested(fptu_ro ro, unsigned column, int *error);

// TODO: fptu_field_array(), fptu_get_array()
typedef struct fptu_array {
    size_t size;
    union {
        uint16_t uint16[2];
        int32_t int32[1];
        uint32_t uint32[1];
        int64_t int64[1];
        uint64_t uint64[1];
        double fp64[1];
        float fp32[1];
        const char *cstr[1];
        fptu_ro nested[1];
        struct iovec opaque[1];
    };
} fptu_array;

//----------------------------------------------------------------------------

typedef enum fptu_cmp {
    fptu_ic = 1,                           // incomparable
    fptu_eq = 2,                           // left == right
    fptu_lt = 4,                           // left < right
    fptu_gt = 8,                           // left > right
    fptu_ne = fptu_lt | fptu_gt | fptu_ic, // left != right
    fptu_le = fptu_lt | fptu_eq,           // left <= right
    fptu_ge = fptu_gt | fptu_eq,           // left >= right
} fptu_cmp;

int fptu_cmp_96(fptu_ro ro, unsigned column, const uint8_t *value);
int fptu_cmp_128(fptu_ro ro, unsigned column, const uint8_t *value);
int fptu_cmp_160(fptu_ro ro, unsigned column, const uint8_t *value);
int fptu_cmp_192(fptu_ro ro, unsigned column, const uint8_t *value);
int fptu_cmp_256(fptu_ro ro, unsigned column, const uint8_t *value);
int fptu_cmp_opaque(fptu_ro ro, unsigned column, const void *value,
                    size_t bytes);
int fptu_cmp_opaque_iov(fptu_ro ro, unsigned column,
                        const struct iovec value);

//----------------------------------------------------------------------------

enum fptu_error {
    fptu_ok = 0,
    fptu_noent = -1,
    fptu_einval = -2,
    fptu_enospc = -3,
    fptu_etm = -4,
};

#ifdef __cplusplus
}
#endif

#endif /* FAST_POSITIVE_TUPLES_H */

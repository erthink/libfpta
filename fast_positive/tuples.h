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

/*
 * libfptu = { Fast Positive Tuples, aka Позитивные Кортежи }
 *
 * The Future will Positive. Всё будет хорошо.
 *
 * Please see README.md
 */

#pragma once
#ifndef FAST_POSITIVE_TUPLES_H
#define FAST_POSITIVE_TUPLES_H

#include "fast_positive/config.h"
#include "fast_positive/defs.h"

#include <errno.h>  // for error codes
#include <string.h> // for strlen()
#include <time.h>   // for struct timespec, struct timeval

#ifdef HAVE_SYS_UIO_H
#include <sys/uio.h> // for struct iovec
#else
struct iovec {
  void *iov_base; /* Starting address */
  size_t iov_len; /* Number of bytes to transfer */
};
#endif /* windows mustdie */

#ifdef __cplusplus
#include <string> // for std::string

extern "C" {
#endif

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(                                                             \
    disable : 4201 /* нестандартное расширение: структура (объединение) без имени */)
#pragma pack(push, 1)
#endif /* windows mustdie */

//----------------------------------------------------------------------------
/* Опции конфигурации управляющие внутренним поведением libfptu, т.е
 * их изменение требует пересборки библиотеки.
 *
 * Чуть позже эти определения передедут в fptu_config.h */

// TBD

//----------------------------------------------------------------------------
/* Общие перечисления и структуры */

/* Коды ошибок.
 * Список будет пополнен, а описания уточнены. */
enum fptu_error {
  FPTU_SUCCESS = 0,
  FPTU_OK = FPTU_SUCCESS,
  FPTU_ENOFIELD = ENOENT,
  FPTU_EINVAL = EINVAL,
  FPTU_ENOSPACE = ENOSPC,
};

/* Внутренний тип для хранения размера полей переменной длины. */
typedef union fptu_varlen {
  struct __packed {
    uint16_t brutto; /* брутто-размер в 4-байтовых юнитах,
                      * всегда больше или равен 1. */
    union {
      uint16_t opaque_bytes;
      uint16_t array_length;
      uint16_t tuple_items;
    };
  };
  uint32_t flat;
} fptu_varlen;

/* Поле кортежа.
 *
 * Фактически это дескриптор поля, в котором записаны: тип данных,
 * номер колонки и смещение к данным. */
typedef union fptu_field {
  struct __packed {
    uint16_t ct;     /* тип и "номер колонки". */
    uint16_t offset; /* смещение к данным относительно заголовка, либо
                        непосредственно данные для uint16_t. */
  };
  uint32_t header;
  uint32_t body[1]; /* в body[0] расположен дескриптор/заголовок,
                     * а начиная с body[offset] данные. */
#ifdef __cplusplus
  uint16_t get_payload_uint16() const { return offset; }
#endif
} fptu_field;

/* Внутренний тип соответствующий 32-битной ячейке с данными. */
typedef union fptu_unit {
  fptu_field field;
  fptu_varlen varlen;
  uint32_t data;
} fptu_unit;

/* Представление сериализованной формы кортежа.
 *
 * Фактические это просто системная структура iovec, т.е. указатель
 * на буфер с данными и размер этих данных в байтах. Системный тип struct
 * iovec выбран для совместимости с функциями readv(), writev() и т.п.
 * Другими словами, это просто "оболочка", а сами данные кортежа должны быть
 * где-то размещены. */
typedef union fptu_ro {
  struct {
    const fptu_unit *units;
    size_t total_bytes;
  };
  struct iovec sys;
} fptu_ro;

/* Изменяемая форма кортежа.
 * Является плоским буфером, в начале которого расположены служебные поля.
 *
 * Инициализируется функциями fptu_init(), fptu_alloc() и fptu_fetch(). */
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

/* Основные ограничения, константы и их производные. */
enum fptu_bits {
  // базовые лимиты и параметры
  fptu_bits = 16, // ширина счетчиков
  fptu_typeid_bits = 5, // ширина типа в идентификаторе поля
  fptu_ct_reserve_bits = 1, // резерв в идентификаторе поля
  fptu_unit_size = 4,       // размер одного юнита
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
  // маска резервных битов в идентификаторе поля/колонки
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

/* Типы полей.
 *
 * Следует обратить внимание, что fptu_farray является флагом,
 * а значения начиная с fptu_filter используются как маски для
 * поиска/фильтрации полей (и видимо будут выделены в отдельный enum). */
typedef enum fptu_type {
  // fixed length, without ex-data (descriptor only)
  fptu_null = 0,
  fptu_uint16 = 1,

  // fixed length with ex-data (at least 4 byte after the pivot)
  fptu_int32 = 2,
  fptu_uint32 = 3,
  fptu_fp32 = 4, // 32-bit float-point, e.g. float

  fptu_int64 = 5,
  fptu_uint64 = 6,
  fptu_fp64 = 7,     // 64-bit float-point, e.g. double
  fptu_datetime = 8, // date-time as fixed-point, compatible with UTC

  fptu_96 = 9,   // opaque 12-bytes (subject to change)
  fptu_128 = 10, // opaque 16-bytes (uuid, ipv6, etc).
  fptu_160 = 11, // opaque 20-bytes (sha1).
  fptu_256 = 12, // opaque 32-bytes (sha256).

  // variable length, e.g. length and payload inside ex-data
  fptu_cstr = 13, // utf-8 с-string, zero terminated

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
  fptu_any_uint = fptu_filter | (1 << fptu_uint16) | (1 << fptu_uint32) |
                  (1 << fptu_uint64), // match uint16/uint32/uint64
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
} fptu_type;

/* Представление времени.
 *
 * В формате фиксированной точки 32-dot-32:
 *   - В старшей "целой" части секунды по UTC, буквально как выдает time(),
 *     но без знака. Это отодвигает проблему 2038-го года на 2106,
 *     требуя взамен аккуратности при вычитании.
 *   - В младшей "дробной" части неполные секунды в 1/2**32 долях.
 *
 * Эта форма унифицирована с "Positive Hyper100re" и одновременно достаточно
 * удобна в использовании. Поэтому настоятельно рекомендуется использовать
 * именно её, особенно для хранения и передачи данных. */
typedef union fptu_time {
  uint64_t fixedpoint;
  struct __packed {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    uint32_t fractional;
    uint32_t utc;
#else
    uint32_t utc;
    uint32_t fractional;
#endif
  };

#ifdef __cplusplus
  static uint32_t ns2fractional(uint32_t);
  static uint32_t fractional2ns(uint32_t);
  static uint32_t us2fractional(uint32_t);
  static uint32_t fractional2us(uint32_t);
  static uint32_t ms2fractional(uint32_t);
  static uint32_t fractional2ms(uint32_t);

#ifdef HAVE_TIMESPEC_TV_NSEC
  /* LY: Clang не позволяет возвращать из C-linkage функции структуру,
   * у которой есть какие-либо конструкторы C++. Поэтому необходимо отказаться
   * либо от возможности использовать libfptu из C, либо от Clang,
   * либо от конструкторов (они и пострадали). */
  static fptu_time from_timespec(const struct timespec &ts) {
    fptu_time result;
    result.fixedpoint =
        ((uint64_t)ts.tv_sec << 32) | ns2fractional((uint32_t)ts.tv_nsec);
    return result;
  }
#endif /* HAVE_TIMESPEC_TV_NSEC */

#ifdef HAVE_TIMEVAL_TV_USEC
  static fptu_time from_timeval(const struct timeval &tv) {
    fptu_time result;
    result.fixedpoint =
        ((uint64_t)tv.tv_sec << 32) | us2fractional((uint32_t)tv.tv_usec);
    return result;
  }
#endif /* HAVE_TIMEVAL_TV_USEC */
#endif
} fptu_time;

/* Возвращает текущее время в правильной форме.
 *
 * Аргумент grain_ns задает желаемую точность в наносекундах, в зависимости от
 * которой будет использован CLOCK_REALTIME, либо CLOCK_REALTIME_COARSE.
 *
 * Положительные значения grain_ns, включая нуль, трактуются как наносекунды.
 *
 * Отрицательные же означают количество младших бит, которые НЕ требуются в
 * результате и будут обнулены. Таким образом, отрицательные значения grain_ns
 * позволяют запросить текущее время, одновременно с "резервированием" младших
 * бит результата под специфические нужды.
 *
 * В конечном счете это позволяет существенно экономить на системных вызовах
 * и/или обращении к аппаратуре. В том числе не выполнять системный вызов,
 * а ограничиться использованием механизма vdso (прямое чтение из открытой
 * страницы данных ядра). В зависимости от запрошенной точности,
 * доступной аппаратуры и актуальном режиме работы ядра, экономия может
 * составить до сотен и даже тысяч раз.
 *
 * Следует понимать, что реальная точность зависит от актуальной конфигурации
 * аппаратуры и ядра ОС. Проще говоря, запрос текущего времени с grain_ns = 1
 * достаточно абсурден и вовсе не гарантирует такой точности результата. */
fptu_time fptu_now(int grain_ns);
fptu_time fptu_now_fine(void);
fptu_time fptu_now_coarse(void);

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

/* Очищает ранее инициализированный кортеж.
 * В случае успеха возвращает ноль, иначе код ошибки. */
int fptu_clear(fptu_rw *pt);

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
const char *fptu_check(const fptu_rw *pt);

/* Возвращает сериализованную форму кортежа, которая находится внутри
 * модифицируемой. Дефрагментация не выполняется, поэтому сериализованная
 * форма может содержать лишний мусор, см fptu_junkspace().
 *
 * Возвращаемый результат валиден до изменения или разрушения исходной
 * модифицируемой формы кортежа. */
fptu_ro fptu_take_noshrink(const fptu_rw *pt);

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

/* Производит дефрагментацию модифицируемой формы кортежа.
 * Возвращает true если была произведена дефрагментация, что можно
 * использовать как признак инвалидации итераторов. */
bool fptu_shrink(fptu_rw *pt);

/* Производит дефрагментацию модифицируемой формы кортежа при наличии
 * пустот/мусора после удаления полей.
 * Возвращает true если была произведена дефрагментация, что можно
 * использовать как признак инвалидации итераторов. */
static __inline bool fptu_cond_shrink(fptu_rw *pt) {
  return pt->junk != 0 && fptu_shrink(pt);
}

/* Возвращает сериализованную форму кортежа, которая находится внутри
 * модифицируемой. При необходимости автоматически производится
 * дефрагментация.
 * Возвращаемый результат валиден до изменения или разрушения исходной
 * модифицируемой формы кортежа. */
static __inline fptu_ro fptu_take(fptu_rw *pt) {
  fptu_cond_shrink(pt);
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

extern const char fptu_empty_cstr[];

/* Вставка или обновление существующего поля.
 *
 * В случае коллекций, когда в кортеже более одного поля с соответствующим
 * типом и номером), будет обновлен первый найденный экземпляр. Но так как
 * в общем случае физический порядок полей не определен, следует считать что
 * функция обновит произвольный экземпляр поля. Поэтому для манипулирования
 * коллекциями следует использовать fptu_erase() и/или fput_field_set_xyz().
 */
int fptu_upsert_null(fptu_rw *pt, unsigned column);
int fptu_upsert_uint16(fptu_rw *pt, unsigned column, unsigned value);
int fptu_upsert_int32(fptu_rw *pt, unsigned column, int32_t value);
int fptu_upsert_uint32(fptu_rw *pt, unsigned column, uint32_t value);
int fptu_upsert_int64(fptu_rw *pt, unsigned column, int64_t value);
int fptu_upsert_uint64(fptu_rw *pt, unsigned column, uint64_t value);
int fptu_upsert_fp64(fptu_rw *pt, unsigned column, double value);
int fptu_upsert_fp32(fptu_rw *pt, unsigned column, float value);
int fptu_upsert_datetime(fptu_rw *pt, unsigned column, const fptu_time);

int fptu_upsert_96(fptu_rw *pt, unsigned column, const void *data);
int fptu_upsert_128(fptu_rw *pt, unsigned column, const void *data);
int fptu_upsert_160(fptu_rw *pt, unsigned column, const void *data);
int fptu_upsert_256(fptu_rw *pt, unsigned column, const void *data);

int fptu_upsert_string(fptu_rw *pt, unsigned column, const char *text,
                       size_t length);
static __inline int fptu_upsert_cstr(fptu_rw *pt, unsigned col,
                                     const char *value) {
  if (value == nullptr)
    value = fptu_empty_cstr;

  return fptu_upsert_string(pt, col, value, strlen(value));
}

int fptu_upsert_opaque(fptu_rw *pt, unsigned column, const void *value,
                       size_t bytes);
int fptu_upsert_opaque_iov(fptu_rw *pt, unsigned column,
                           const struct iovec value);
int fptu_upsert_nested(fptu_rw *pt, unsigned column, fptu_ro ro);

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
// int fptu_upsert_array_cstr(fptu_rw* pt, unsigned ct, size_t array_length,
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
int fptu_insert_datetime(fptu_rw *pt, unsigned column, const fptu_time);

int fptu_insert_96(fptu_rw *pt, unsigned column, const void *data);
int fptu_insert_128(fptu_rw *pt, unsigned column, const void *data);
int fptu_insert_160(fptu_rw *pt, unsigned column, const void *data);
int fptu_insert_256(fptu_rw *pt, unsigned column, const void *data);

int fptu_insert_string(fptu_rw *pt, unsigned column, const char *text,
                       size_t length);
static __inline int fptu_insert_cstr(fptu_rw *pt, unsigned col,
                                     const char *value) {
  if (value == nullptr)
    value = fptu_empty_cstr;

  return fptu_insert_string(pt, col, value, strlen(value));
}

int fptu_insert_opaque(fptu_rw *pt, unsigned column, const void *value,
                       size_t bytes);
int fptu_insert_opaque_iov(fptu_rw *pt, unsigned column,
                           const struct iovec value);
int fptu_insert_nested(fptu_rw *pt, unsigned column, fptu_ro ro);

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
int fptu_update_datetime(fptu_rw *pt, unsigned column, const fptu_time);

int fptu_update_96(fptu_rw *pt, unsigned column, const void *data);
int fptu_update_128(fptu_rw *pt, unsigned column, const void *data);
int fptu_update_160(fptu_rw *pt, unsigned column, const void *data);
int fptu_update_256(fptu_rw *pt, unsigned column, const void *data);

int fptu_update_string(fptu_rw *pt, unsigned column, const char *text,
                       size_t length);
static __inline int fptu_update_cstr(fptu_rw *pt, unsigned col,
                                     const char *value) {
  if (value == nullptr)
    value = fptu_empty_cstr;

  return fptu_update_string(pt, col, value, strlen(value));
}

int fptu_update_opaque(fptu_rw *pt, unsigned column, const void *value,
                       size_t bytes);
int fptu_update_opaque_iov(fptu_rw *pt, unsigned column,
                           const struct iovec value);
int fptu_update_nested(fptu_rw *pt, unsigned column, fptu_ro ro);

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
// int fptu_update_array_cstr(fptu_rw* pt, unsigned ct, size_t array_length,
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
/* Подсчет количества полей по заданному номеру колонки и типу,
 * либо маски типов .*/
size_t fptu_field_count(const fptu_rw *pt, unsigned column,
                        int type_or_filter);
size_t fptu_field_count_ro(fptu_ro ro, unsigned column, int type_or_filter);

/* Подсчет количества полей задаваемой функцией-фильтром. */
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
fptu_time fptu_field_datetime(const fptu_field *pf);
const uint8_t *fptu_field_96(const fptu_field *pf);
const uint8_t *fptu_field_128(const fptu_field *pf);
const uint8_t *fptu_field_160(const fptu_field *pf);
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
fptu_time fptu_get_datetime(fptu_ro ro, unsigned column, int *error);

int64_t fptu_get_sint(fptu_ro ro, unsigned column, int *error);
uint64_t fptu_get_uint(fptu_ro ro, unsigned column, int *error);
double fptu_get_fp(fptu_ro ro, unsigned column, int *error);

const uint8_t *fptu_get_96(fptu_ro ro, unsigned column, int *error);
const uint8_t *fptu_get_128(fptu_ro ro, unsigned column, int *error);
const uint8_t *fptu_get_160(fptu_ro ro, unsigned column, int *error);
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
/* Определения и примитивы для сравнения. */

typedef enum fptu_lge {
  fptu_ic = 1,                           // incomparable
  fptu_eq = 2,                           // left == right
  fptu_lt = 4,                           // left < right
  fptu_gt = 8,                           // left > right
  fptu_ne = fptu_lt | fptu_gt | fptu_ic, // left != right
  fptu_le = fptu_lt | fptu_eq,           // left <= right
  fptu_ge = fptu_gt | fptu_eq            // left >= right
} fptu_lge;

fptu_lge fptu_cmp_96(fptu_ro ro, unsigned column, const uint8_t *value);
fptu_lge fptu_cmp_128(fptu_ro ro, unsigned column, const uint8_t *value);
fptu_lge fptu_cmp_160(fptu_ro ro, unsigned column, const uint8_t *value);
fptu_lge fptu_cmp_256(fptu_ro ro, unsigned column, const uint8_t *value);
fptu_lge fptu_cmp_opaque(fptu_ro ro, unsigned column, const void *value,
                         size_t bytes);
fptu_lge fptu_cmp_opaque_iov(fptu_ro ro, unsigned column,
                             const struct iovec value);

fptu_lge fptu_cmp_binary(const void *left_data, size_t left_len,
                         const void *right_data, size_t right_len);

fptu_lge fptu_cmp_fields(const fptu_field *left, const fptu_field *right);
fptu_lge fptu_cmp_tuples(fptu_ro left, fptu_ro right);

#ifdef __cplusplus
}

//----------------------------------------------------------------------------
/* Сервисные функции и классы для C++ (будет существенно пополнятся). */

namespace fptu {
std::string format(const char *fmt, ...)
#ifdef __GNUC__
    __attribute__((format(printf, 1, 2)))
#endif
    ;
std::string hexadecimal(const void *data, size_t bytes);
} /* namespace fptu */

namespace std {
string to_string(fptu_error);
string to_string(const fptu_varlen &);
string to_string(const fptu_unit &);
string to_string(fptu_type);
string to_string(const fptu_field &);
string to_string(const fptu_rw &);
string to_string(const fptu_ro &);
string to_string(fptu_lge);
string to_string(const fptu_time &time);
} /* namespace std */

/* Явно удаляем лишенные смысла операции, в том числе для выявления ошибок */
bool operator>(const fptu_lge &, const fptu_lge &) = delete;
bool operator>=(const fptu_lge &, const fptu_lge &) = delete;
bool operator<(const fptu_lge &, const fptu_lge &) = delete;
bool operator<=(const fptu_lge &, const fptu_lge &) = delete;
#endif /* __cplusplus */

const char *fptu_type_name(fptu_type);

#ifdef _MSC_VER
#pragma pack(pop)
#pragma warning(pop)
#endif /* windows mustdie */

#endif /* FAST_POSITIVE_TUPLES_H */

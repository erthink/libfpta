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

/*
 * libfpta = { Fast Positive Tables, aka Позитивные Таблицы }
 *
 * Ultra fast, compact, embeddable storage engine for (semi)structured data:
 * multiprocessing with zero-overhead, full ACID semantics with MVCC,
 * variety of indexes, saturation, sequences and much more.
 * Please see README.md at https://github.com/leo-yuriev/libfpta
 *
 * The Future will Positive. Всё будет хорошо.
 *
 * "Позитивные таблицы" предназначены для построения высокоскоростных
 * локальных хранилищ структурированных данных, с целевой производительностью
 * до 1.000.000 запросов в секунду на каждое ядро процессора.
 */

#pragma once
#ifndef FAST_POSITIVE_TABLES_H
#define FAST_POSITIVE_TABLES_H

#define FPTA_VERSION_MAJOR 0
#define FPTA_VERSION_MINOR 0

#include "fast_positive/config.h"
#include "fast_positive/defs.h"

#if defined(fpta_EXPORTS)
#define FPTA_API __dll_export
#elif LIBFPTA_STATIC
#define FPTA_API
#else
#define FPTA_API __dll_import
#endif /* fpta_EXPORTS */

#include "fast_positive/tuples.h"

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4514) /* 'xyz': unreferenced inline function         \
                                   has been removed */
#pragma warning(disable : 4710) /* 'xyz': function not inlined */
#pragma warning(disable : 4711) /* function 'xyz' selected for                 \
                                   automatic inline expansion */
#pragma warning(disable : 4061) /* enumerator 'abc' in switch of enum 'xyz' is \
                                   not explicitly handled by a case label */
#pragma warning(disable : 4201) /* nonstandard extension used :                \
                                   nameless struct / union */
#pragma warning(disable : 4127) /* conditional expression is constant */

#pragma warning(push, 1)
#ifndef _STL_WARNING_LEVEL
#define _STL_WARNING_LEVEL 3 /* Avoid warnings inside nasty MSVC STL code */
#endif
#pragma warning(disable : 4548) /* expression before comma has no effect;      \
                                   expected expression with side - effect */
#pragma warning(disable : 4530) /* C++ exception handler used, but unwind      \
                                   semantics are not enabled. Specify /EHsc */
#pragma warning(disable : 4577) /* 'noexcept' used with no exception handling  \
                                   mode specified; termination on exception    \
                                   is not guaranteed. Specify /EHsc */
#endif                          /* _MSC_VER (warnings) */

#include <assert.h> // for assert()
#include <errno.h>  // for error codes
#include <limits.h> // for INT_MAX
#include <string.h> // for strlen()

#if defined(HAVE_SYS_STAT_H) && !defined(_WIN32) && !defined(_WIN64)
#include <sys/stat.h> // for mode_t
#elif !defined(__mode_t_defined)
typedef unsigned short mode_t;
#endif

#ifdef _MSC_VER
#pragma warning(pop)
#pragma pack(push, 1)
#endif

//----------------------------------------------------------------------------
/* Опции конфигурации управляющие внутренним поведением libfpta, т.е
 * их изменение требует пересборки библиотеки.
 *
 * Чуть позже эти определения передедут в fpta_config.h */

#ifndef FPTA_ALLOW_DOT4NAMES
/* Опция разрешает использование точки в именах таблиц и колонок. */
#define FPTA_ALLOW_DOT4NAMES 0
#endif /* FPTA_ALLOW_DOT4NAMES */

#ifndef FPTA_PROHIBIT_UPSERT_NAN
/* Опция определяет поведение при вставке NaN значений
 * посредством fpta_upsert_column().
 *
 * Если опция ВЫКЛЮЧЕНА (определена как 0), то будет вставлено переданное
 * NaN-значение.
 *
 * Если же опция ВКЛЮЧЕНА (определена как не 0), то при попытке такой
 * вставке или обновлении будет возвращена ошибка FPTA_EVALUE. */
#define FPTA_PROHIBIT_UPSERT_NAN 1
#endif /* FPTA_PROHIBIT_UPSERT_NAN */

#ifndef FPTA_PROHIBIT_UPSERT_DENIL
/* Опция определяет поведение при вставке посредством fpta_upsert_column()
 * значений зарезервированных за designated empty.
 *
 * Если опция ВЫКЛЮЧЕНА (определена как 0), то такая вставка или обновление
 * приведет к удалению соответствующей колонки, а не возврату ошибки.
 *
 * Если же опция ВКЛЮЧЕНА (определена как не 0), то при попытке такой
 * вставке или обновлении будет возвращена ошибка FPTA_EVALUE. */
#define FPTA_PROHIBIT_UPSERT_DENIL 1
#endif /* FPTA_PROHIBIT_UPSERT_DENIL */

#ifndef FPTA_CLEAN_DENIL
/* Опция включает внутри fpta_field2value() чистку значений зарезервированных
 * за designated empty.
 *
 * Чистка designated empty не нужна, если fpta_field2value() вызывается для
 * данных, которые были штатно помещены и после прочитаны из ftpa-таблицы,
 * так как при этом designated empty значения не должны попасть в данные.
 *
 * Если опция ВЫКЛЮЧЕНА (определена как 0), то чистка не выполняется.
 * Соответственно, при подаче на вход fpta_field2value() поля с designated
 * empty значением внутри это значение будет преобразованно не в fpta_null,
 * а в недопустимое значение соответствующего типа. При этом в отладочных
 * версиях библиотеки сработает assert-проверка.
 *
 * Если же опция ВКЛЮЧЕНА (определена как не 0), то поля с соответствующим
 * значениями будут преобразованы в fpta_null. */
#define FPTA_CLEAN_DENIL 0
#endif /* FPTA_CLEAN_DENIL */

#ifndef FPTA_PROHIBIT_NEARBY4UNORDERED
/* Опция определяет поведение при запросах позиционирования к ближайшему
 * значению посредством fpta_cursor_locate() для "неупорядоченных" курсоров.
 *
 * Установка курсора в позицию с ближайшим значением (аналог lower bound)
 * возможна, только если заданный при открытии курсора индекс упорядочен.
 * В свою очередь, для неупорядоченных индексов допускается открытие только
 * курсоров без сортировки. Поэтому для таких курсоров гарантированно
 * можно выполнить только точное позиционирование.
 *
 * Соответственно, опция определяет, будет ли в описанных случаях выполняться
 * точное позиционирование (вместо неточного), либо же будет возвращена
 * ошибка FPTA_EINVAL. */
#define FPTA_PROHIBIT_NEARBY4UNORDERED 1
#endif /* FPTA_PROHIBIT_NEARBY4UNORDERED */

#ifndef FPTA_PROHIBIT_LOSS_PRECISION
/* При обслуживании индексированных колонок типа float/fptu_fp32
 * double значения из fpta_value будут преобразованы во float.
 * Опция определяет, считать ли потерю точности ошибкой при этом или нет. */
#define FPTA_PROHIBIT_LOSS_PRECISION 0
#endif /* FPTA_PROHIBIT_LOSS_PRECISION */

#ifndef FPTA_ENABLE_RETURN_INTO_RANGE
/* Опция определяет, поддерживать ли для курсора возврат в диапазон строк
 * после его исчерпания при итерировании. Например, позволить ли возврат
 * к последней строке посредством move(prev), после того как move(next)
 * вернул FPTA_NODATA, так как курсор уже был на последней строке. */
#define FPTA_ENABLE_RETURN_INTO_RANGE 1
#endif /*FPTA_ENABLE_RETURN_INTO_RANGE */

#ifndef FPTA_ENABLE_ABORT_ON_PANIC
/* Опция определяет, что делать при фатальных ошибках, например в случае
 * ошибки отката/прерывания транзакции.
 *
 * Как правило, такие ошибки возникают при "росписи" памяти или при серьезных
 * нарушениях соглашений API. Лучшим выходом, в таких случаях, обычно является
 * скорейшее прерывание выполнения процесса. Согласованность данных при этом
 * обеспечивается откатом (не фиксацией) транзакции внутри libmdbx.
 *
 * Если FPTA_ENABLE_ABORT_ON_PANIC == 0, то при фатальной проблеме
 * будет возвращена ошибка FPTA_WANNA_DIE, клиентскому коду следует её
 * обработать и максимально быстро завершить выполнение.
 *
 * Если FPTA_ENABLE_ABORT_ON_PANIC != 0, то при фатальной проблеме будет
 * вызвана системная abort().
 *
 * На платформах с поддержкой __attribute__((weak)) поведение может быть
 * дополнительно изменено перекрытием функции fpta_panic(). */
#define FPTA_ENABLE_ABORT_ON_PANIC 1
#endif /* FPTA_ENABLE_ABORT_ON_PANIC */

#ifdef __cplusplus
#include <array>  // for std::array
#include <string> // for std::string

extern "C" {
#endif

//----------------------------------------------------------------------------
/* Общие перечисления и структуры */

/* Основные ограничения, константы и их производные. */
enum fpta_bits {
  /* Максимальное кол-во таблиц */
  fpta_tables_max = 1024,
  /* Максимальное кол-во колонок (порядка 1000) */
  fpta_max_cols = fptu_max_cols,
  /* Максимальный размер строки/записи в байтах */
  fpta_max_row_bytes = fptu_max_tuple_bytes,
  /* Максимальная длина значения колонки в байтах */
  fpta_max_col_bytes = fptu_max_opaque_bytes,
  /* Максимальное кол-во элементов в массиве */
  fpta_max_array_len = fptu_max_array_len,

  /* Максимальная длина ключа и/или индексируемого поля. Ограничение
   * актуально только для упорядоченных индексов для строк, бинарных данных
   * переменной длины, а также для составных упорядоченных индексов.
   *
   * ВАЖНО: При превышении заданной величины в индекс попадет только
   * помещающаяся часть ключа, с дополнением 64-битным хэшем остатка.
   * Поэтому в упорядоченных индексах будет нарушаться порядок сортировки
   * для длинных строк, больших бинарных данных и составных колонок с
   * подобными длинными значениями внутри.
   *
   * Для эффективного индексирования значений, у которых наиболее значимая
   * часть находится в конце (например доменных имен), предусмотрен
   * специальный тип реверсивных индексов. При этом ограничение сохраняется,
   * но ключи обрабатываются и сравниваются с конца.
   *
   * Ограничение можно немного "подвинуть" за счет производительности,
   * но нельзя убрать полностью. Также будет рассмотрен вариант перехода
   * на 128-битный хэш. */
  fpta_max_keylen = 64 * 1 - 8,

  /* Размер буфера достаточный для размещения любого ключа во внутреннем
   * представлении, в том числе размер буфера необходимого функции
   * fpta_get_column2buffer() для формирование fpta_value составной колонки. */
  fpta_keybuf_len = fpta_max_keylen + 8 + sizeof(void *) + sizeof(size_t),

  /* Минимальная длина имени/идентификатора */
  fpta_name_len_min = 1,
  /* Максимальная длина имени/идентификатора */
  fpta_name_len_max = 512,

  /* Далее внутренние технические детали. */
  fpta_id_bits = 64,

  fpta_column_typeid_bits = fptu_typeid_bits,
  fpta_column_typeid_shift = 0,
  fpta_column_typeid_mask = (1 << fptu_typeid_bits) - 1,

  fpta_column_index_bits = 5,
  fpta_column_index_shift = fpta_column_typeid_bits,
  fpta_column_index_mask = ((1 << fpta_column_index_bits) - 1)
                           << fpta_column_index_shift,

  fpta_name_hash_bits =
      fpta_id_bits - fpta_column_typeid_bits - fpta_column_index_bits,
  fpta_name_hash_shift = fpta_column_index_shift + fpta_column_index_bits,

  /* Максимальное кол-во индексов для одной таблице (порядка 500) */
  fpta_max_indexes = (1 << (fpta_id_bits - fpta_name_hash_bits)),

  /* Максимальное суммарное кол-во таблиц и всех вторичных индексов,
   * включая составные индексы/колонки */
  fpta_max_dbi = 32764 /* соответствует MDBX_MAX_DBI - FPTA_SCHEMA_DBI
    = (INT16_MAX - CORE_DBS) - 1 = 32767 - 2 - 1 */
};

/* Экземпляр БД.
 *
 * Базу следует открыть посредством fpta_db_open(), а по завершении всех
 * манипуляций закрыть через fpta_db_close().
 * Открытие и закрытие базы относительно дорогие операции. */
typedef struct fpta_db fpta_db;

/* Транзакция.
 *
 * Чтение и изменение данных всегда происходят в контексте транзакции.
 * В fpta есть три вида (уровня) транзакций: только для чтения, для
 * чтение-записи, для чтения-записи и изменения схемы БД.
 *
 * Транзакции инициируются посредством fpta_transaction_begin()
 * и завершается через fpta_transaction_end(). */
typedef struct fpta_txn fpta_txn;

/* Курсор для чтения и изменения данных.
 *
 * Курсор связан с диапазоном записей, которые выбираются в порядке одного
 * из индексов и опционально фильтруются (если задан фильтр). Курсор
 * позволяет "ходить" по записям, обновлять их и удалять.
 *
 * Открывается курсор посредством fpta_cursor_open(), а закрывается
 * соответственно через fpta_cursor_close(). */
typedef struct fpta_cursor fpta_cursor;

//----------------------------------------------------------------------------

/* Коды ошибок.
 * Список будет пополнен, а описания уточнены. */
enum fpta_error {
  FPTA_SUCCESS = 0,
  FPTA_OK = FPTA_SUCCESS,
  FPTA_ERRROR_BASE = 4242,

  FPTA_EOOPS
  /* Internal unexpected Oops */,
  FPTA_SCHEMA_CORRUPTED
  /* Schema is invalid or corrupted (internal error) */,
  FPTA_ETYPE
  /* Type mismatch (given value vs column/field or index) */,
  FPTA_DATALEN_MISMATCH
  /* Data length mismatch (given value vs data type) */,
  FPTA_KEY_MISMATCH
  /* Key mismatch while updating row via cursor */,
  FPTA_COLUMN_MISSING
  /* Required column missing */,
  FPTA_INDEX_CORRUPTED
  /* Index is inconsistent or corrupted (internal error) */,
  FPTA_NO_INDEX
  /* No (such) index for given column */,
  FPTA_SCHEMA_CHANGED
  /* Schema changed (transaction should be restared) */,
  FPTA_ECURSOR
  /* Cursor is not positioned */,
  FPTA_TOOMANY
  /* Too many columns or indexes (one of fpta's limits reached) */,
  FPTA_WANNA_DIE
  /* Failure while transaction rollback */,
  FPTA_TXN_CANCELLED
  /* Transaction already cancelled */,
  FPTA_SIMILAR_INDEX
  /* Adding index which is similar to one of the existing */,

  FPTA_NODATA = -1 /* No data or EOF was reached */,
  FPTA_DEADBEEF = UINT32_C(0xDeadBeef) /* Pseudo error for results by refs,
    mean `no value` returned */,

  /**************************************** Native (system) error codes ***/

  FPTA_ENOFIELD = FPTU_ENOFIELD,
  FPTA_ENOSPACE = FPTU_ENOSPACE,
  FPTA_EINVAL = FPTU_EINVAL /* Invalid argument */,

#if defined(_WIN32) || defined(_WIN64)
  FPTA_ENOMEM = 14 /* ERROR_OUTOFMEMORY */,
  FPTA_ENOIMP = 50 /* ERROR_NOT_SUPPORTED */,
  FPTA_EVALUE = 13 /* ERROR_INVALID_DATA */,
  FPTA_OVERFLOW = 534 /* ERROR_ARITHMETIC_OVERFLOW */,
  FPTA_EEXIST = 183 /* ERROR_ALREADY_EXISTS */,
  FPTA_ENOENT = 1168 /* ERROR_NOT_FOUND */,
  FPTA_EPERM = 1 /* ERROR_INVALID_FUNCTION */,
  FPTA_EBUSY = 170 /* ERROR_BUSY */,
  FPTA_ENAME = 123 /* ERROR_INVALID_NAME */,
  FPTA_EFLAG = 186 /* ERROR_INVALID_FLAG_NUMBER */,
#else
  FPTA_ENOMEM = ENOMEM /* Out of Memory (POSIX) */,
  FPTA_ENOIMP = ENOSYS /* Function not implemented (POSIX) */,
  FPTA_EVALUE =
      EDOM /* Mathematics argument out of domain of function (POSIX) */,
  FPTA_OVERFLOW =
      EOVERFLOW /* Value too large to be stored in data type (POSIX) */,
#ifdef ENOTUNIQ
  FPTA_EEXIST = ENOTUNIQ /* Name not unique on network */,
#else
  FPTA_EEXIST = EADDRINUSE /* Address already in use (POSIX) */,
#endif
  FPTA_ENOENT = ENOENT /* No such file or directory (POSIX) */,
  FPTA_EPERM = EPERM /* Operation not permitted (POSIX) */,
  FPTA_EBUSY = EBUSY /* Device or resource busy (POSIX) */,
  FPTA_ENAME = EKEYREJECTED /* FPTA_EINVAL */,
  FPTA_EFLAG = EBADRQC /* FPTA_EINVAL */,
#endif

  /************************************************* MDBX's error codes ***/
  FPTA_KEYEXIST = -30799 /* key/data pair already exists */,

  FPTA_NOTFOUND = -30798 /* key/data pair not found */,

  FPTA_DB_REF = -30797 /* wrong page address/number,
    this usually indicates corruption */,

  FPTA_DB_DATA = -30796 /* Located page was wrong data */,

  FPTA_DB_PANIC = -30795 /* Update of meta page failed
    or environment had fatal error */,

  FPTA_DB_MISMATCH = -30794 /* DB version mismatch libmdbx */,

  FPTA_DB_INVALID = -30793 /* File is not a valid LMDB file */,

  FPTA_DB_FULL = -30792 /* Environment mapsize reached */,

  FPTA_DBI_FULL = -30791 /* Too may DBI (maxdbs reached) */,

  FPTA_READERS_FULL = -30790 /* Too many readers (maxreaders reached) */,

  FPTA_TXN_FULL = -30788 /* Transaction has too many dirty pages,
    e.g. a lot of changes. */,

  FPTA_CURSOR_FULL = -30787 /* Cursor stack too deep (mdbx internal) */,

  FPTA_PAGE_FULL = -30786 /* Page has not enough space (mdbx internal) */,

  FPTA_DB_RESIZED = -30785 /* Database contents grew
    beyond environment mapsize */,

  FPTA_DB_INCOMPAT = -30784 /* Operation and DB incompatible (mdbx internal),
   This can mean:
     - The operation expects an MDBX_DUPSORT/MDBX_DUPFIXED database.
     - Opening a named DB when the unnamed DB has MDBX_DUPSORT/MDBX_INTEGERKEY.
     - Accessing a data record as a database, or vice versa.
     - The database was dropped and recreated with different flags. */,

  FPTA_BAD_RSLOT = -30783 /* Invalid reuse of reader locktable slot */,

  FPTA_BAD_TXN = -30782 /* Transaction must abort,
    e.g. has a child, or is invalid */,

  FPTA_BAD_VALSIZE = -30781 /* Unsupported size of key/DB name/data,
    or wrong DUPFIXED size */,

  FPTA_BAD_DBI = -30780 /* The specified DBI was changed unexpectedly */,

  FPTA_DB_PROBLEM = -30779 /* Unexpected internal mdbx problem,
    txn should abort */,

  FPTA_EMULTIVAL = -30421 /* the mdbx_put() or mdbx_replace() was called
    for a key, that has more that one associated value. */,

  FPTA_EBADSIGN = -30420 /* wrong signature of a runtime object(s) */,
};

/* Возвращает краткое описание ошибки по её коду.
 *
 * Функция идентифицирует "свои" ошибки и при необходимости по-цепочке
 * вызывает mdbx_strerror() и системную strerror().
 *
 * Функция потоко-НЕ-безопасна в случае системной ошибки, так как при
 * этом вызывается потоко-НЕ-безопасная системная strerror(). */
FPTA_API const char *fpta_strerror(int errnum);

/* Потоко-безопасный вариант fpta_strerror().
 *
 * Функция потоко-безопасна в том числе в случае системной ошибки, так
 * как при этом вызывается потоко-безопасная системная strerror_r(). */
FPTA_API const char *fpta_strerror_r(int errnum, char *buf, size_t buflen);

/* Внутренняя функция, которая вызывается при фатальных ошибках и может
 * быть замещена на платформах с поддержкой __attribute__((weak)).
 * При таком замещении:
 *  - в errnum_initial будет передан первичный код ошибки,
 *    в результате которой потребовалась отмена транзакции.
 *  - в errnum_fatal будет передан вторичный код ошибки,
 *    которая случилась при отмене транзакции и привела к панике.
 *  - возврат НУЛЯ приведет к вызову системной abort(), ИНАЧЕ выполнение
 *    будет продолжено с генерацией кода ошибки FPTA_WANNA_DIE. */
FPTA_API int fpta_panic(int errnum_initial, int errnum_fatal);

//----------------------------------------------------------------------------

/* Типы данных для ключей (проиндексированных полей) и значений
 * для сравнения в условиях фильтров больше/меньше/равно/не-равно. */
typedef enum fpta_value_type {
  fpta_null,       /* "Пусто", в том числе для проверки присутствия
                    * или отсутствия колонки/поля в строке. */
  fpta_signed_int, /* Integer со знаком, задается в int64_t */
  fpta_unsigned_int, /* Беззнаковый integer, задается в uint64_t */
  fpta_datetime,    /* Время в форме fptu_time */
  fpta_float_point, /* Плавающая точка, задается в double */
  fpta_string, /* Строка utf8, задается адресом и длиной,
                * без терминирующего нуля!
                * (объяснение см внутри fpta_value) */
  fpta_binary, /* Бинарные данные, задается адресом и длиной */
  fpta_shoved, /* Преобразованный длинный ключ из индекса. */
  fpta_begin, /* Псевдо-тип, всегда меньше любого значения.
               * Используется при открытии курсора для выборки
               * первой записи посредством range_from. */
  fpta_end, /* Псевдо-тип, всегда больше любого значения.
             * Используется при открытии курсора для выборки
             * последней записи посредством range_to. */
  fpta_invalid /* Псевдо-тип для обозначения разрушенных экземпляров
                * или ошибочных результатов */
} fpta_value_type;

/* Структура-контейнер для представления значений.
 *
 * В том числе для передачи ключей (проиндексированных полей)
 * и значений для сравнения в условия больше/меньше/равно/не-равно. */
typedef struct fpta_value {
  fpta_value_type type;
  unsigned binary_length;
  union {
    void *binary_data;
    int64_t sint;
    uint64_t uint;
    double_t fp;
    fptu_time datetime;

    /* ВАЖНО! К большому сожалению и грандиозному неудобству, строка
     * здесь НЕ в традиционной для C форме, а БЕЗ терминирующего
     * нуля с явным указанием длины в binary_length.
     *
     * Причины таковы:
     *  - fpta_value также используется для возврата значений из индексов;
     *  - в индексах и ключах строки без терминирующего нуля;
     *  - поддержка двух форм/типов строк только увеличивает энтропию.
     *
     * Однако, в реальности терминирующий ноль:
     *  - будет на конце строк расположенных внутри
     *    строк таблицы (кортежах);
     *  - отсутствует в строках от fpta_cursor_key();
     *  - в остальных случаях - как было подготовлено вашим кодом;
     */
    const char *str;
  };
#ifdef __cplusplus
  /* TODO: constructors from basic types (на самом деле через отдельный
   * дочерний класс, так как Clang капризничает и не позволяет возвращать из
   * C-linkage функции тип, у которого есть конструкторы C++). */

  bool is_number() const {
    const int number_mask = (1 << fpta_unsigned_int) | (1 << fpta_signed_int) |
                            (1 << fpta_float_point);
    return (number_mask & (1 << type)) != 0;
  }

  bool is_negative() const {
    assert(is_number());
    const int signed_mask = (1 << fpta_signed_int) | (1 << fpta_float_point);
    return sint < 0 && (signed_mask & (1 << type));
  }

  inline fpta_value negative() const;
  fpta_value operator-() const { return negative(); }

#endif
} fpta_value;

/* Конструктор value с целочисленным значением. */
static __inline fpta_value fpta_value_sint(int64_t value) {
  fpta_value r;
  r.type = fpta_signed_int;
  r.binary_length = ~0u;
  r.sint = value;
  return r;
}

/* Конструктор value с беззнаковым целочисленным значением. */
static __inline fpta_value fpta_value_uint(uint64_t value) {
  fpta_value r;
  r.type = fpta_unsigned_int;
  r.binary_length = ~0u;
  r.uint = value;
  return r;
}

/* Конструктор value для datetime. */
static __inline fpta_value fpta_value_datetime(fptu_time datetime) {
  fpta_value r;
  r.type = fpta_datetime;
  r.binary_length = ~0u;
  r.datetime = datetime;
  return r;
}

/* Конструктор value с плавающей точкой. */
static __inline fpta_value fpta_value_float(double_t value) {
  fpta_value r;
  r.type = fpta_float_point;
  r.binary_length = ~0u;
  r.fp = value;
  return r;
}

/* Конструктор value со строковым значением из c-str,
 * строка не копируется и не хранится внутри. */
static __inline fpta_value fpta_value_cstr(const char *value) {
  size_t length = value ? strlen(value) : 0;
  assert(length < INT_MAX);
  fpta_value r;
  r.type = fpta_string;
  r.binary_length = (length < INT_MAX) ? (unsigned)length : INT_MAX;
  r.str = value;
  return r;
}

/* Конструктор value со строковым значением
 * строка не копируется и не хранится внутри. */
static __inline fpta_value fpta_value_string(const char *text, size_t length) {
  assert(strnlen(text, length) == length);
  assert(length < INT_MAX);
  fpta_value r;
  r.type = fpta_string;
  r.binary_length = (length < INT_MAX) ? (unsigned)length : INT_MAX;
  r.str = text;
  return r;
}

/* Конструктор value с бинарным/opaque значением,
 * даные не копируются и не хранятся внутри. */
static __inline fpta_value fpta_value_binary(const void *data, size_t length) {
  assert(length < INT_MAX);
  fpta_value r;
  r.type = fpta_binary;
  r.binary_length = (length < INT_MAX) ? (unsigned)length : INT_MAX;
  r.binary_data = (void *)data;
  return r;
}

/* Конструктор value с void/null значением. */
static __inline fpta_value fpta_value_null(void) {
  fpta_value r;
  r.type = fpta_null;
  r.binary_length = 0;
  r.binary_data = nullptr;
  return r;
}

/* Конструктор value с псевдо-значением "начало". */
static __inline fpta_value fpta_value_begin(void) {
  fpta_value r;
  r.type = fpta_begin;
  r.binary_length = ~0u;
  r.binary_data = nullptr;
  return r;
}

/* Конструктор value с псевдо-значением "конец". */
static __inline fpta_value fpta_value_end(void) {
  fpta_value r;
  r.type = fpta_end;
  r.binary_length = ~0u;
  r.binary_data = nullptr;
  return r;
}

/* Псевдо-деструктор fpta_value.
 * В случае успеха возвращает ноль, иначе код ошибки. */
static __inline int fpta_value_destroy(fpta_value *value) {
  if ((unsigned)value->type < (unsigned)fpta_invalid) {
    value->type = fpta_invalid;
    return FPTA_SUCCESS;
  }

  return FPTA_EINVAL;
}

/* Значение fpta_value совмещенное с буфером для удобного получения
 * составных ключей, используется для вызова fpta_get_column4key(). */
typedef struct fpta_value4key {
  fpta_value value;
  uint8_t key_buffer[fpta_keybuf_len];
} fpta_value4key;

//----------------------------------------------------------------------------
/* In-place numeric operations with saturation */

typedef enum fpta_inplace {
  fpta_saturated_add /* target = min(target + argument, MAX_TYPE_VALUE) */,
  fpta_saturated_sub /* target = max(target - argument, MIN_TYPE_VALUE) */,
  fpta_saturated_mul /* target = min(target * argument, MAX_TYPE_VALUE) */,
  fpta_saturated_div /* target = max(target / argument, MIN_TYPE_VALUE) */,
  fpta_min /* target = min(target, argument) */,
  fpta_max /* target = max(target, argument) */,
  fpta_bes /* Basic Exponential Smoothing, при этом коэффициент сглаживания
            * определяется дополнительным (третьим) аргументом.
            * https://en.wikipedia.org/wiki/Exponential_smoothing */,
} fpta_inplace;

//----------------------------------------------------------------------------
/* Designated empty, aka Denoted NILs */

/* Предназначение и использование.
 *
 * Для части базовых типов выделены значения, которые используются
 * для замены "пустоты" внутри fpta, в частности во вторичных индексах
 * при физическом отсутствии (NIL) соответствующих колонок в строках таблиц.
 * Такая замена необходима для сохранения простой и эффективной формы
 * индексов, без дополнительных расходов на хранение признаков "пустоты".
 *
 * Соответственно, для всех типов фиксированной длины возникает проблема
 * выбора значений, от использования которых следует отказаться в пользу
 * designated empty.
 * Одновременно, существует ряд технических сложностей для поддержки NIL
 * в индексах с контролем уникальности (чтобы включать такие строки в индекс,
 * но не считать их уникальными).
 *
 * Итоговое соломоново решение (10 заповедей):
 *   1) NIL значим для индексов с контролем уникальности,
 *      т.е. НЕ ДОПУСКАЕТСЯ наличие более одного NIL:
         - для первичных индексов NIL является одним из уникальных значений,
           и этого нельзя изменить.
         - для вторичных индексов NIL является одним из уникальных значений,
           либо не индексируется (опция при сборке библиотеки).
 *   2) При описании схемы таблиц, каждая колонка помечается
 *      как nullable или non-nullable.
 *   3) Для non-nullable колонок проверяется их наличие (FPTA_COLUMN_MISSING).
 *   4) Для индексируемых nullable-колонок НЕ ДОПУСКАЕТСЯ установка
 *      значений равных designated empty (FPTA_EVALUE),
 *      а для остальных колонок designated empty НЕ действует.
 *   5) При чтении и поиске НЕ ПРОИЗВОДИТСЯ контроль либо "чистка"
 *      значений равных designated empty.
 *   6) Для строк и типов переменной длины допускаются все значения,
 *      при этом явно различаются NIL и значение нулевой длины.
 *   7) Для знаковых целочисленных типов (int32, int64) в качестве designated
 *      empty используется соответствующие INT_MIN значения.
 *   8) Для типов плавающей точки (fp32/float, fp64/double)
 *      используется NaN-значение (тихое отрицательное бесконечное не-число,
 *      в бинарном виде «все единицы»).
 *   9) Для остальных типов (без знаковых и фиксированных бинарных) designated
 *      empty зависит от подвида индекса:
 *       - для obverse-индексов (сравнение от первого байта к последнему):
 *         designated empty = все нули и 0 для без знаковых целых.
 *       - для reverse-индексов (сравнение от последнего байта к первому):
 *         designated empty = все единицы и INT_MAX для без знаковых целых.
 *  10) При сортировке (просмотре через курсор) NIL-значения всегда следуют
 *      в естественном порядке за своим designated empty:
 *       - NIL меньше не-NIL во всех случаях, КРОМЕ reverse-индексов для
 *         без знаковых и фиксированных бинарных типов.
 *       - для без знаковых и фиксированных бинарных и reverse-индексе
 *         NIL больше не-NIL (так как на самом деле внутри "все единицы").
 *
 * ВАЖНО: Обратите внимание на наличие функции fpta_confine_number(),
 *        см описание ниже.
 *
 * Внутренние механизмы:
 *  - designated empty нужны только для индексирования NIL-значений,
 *    остальные ограничения, соглашения и манипуляции являются следствием
 *    необходимости не допускать или как-то контролировать использование
 *    значений designated empty.
 *  - для строк и типов переменной длины, значения в nullable колонках
 *    дополняются префиксом, с тем чтобы отличать NIL от пустых значений.
 *  - для плавающей точки учитывается усечение при конвертации из double
 *    во float, а именно:
 *     - в качестве designated empty используется бинарное значение
 *       "все единицы", что соответствует не-сигнальному (тихому)
 *       отрицательному NaN с максимальной мантиссой.
 *     - для double/fptu_fp64 дополнительных преобразований не выполняется,
 *       так как хранимый тип нативно совпадает с double внутри fpta_value;
 *     - для float/fptu_fp32 необходима конвертация из double изнутри
 *       fpta_value, при этом float-DENIL (все единицы) будет сформирован
 *       только если внутри fpta_value было значение double-DENIL.
 *       Для всех других значений, дающих float-DENIL при конвертации из
 *       double во float, будет записано ближайшее значение, но отличное от
 *       float-DENIL ("все единицы" за исключением младшего бита мантиссы).
 *     - таким образом, при сохранении из fpta_value во float/fptu_fp32
 *       действуют стандартные правила преобразования, а поведение
 *       обусловленное designated empty включается только когда в fpta_value
 *       форме double содержится double-DENIL значение.
 */

#define FPTA_DENIL_SINT32 INT32_MIN
#define FPTA_DENIL_SINT64 INT64_MIN
#define FPTA_DENIL_SINT FPTA_DENIL_SINT64

typedef union {
  uint64_t __i;
  double __d;
} fpta_fp64_t;
FPTA_API extern const fpta_fp64_t fpta_fp64_denil;

#define FPTA_DENIL_FP64_BIN UINT64_C(0xFFFFffffFFFFffff)

#ifndef _MSC_VER /* MSVC provides invalid nan() */
#define FPTA_DENIL_FP64_MAS "4503599627370495"
#endif /* ! _MSC_VER */

#if __GNUC_PREREQ(3, 3) || __has_builtin(nan)
#define FPTA_DENIL_FP (-__builtin_nan(FPTA_DENIL_FP64_MAS))
#else
#define FPTA_DENIL_FP (fpta_fp64_denil.__d)
#endif

#define FPTA_DENIL_FIXBIN_OBVERSE UINT8_C(0)
#define FPTA_DENIL_UINT16_OBVERSE UINT16_C(0)
#define FPTA_DENIL_UINT32_OBVERSE UINT32_C(0)
#define FPTA_DENIL_UINT64_OBVERSE UINT64_C(0)
#define FPTA_DENIL_UINT_OBVERSE FPTA_DENIL_UINT64_OBVERSE

#define FPTA_DENIL_FIXBIN_REVERSE UINT8_MAX
#define FPTA_DENIL_UINT16_REVERSE UINT16_MAX
#define FPTA_DENIL_UINT32_REVERSE UINT32_MAX
#define FPTA_DENIL_UINT64_REVERSE UINT64_MAX
#define FPTA_DENIL_UINT_REVERSE FPTA_DENIL_UINT64_REVERSE

#define FPTA_DENIL_DATETIME_BIN FPTU_DENIL_TIME_BIN
#define FPTA_DENIL_DATETIME FPTU_DENIL_TIME

//----------------------------------------------------------------------------
/* Открытие и закрытие БД */

/* Режим сохранности для изменений и БД в целом.
 *
 * Одновременно выбирает компромисс между производительностью по записи
 * и durability. */
typedef enum fpta_durability {

  fpta_readonly /* Только чтение, изменения запрещены. */,

  fpta_sync /* Полностью синхронная запись на диск.
              * Самый надежный и самый медленный режим.
              *
              * При завершении каждой транзакции формируется сильная точка
              * фиксации (выполняется fdatasync).
              *
              * Производительность по записи определяется скоростью диска,
              * ориентировано 500 TPS для SSD. */,

  fpta_lazy /* "Ленивый" режим записи.
              * Достаточно быстрый режим, но с риском потери последних
              * изменений при аварии.
              *
              * FIXME: Скорректировать описание после перехода на libmdbx
              * с "догоняющей" фиксацией.
              *
              * Периодически формируются сильные точки фиксации. В случае
              * системной аварии могут быть потеряны последние транзакции.
              *
              * Производительность по записи в основном определяется
              * скоростью диска, порядка 50K TPS для SSD. */,

  fpta_weak /* Самый быстрый режим, но без гарантий сохранности всей базы
              * при аварии.
              *
              * Cильные точки фиксации не формируются, а существующие стираются
              * в процессе сборки мусора.
              *
              * Ядро ОС, по своему усмотрению, асинхронно записывает измененные
              * страницы данных на диск. При этом ядро ОС обещает запись всех
              * изменений при сбое приложения, при срабатывании OOM-killer или
              * при штатной остановке системы. Но НЕ при сбое в ядре или при
              * отключении питания.
              *
              * Производительность по записи в основном определяется скоростью
              * CPU и RAM (более 100K TPS). */,
} fpta_durability;

/* Дополнительные флаги для оптимизации работы БД.
 *
 * Все опции можно комбинировать, но совместное использование нескольких
 * frendly-опции может как увеличить нагрузку на процессор, так и снизить
 * эффективность каждой из них из-за конфликта интересов. */
typedef enum fpta_regime_flags {
  fpta_regime_default = 0 /* Режим по-умолчанию */,
  fpta_frendly4writeback = 1, /* Для кеша обратной записи. Движок будет
                               * стремиться повторно использовать страницы в
                               * LIFO-порядке, что увеличивает эффективность
                               * работы кеша обратной записи. */
  fpta_frendly4hdd = 2 /* Для шпиндельных дисков. Движок будет стремиться
                               * выделять и повторно использовать
                               * страницы так, чтобы запись на диск была более
                               * последовательной. */,
  fpta_frendly4compaction = 4 /* Для освобождения места. Движок будет
                               * стремиться повторно использовать страницы
                               * размещенные ближе к началу БД, с тем чтобы
                               * иметь больше шансов освободить место в конце
                               * БД и уменьшить размер файла. */,
  fpta_saferam = 8 /* Защита от записи в ОЗУ. Данные будут отображены
                               * в память в режиме «только для чтения», что
                               * исключит возможность их непосредственного
                               * повреждения вследствие некорректно использования
                               * указателей в коде приложения. */,
  fpta_openweakness = 16 /* При открытии базы не производить откат
                          * к сильной точке фиксации. */,
} fpta_regime_flags;

/* Открывает базу по заданному пути и в durability режиме.
 *
 * Аргумент file_mode задает права доступа, которые используются
 * в случае создания новой БД.
 *
 * Аргумент megabytes задает размер БД в мегабайтах. При создании новой
 * БД за основным файлом БД резервируется указанное место. При открытии
 * существующей БД, в зависимости от заданного размера, может быть
 * приведено как увеличения файла, так и его усечение. Причем усечение
 * выполняется не более чем до последней использованной страницы.
 *
 * Аргумент alterable_schema определяет намерения по созданию и/или
 * удалению таблиц в процессе работы. Обещание "не менять схему"
 * позволяет отказаться от захвата fpta_rwl_t в процессе работы.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
FPTA_API int fpta_db_open(const char *path, fpta_durability durability,
                          fpta_regime_flags regime_flags, mode_t file_mode,
                          size_t megabytes, bool alterable_schema,
                          fpta_db **db);

/* Закрывает ранее открытую базу.
 *
 * На момент закрытия базы должны быть закрыты все ранее открытые
 * курсоры и завершены все транзакции.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
FPTA_API int fpta_db_close(fpta_db *db);

/* Возвращает очередное 64-битное значение из линейной последовательности
 * связанной с базой данных.
 *
 * Аргумент result служит для получения значение, а increment задает требуемое
 * приращение.
 *
 * Функция считывает в result текущее значение линейного счетчика, который
 * существует в контексте базы данных. После считывания к счетчику добавляется
 * значение аргумента increment. При переполнении счетчика будет возвращена
 * ошибка FPTA_NODATA, а в result попадет исходное значение счетчика.
 *
 * Измененное значение счетчика будет сохранено и станет видимыми из других
 * транзакций и процессов только после успешной фиксации транзакции. Если же
 * транзакция будет отменена, то все изменения будут потеряны.
 *
 * Поэтому, с одной стороны, отличное от нуля значение аргумента increment
 * допускается только для пишущих транзакций. С другой стороны, вызов функции
 * при нулевом значении increment позволяет произвести чтение счетчика
 * (в том числе) из читающей транзакции.
 *
 * Гарантируется, что в завершенных транзакциях при ненулевом increment
 * получаемые значения всегда будут увеличиваться, а при increment равном 1
 * будут строго последовательны. По этой причине, после завершения транзакции,
 * нет какой-либо возможности отмотать последовательность назад, без удаления
 * и повторного создания всей базы данных.
 *
 * Также см описание родственной функции fpta_table_sequence().
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
FPTA_API int fpta_db_sequence(fpta_txn *txn, uint64_t *result,
                              uint64_t increment);

//----------------------------------------------------------------------------
/* Инициация и завершение транзакций. */

/* Уровень доступа к данным из транзакции. */
typedef enum fpta_level {

  fpta_read = 1, /* Только чтение.
                  * Одновременно бесконфликтно могут выполняться
                  * несколько транзакций в режиме чтения. При
                  * этом они не блокируются пишущими транзакциями,
                  * как из этого же процесса, так и из других
                  * процессов работающих с БД.
                  *
                  * Однако, транзакция уровня изменения схемы
                  * всегда блокирует читающие транзакции в рамках
                  * того же процесса (но не в других процессах).
                  *
                  * При старте каждая читающая транзакция получает
                  * в свое распоряжение консистентный MVCC снимок
                  * всей БД, который видит до своего завершения.
                  * Другими словами, читающая транзакция не видит
                  * изменений в данных, которые произошли после
                  * её старта.
                  *
                  * По этой же причине следует избегать долгих
                  * читающих транзакций. Так как такая долгая
                  * транзакция производит удержание снимка-версии
                  * БД в линейной истории и этим приостанавливает
                  * переработку старых снимков (сборку мусора).
                  * Соответственно, долгая читающая транзакция
                  * на фоне большого темпа изменений может
                  * приводить к исчерпанию свободного места в БД. */

  fpta_write = 2, /* Чтение и изменение данных, но не схемы.
                   *
                   * Одновременно в одной БД может быть активна только
                   * одна транзакция изменяющая данные. Проще говоря,
                   * при старте такой транзакции захватывается
                   * глобальный мьютекс в разделяемой памяти.
                   *
                   * Пишущая транзакция никак не мешает выполнению
                   * читающих транзакций как в этом, так и в других
                   * процессах.
                   *
                   * Изменения сделанные из пишущей транзакции сразу
                   * видны в её контексте. Но для других транзакций и
                   * процессов они вступят в силу и будут видимы
                   * только после успешной фиксации транзакции.
                   *
                   * Пишущая транзакция может быть либо зафиксирована,
                   * либо отменена (при этом теряются все произведенные
                   * изменения). */

  fpta_schema = 3 /* Чтение, плюс изменение данных и схемы.
                   *
                   * Прежде всего, это пишущая транзакция, т.е.
                   * в пределах БД может быть активна только одна.
                   * Аналогично, транзакция изменения схемы может быть
                   * либо зафиксирована, либо отменена (с потерей всех
                   * изменений).
                   *
                   * Однако, транзакция изменения схемы также
                   * блокирует все читающие транзакции в рамках
                   * своего процесса посредством fpta_rwl_t.
                   * Такая блокировка обусловлена двумя причинами:
                   *  - спецификой движков libmdbx/LMDB (удаление
                   *    таблицы приводит к закрытию её разделяемого
                   *    внутри процесса дескриптора, и таким образом,
                   *    к нарушению принципов MVCC внутри процесса);
                   *  - ради упрощения реализации "Позитивных Таблиц";
                   *
                   * Инициация транзакции изменяющей схему возможна,
                   * только если при БД была открыта в соответствующем
                   * режиме (было задано alterable_schema = true).
                   *
                   * С другой стороны, обещание не менять схему
                   * (указание alterable_schema = false) позволяет
                   * экономить на захвате fpta_rwl_t при старте
                   * читающих транзакций. */
} fpta_level;

/* Инициация транзакции заданного уровня.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
FPTA_API int fpta_transaction_begin(fpta_db *db, fpta_level level,
                                    fpta_txn **txn);

/* Завершение транзакции.
 *
 * Аргумент abort для пишущих транзакций (уровней fpta_write и fpta_schema)
 * определяет будет ли транзакция зафиксирована или отменена.
 *
 * На момент завершения транзакции должны быть закрыты все связанные
 * с ней курсоры.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
FPTA_API int fpta_transaction_end(fpta_txn *txn, bool abort);
static __inline int fpta_transaction_commit(fpta_txn *txn) {
  return fpta_transaction_end(txn, false);
}
static __inline int fpta_transaction_abort(fpta_txn *txn) {
  return fpta_transaction_end(txn, true);
}

/* Получение версий схемы и данных.
 *
 * Для снимка базы (которая читается транзакцией)
 * и версию схемы (которая действует внутри транзакции).
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
FPTA_API int fpta_transaction_versions(fpta_txn *txn, uint64_t *db_version,
                                       uint64_t *schema_version);

//----------------------------------------------------------------------------
/* Управление схемой:
 *  - Под управлением схемой в libfpta подразумевается её изменение,
 *    т.е. создание либо удаление таблиц.
 *  - Изменение схемы происходит в рамках "пишущей" транзакции
 *    уровня fpta_schema.
 *  - При создании таблицы задается её уникальное имя, имена и тип колонок,
 *    а также индексы.
 *  - Для удаления таблицы достаточно только её имени.
 *  - В рамках одной транзакции можно создать и удалить любое разумное
 *    количество таблиц с пределах общих ограничений и доступных ресурсов.
 *  - Изменения вступают в силу и видны другим процессам только
 *    после фиксации транзакции.
 *
 * Колонки и индексы при создании таблиц:
 *  - Для простоты в libfpta описание индексов совмещено с описанием колонок.
 *  - Каждая колонка может иметь только один индекс, а таблица в целом должна
 *    иметь ровно один первичный индекс.
 *  - При создании таблицы набор её колонок и индексов задается структурой
 *    fpta_column_set, которая предварительно заполняется элементами при
 *    помощи fpta_column_describe().
 *  - При описании колонки указывается: её имя, тип данных, признак
 *    индексируемости (primary, secondary, none), вид индекса (ordered,
 *    unordered, obverse, reverse) и признак nullable.
 *  - Более подробно см описание индексации ниже.
 *
 * Хэш-коллизии в именах:
 *  - Имена таблиц и колонок подвергаются компактификации (сжатию).
 *  - Можно сказать, что из исходного имени формируется 54-битное
 *    хэш-значение. Соответственно, с вероятностью примерно
 *    1 к 100 миллионам возможна хэш-коллизия.
 *  - При возникновении такой коллизии, при создании таблицы будет
 *    возвращаться ошибка. Как если бы такая таблица уже существовала,
 *    или среди её колонок есть дубликат.
 */

/* Режимы индексирования для колонок таблиц.
 *
 * Ради достижения максимальной производительности libfpta не расходует
 * ресурсы на служебные колонки, в том числе на идентификаторы строк (ROW_ID).
 * Проще говоря, в libfpta в качестве ROW_ID используется первичный ключ.
 *
 * Поэтому вторичные индексы возможны только при первичном индексе с контролем
 * уникальности. Если естественным образом в таблице нет поля/колонки
 * подходящего на роль первичного ключа, то такую колонку необходимо добавить
 * и заполнять искусственными уникальными значениями, см функции
 * fpta_table_sequence() и fpta_db_sequence().
 *
 * Упорядоченные индексы:
 *   - это традиционные для B-Tree индексы.
 *   - ВАЖНО: в libfpta существует ограничения на длину ключа, при превышении
 *     которого будет нарушаться порядок для длинных данных. Более подробно см
 *     описание константы fpta_max_keylen.
 *
 * Неупорядоченные индексы:
 *   - fpta_primary_unique_unordered, fpta_primary_withdups_unordered;
 *   - fpta_secondary_unique_unordered, fpta_secondary_withdups_unordered.
 *
 *   Строятся посредством хеширования значений ключа. Такой индекс позволяет
 *   искать данные только по конкретному значению ключа. Основной бонус
 *   неупорядоченных индексов в минимизации накладных расходов для значений
 *   длиннее 8 байт, так как внутри БД все ключи становятся одинакового
 *   фиксированного размера (в том числе не хранится размер ключа).
 *
 * Индексы со сравнением ключей с конца:
 *   - fpta_primary_unique_ordered_reverse,
 *     fpta_primary_withdups_ordered_reverse;
 *   - fpta_secondary_unique_ordered_reverse,
 *     fpta_secondary_withdups_ordered_reverse.
 *
 *   Индексы этого типа применимы только для строк и бинарных данных (типы
 *   fptu_96..fptu_256, fptu_cstr и fptu_opaque При этом значения ключей
 *   сравниваются в обратном порядке байт. Не от первых к последним,
 *   а от последних к первым. Не следует пусть с обратным порядком сортировки.
 *
 *   Кроме этого, посредством признака reverse/obverse, для безнаковых типов
 *   и бинарных строк фиксированного размера, выбирается DENIL значение
 *   замещающее "пусто" при индексации nullable колонок, подробности далее.
 *
 * Nullable колонки:
 *   Для индексирования nullable колонок пустые значения материализуются. При
 *   этом в индекс вместо "пусто" помещаются специальные замещающие значения.
 *   Для типов фиксированного размера это предполагает резервирование DENIL
 *   значений, которые исключаются из множества допустимых значений колонки.
 *   Кроме этого, соответствующие DENIL значения также определяют порядок
 *   строк при их сортировке по nullable колонкам.
 *
 *   Для ряда типов выбор значения DENIL достаточно очевиден, например
 *   INT_MIN для целых со знаком и NaN для чисел плавающей точки. Однако,
 *   для беззнаковых целых и фиксированных байтовых строк, в зависимости от
 *   прикладной семантики данных, чаще всего, требуется выбор между значениями
 *   "все нули" и "все единицы". В связи с этим, для таких типов libfpta
 *   позволяет выбрать DENIL значение посредством признака reverse/obverse,
 *   более подробно см выше описание Designated empty.
 *
 *   Таким образом, для ряда типов (беззнаковых целых и байтовых строк
 *   фиксированного размера) признак reverse/obverse влияет на ПОРЯДОК
 *   сортировки nullable колонок, что не имеет смысла для unordered индексов.
 *
 *   Другими словами, для упорядоченных индексов признак obverse/reverse
 *   оказывает влияние на два аспекта поведения и соответственно является
 *   более значимым в сравнении с признаком nullable.
 *   Это обстоятельство отражено в именах идентификаторов:
 *     - для именования констант соответствующих УПОРЯДОЧЕННЫМ индексам
 *       используются суффиксы "obverse_nullable" и "reverse_nullable".
 *     - для именования констант соответствующих НЕ УПОРЯДОЧЕННЫМ индексам
 *       используются "nullable_obverse" и "nullable_reverse".
 *     - этим подчеркивается разница в поведении и большое влияние
 *       reverse/obverse на упорядоченные индексы.
 */
typedef enum fpta_index_type {
  /* Служебные флажки/битики для комбинаций */
  fpta_index_funique = 1 << fpta_column_index_shift,
  fpta_index_fordered = 2 << fpta_column_index_shift,
  fpta_index_fobverse = 4 << fpta_column_index_shift,
  fpta_index_fsecondary = 8 << fpta_column_index_shift,
  fpta_index_fnullable = 16 << fpta_column_index_shift,

  /* Колонка НЕ индексируется и в последствии не может быть указана
   * при открытии курсора как опорная. */
  fpta_index_none = 0,
  fpta_noindex_nullable = fpta_index_fnullable,
  fpta_tersely_composite =
      /* формирование коротких ключей для составных индексов,
       * подробности см. в описании fpta_describe_composite_index()
       */ fpta_index_fnullable,

  /* Первичный ключ/индекс.
   *
   * Колонка будет использована как первичный ключ таблицы. При создании
   * таблицы такая колонка должна быть задана одна, и только одна. Вторичные
   * ключи/индексы допустимы только при уникальности по первичному ключу. */

  /* с повторами */
  fpta_primary_withdups_ordered_obverse /* сравнение с первого байта */ =
      fpta_index_fordered + fpta_index_fobverse,
  fpta_primary_withdups_ordered_obverse_nullable =
      fpta_primary_withdups_ordered_obverse + fpta_index_fnullable,
  fpta_primary_withdups_ordered_reverse /* сравнение от последнего байта */ =
      fpta_primary_withdups_ordered_obverse - fpta_index_fobverse,
  fpta_primary_withdups_ordered_reverse_nullable =
      fpta_primary_withdups_ordered_reverse + fpta_index_fnullable,

  /* с контролем уникальности */
  fpta_primary_unique_ordered_obverse /* сравнение с первого байта */ =
      fpta_primary_withdups_ordered_obverse + fpta_index_funique,
  fpta_primary_unique_ordered_obverse_nullable =
      fpta_primary_unique_ordered_obverse + fpta_index_fnullable,
  fpta_primary_unique_ordered_reverse /* сравнение от последнего байта */ =
      fpta_primary_unique_ordered_obverse - fpta_index_fobverse,
  fpta_primary_unique_ordered_reverse_nullable =
      fpta_primary_unique_ordered_reverse + fpta_index_fnullable,

  /* неупорядоченный, с контролем уникальности */
  fpta_primary_unique_unordered =
      fpta_primary_unique_ordered_obverse - fpta_index_fordered,
  fpta_primary_unique_unordered_nullable_obverse =
      fpta_primary_unique_unordered + fpta_index_fnullable,
  fpta_primary_unique_unordered_nullable_reverse =
      fpta_primary_unique_unordered_nullable_obverse - fpta_index_fobverse,

  /* неупорядоченный с повторами */
  fpta_primary_withdups_unordered =
      fpta_primary_withdups_ordered_obverse - fpta_index_fordered,
  /* fpta_primary_withdups_unordered_nullable_reverse = НЕДОСТУПЕН,
   * так как битовая коминация совпадает с fpta_noindex_nullable */
  fpta_primary_withdups_unordered_nullable_obverse =
      fpta_primary_withdups_unordered + fpta_index_fnullable,

  /* Вторичный ключ/индекс.
   *
   * Для колонки будет поддерживаться вторичный индекс, т.е. будет создана
   * дополнительная служебная таблица с key-value проекцией на первичный
   * ключ. Поэтому каждый вторичный индекс линейно увеличивает стоимость
   * операций обновления данных.
   *
   * Вторичные индексы возможны только при первичном индексе с контролем
   * уникальности. Если естественным образом в таблице нет поля/колонки
   * подходящего на роль первичного ключа, то такую колонку необходимо
   * добавить и заполнять искусственными уникальными значениями. Например,
   * посмотреть в сторону fptu_datetime и fptu_now().
   */

  /* с повторами */
  fpta_secondary_withdups_ordered_obverse /* сравнение с первого байта */ =
      fpta_index_fsecondary + fpta_index_fordered + fpta_index_fobverse,
  fpta_secondary_withdups_ordered_obverse_nullable =
      fpta_secondary_withdups_ordered_obverse + fpta_index_fnullable,
  fpta_secondary_withdups_ordered_reverse /* сравнение от последнего байта */ =
      fpta_secondary_withdups_ordered_obverse - fpta_index_fobverse,
  fpta_secondary_withdups_ordered_reverse_nullable =
      fpta_secondary_withdups_ordered_reverse + fpta_index_fnullable,

  /* с контролем уникальности */
  fpta_secondary_unique_ordered_obverse /* сравнение с первого байта */ =
      fpta_secondary_withdups_ordered_obverse + fpta_index_funique,
  fpta_secondary_unique_ordered_obverse_nullable =
      fpta_secondary_unique_ordered_obverse + fpta_index_fnullable,
  fpta_secondary_unique_ordered_reverse /* сравнение от последнего байта */ =
      fpta_secondary_unique_ordered_obverse - fpta_index_fobverse,
  fpta_secondary_unique_ordered_reverse_nullable =
      fpta_secondary_unique_ordered_reverse + fpta_index_fnullable,

  /* неупорядоченный, с контролем уникальности */
  fpta_secondary_unique_unordered =
      fpta_secondary_unique_ordered_obverse - fpta_index_fordered,
  fpta_secondary_unique_unordered_nullable_obverse =
      fpta_secondary_unique_unordered + fpta_index_fnullable,
  fpta_secondary_unique_unordered_nullable_reverse =
      fpta_secondary_unique_unordered_nullable_obverse - fpta_index_fobverse,

  /* неупорядоченный с повторами */
  fpta_secondary_withdups_unordered =
      fpta_secondary_withdups_ordered_obverse - fpta_index_fordered,
  fpta_secondary_withdups_unordered_nullable_obverse =
      fpta_secondary_withdups_unordered + fpta_index_fnullable,
  fpta_secondary_withdups_unordered_nullable_reverse =
      fpta_secondary_withdups_unordered_nullable_obverse - fpta_index_fobverse
} fpta_index_type;

#if defined(___cplusplus) && __cplusplus >= 201103L
inline constexpr fpta_index_type operator|(const fpta_index_type a,
                                           const fpta_index_type b) {
  return (fpta_index_type)((unsigned)a | (unsigned)b);
}

inline constexpr fpta_index_type nullable(const fpta_index_type index) {
  return index | fpta_index_fnullable;
}
#endif /* __cplusplus >= 201103L */

/* Внутренний тип для сжатых описаний идентификаторов. */
typedef uint64_t fpta_shove_t;

/* Набор колонок для создания таблицы */
typedef struct fpta_column_set {
  /* Счетчик заполненных описателей. */
  unsigned count;
  /* Упакованные описатели колонок. */
  fpta_shove_t shoves[fpta_max_cols];
  /* Информация о составных колонках */
  uint16_t composites[fpta_max_cols];
} fpta_column_set;

/* Вспомогательная функция, проверяет корректность имени */
FPTA_API bool fpta_validate_name(const char *name);

/* Вспомогательная функция для создания колонок.
 *
 * Добавляет описание колонки в column_set.
 * Аргумент column_name задает имя колонки. Для совместимости в именах
 * таблиц и колонок допускаются символы: 0-9 A-Z a-z _
 * Начинаться имя должно с буквы. Регистр символов не различается.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
FPTA_API int fpta_column_describe(const char *column_name,
                                  enum fptu_type data_type,
                                  enum fpta_index_type index_type,
                                  fpta_column_set *column_set);

/* Вспомогательная функция для создания составных колонок.
 *
 * Добавляет описание составной псевдо-колонки в column_set, которая будет
 * состоять из обычных колонок, имена которых определяются вектором посредством
 * аргументов column_names_array и column_names_count.
 *
 * Аргумент composite_name задает имя колонки. Для совместимости в именах
 * таблиц и колонок допускаются символы: 0-9 A-Z a-z _
 * Начинаться имя должно с буквы. Регистр символов не различается.
 *
 * Составная колонка может состоять из двух и более обычных колонок таблицы,
 * которые уже должны быть добавлены в column_set на момент создания составной
 * колонки. Составные колонки не могут совпадать по набору составляющих. В том
 * числе одна составная колонка не может полностью включать множество колонок
 * другой составной колонки.
 *
 * Составная колонка может быть использована только для индексирования
 * по совокупному значению образующих её обычных колонок. Значение составной
 * колонки нельзя установить, либо получить в явном виде. Другими словами,
 * составная колонка является средством описания составного индекса. Поэтому
 * составная колонка не может быть неиндексируемой.
 *
 * Получаемый таким образом составной индекс, в свою очередь, может быть
 * использован как для ускорения поиска, так и для контроля уникальности
 * по совокупному значению нескольких колонок (сomposite unique constraint).
 *
 * Для составного индекса опция fpta_index_fnullable не имеет смысла. Поэтому
 * это же значение использовано для опции fpta_tersely_composite, которую
 * следует считать экспериментальной. В целом, опция fpta_tersely_composite
 * предназначена для уменьшения длины составных ключей, в некоторых
 * специфических случаях:
 *   - Если в составной индекс входит много колонок с типами данных
 *     фиксированной длины и среди их значений много NIL. В этом случае
 *     fpta_tersely_composite позволяет уменьшить длину составного ключа за
 *     счет использования однобайтых префиксов, вместо полных DENIL-значений.
 *     При этом меняется порядок сортировки, NIL значения становятся
 *     безусловно меньше остальных, вне зависимости от соответствующих
 *     замещающих DENIL-значений.
 *   - Если в составной индекс входит много колонок с типами данных переменной
 *     длины (допускающими нулевую длину) и большинство их значений не пустые.
 *     В этом случае fpta_tersely_composite позволяет уменьшить длину ключа
 *     за счет отказа от префиксов-разделителей. При этом составной индекс не
 *     только теряет способность различать NIL и значения нулевой длины, но и
 *     не видит границ полей внутри составного ключа. Иначе говоря, при
 *     индексации становятся неотличимыми наборы значений {ab, c} и {a, bc},
 *     равно как и {abc, ""} или {"", abc}.
 *
 * ВАЖНО: Составные индексы подчиняются ограничению на длину ключей, более
 *        подробно см описание константы fpta_max_keylen.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
FPTA_API int fpta_describe_composite_index(
    const char *composite_name, enum fpta_index_type index_type,
    fpta_column_set *column_set, const char *const column_names_array[],
    size_t column_names_count);

FPTA_API int fpta_describe_composite_index_va(const char *composite_name,
                                              enum fpta_index_type index_type,
                                              fpta_column_set *column_set,
                                              const char *first,
                                              const char *second,
                                              const char *third, ...);

/* Инициализирует column_set перед заполнением посредством
 * fpta_column_describe(). */
static __inline void fpta_column_set_init(fpta_column_set *column_set) {
  column_set->count = 0;
  column_set->shoves[0] = 0;
  column_set->composites[0] = 0;
}

/* Деструктор fpta_column_set.
 * В случае успеха возвращает ноль, иначе код ошибки. */
static __inline int fpta_column_set_destroy(fpta_column_set *column_set) {
  if (column_set != nullptr && column_set->count != FPTA_DEADBEEF) {
    column_set->count = (unsigned)FPTA_DEADBEEF;
    column_set->shoves[0] = 0;
    column_set->composites[0] = INT16_MAX;
    return FPTA_SUCCESS;
  }

  return FPTA_EINVAL;
}

/* Сбрасывает (повторно инициализирует) column_set для повторного заполнения
 * посредством fpta_column_describe(). */
static __inline int fpta_column_set_reset(fpta_column_set *column_set) {
  if (column_set != nullptr && column_set->count != FPTA_DEADBEEF) {
    fpta_column_set_init(column_set);
    return FPTA_SUCCESS;
  }

  return FPTA_EINVAL;
}

/* Создание таблицы.
 *
 * Аргумент table_name задает имя таблицы. Для совместимости в именах
 * таблиц и колонок допускаются символы: 0-9 A-Z a-z _
 * Начинаться имя должно с буквы.
 *
 * Аргумент column_set должен быть предварительно заполнен
 * посредством fpta_column_describe(). После создания таблицы column_set
 * не требуется и может быть разрушен.
 *
 * Требуется транзакция уровня fpta_schema. Изменения становятся
 * видимыми из других транзакций и процессов только после успешной
 * фиксации транзакции.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
FPTA_API int fpta_table_create(fpta_txn *txn, const char *table_name,
                               fpta_column_set *column_set);

/* Удаление таблицы.
 *
 * Требуется транзакция уровня fpta_schema. Изменения становятся
 * видимыми из других транзакций и процессов только после успешной
 * фиксации транзакции.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
FPTA_API int fpta_table_drop(fpta_txn *txn, const char *table_name);

//----------------------------------------------------------------------------
/* Отслеживание версий схемы,
 * Идентификаторы таблиц/колонок и их кэширование:
 *
 *  - Следует учитывать что схема и все данные в БД могут быть полностью
 *    изменены сторонним процессом. Такое изменение всегда происходит
 *    в контексте "пишущей" транзакции, т.е. изолированно от уже
 *    выполняющихся транзакций чтения.
 *
 *    Другими словами, следует считать, что вне контекста транзакции,
 *    схема и данные волатильны и обновляются асинхронно.
 *
 *  - Таким образом, по-хорошему, трансляция имен таблиц и колонок в
 *    их идентификаторы и фактические типы данных, должна выполняться
 *    в контексте транзакции, т.е. после её инициации.
 *
 *    Однако, изменение схемы происходит редко и не рационально
 *    выполнять такую трансляцию при каждом запросе.
 *
 *    Более того, такая трансляция не может быть эффективно реализована
 *    для всех сценариев получения и обновления данных без введения
 *    некого языка запросов и его интерпретатора.
 *
 *  - Поэтому часть задач по отслеживанию версий схемы и трансляции
 *    имен в идентификаторы переложена на пользователя, см далее.
 *
 *  - При выполнении запросов идентификация таблиц и колонок
 *    производится посредством полей структуры fpta_name.
 *
 *    В свою очередь, каждый экземпляр fpta_name:
 *     1) инициализируется посредством fpta_table_init(), либо
 *        fpta_column_init(), которые на вход получают соответственно
 *        имя таблицы или колонки.
 *     2) перед использованием обновляется в fpta_name_refresh(),
 *        которая выполняется в контексте конкретной транзакции.
 *
 *  - Функция fpta_name_refresh(), сравнивает версию схемы в текущей
 *    транзакции со значением внутри переданного fpta_name,
 *    и при их совпадении не производит каких-либо действий.
 *
 *  - Таким образом, пользователю предоставляются средства для
 *    эффективного кэширования зависящей от схемы информации,
 *    а также её обновления по необходимости.
 *
 *  - Обновление посредством fpta_name_refresh() производится автоматически
 *    внутри всех функций выполняющихся в контексте транзакции (который
 *    получают соответствующий аргумент). Поэтому, как правило, в явном
 *    ручном вызове необходимости нет, исключения указаны отдельно в описании
 *    соответствующих функций.
 */

/* Внутренний тип описывающий структуру (схему) таблицы
 * с отслеживанием версионности. */
struct fpta_table_schema;

/* Операционный идентификатор таблицы или колонки. */
typedef struct fpta_name {
  uint64_t version; /* версия схемы для кэширования. */
  fpta_shove_t shove; /* хэш имени и внутренние данные. */
  union {
    /* для таблицы */
    struct fpta_table_schema
        *table_schema; /* операционная копия схемы с описанием колонок */

    /* для колонки */
    struct {
      struct fpta_name *table; /* операционный идентификатор таблицы */
      unsigned num; /* номер поля в кортеже. */
    } column;
  };
} fpta_name;

/* Инициализирует операционный идентификатор таблицы.
 *
 * Подготавливает идентификатор таблицы к последующему использованию.
 *
 * Допустимо связать с одной таблицей, как с и её колонками, несколько
 * экземпляров идентификаторов. В том числе инициализировать и разрушать
 * их несколько раз в пределах одной транзакции. Однако это может быть
 * достаточно расточительно и неэффективно, так как приведет к множественным
 * лишним внутренним операциям, в том числе к выделению и освобождению
 * динамической памяти.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
FPTA_API int fpta_table_init(fpta_name *table_id, const char *name);

/* Инициализирует операционный идентификатор колонки.
 *
 * Подготавливает идентификатор колонки к последующему использованию,
 * при этом связывает его с идентификатором таблицы задаваемым в table_id.
 * В свою очередь table_id должен быть предварительно подготовлен
 * посредством fpta_table_init().
 *
 * Следует отметить, что идентификатор колонки становится связанным
 * с идентификатором таблицы до разрушения или повторной инициализации.
 * Это не следует (и не получится) использовать с другим идентификатором
 * таблицы, даже если другой идентификатор соответствует той-же таблице.
 *
 * Кроме этого, идентификатор колонки нельзя использовать после разрушения
 * "родительского" идентификатора таблицы, к которому он был привязан
 * при инициализации.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
FPTA_API int fpta_column_init(const fpta_name *table_id, fpta_name *column_id,
                              const char *name);

/* Получение и актуализация идентификаторов таблицы и колонки.
 *
 * Функция работает в семантике кэширования с отслеживанием версии
 * схемы.
 *
 * Перед первым обращением name_id должен быть инициализирован
 * посредством fpta_table_init(), fpta_column_init() либо fpta_schema_fetch().
 *
 * Аргумент column_id может быть нулевым. В этом случае он
 * игнорируется, и обрабатывается только table_id.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
FPTA_API int fpta_name_refresh(fpta_txn *txn, fpta_name *name_id);
FPTA_API int fpta_name_refresh_couple(fpta_txn *txn, fpta_name *table_id,
                                      fpta_name *column_id);

/* Сбрасывает закэшированное состояние идентификатора.
 *
 * Функция может быть полезна в некоторых специфических сценариях,
 * когда движок fpta не может самостоятельно сделать вывод о необходимости
 * обновления информации. Например, если идентификатор используется повторно
 * без полной инициализации, но с новым экземпляром базы.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
FPTA_API int fpta_name_reset(fpta_name *name_id);

/* Возвращает тип данных колонки из дескриптора имени */
static __inline fptu_type fpta_name_coltype(const fpta_name *column_id) {
  assert(column_id->column.num <= fpta_max_cols);
  return (fptu_type)(column_id->shove & fpta_column_typeid_mask);
}

/* Возвращает тип индекса колонки из дескриптора имени */
static __inline fpta_index_type fpta_name_colindex(const fpta_name *column_id) {
  assert(column_id->column.num <= fpta_max_cols);
  return (fpta_index_type)(column_id->shove & fpta_column_index_mask);
}

/* Проверяет является ли указанная колонка составной. */
static __inline bool fpta_column_is_composite(const fpta_name *column_id) {
  /* В текущей реализации у составных колонок тип fptu_null. */
  return fpta_name_coltype(column_id) == fptu_null;
}

/* Разрушает операционные идентификаторы таблиц и колонок. */
FPTA_API void fpta_name_destroy(fpta_name *id);

/* Возвращает очередное 64-битное значение из линейной, связанной с таблицей,
 * последовательности.
 *
 * Аргументом table_id выбирается требуемая таблица. Аргумент result служит
 * для получения значение, а increment задает требуемое приращение.
 *
 * Функция считывает в result текущее значение линейного счетчика, который
 * существует в контексте таблицы. После считывания к счетчику добавляется
 * значение аргумента increment. При переполнении счетчика будет возвращена
 * ошибка FPTA_NODATA, а в result попадет исходное значение счетчика.
 *
 * Измененное значение счетчика будет сохранено и станет видимыми из других
 * транзакций и процессов только после успешной фиксации транзакции. Если же
 * транзакция будет отменена, то все изменения будут потеряны.
 *
 * Поэтому, с одной стороны, отличное от нуля значение аргумента increment
 * допускается только для пишущих транзакций. С другой стороны, вызов функции
 * при нулевом значении increment позволяет произвести чтение счетчика
 * (в том числе) из читающей транзакции.
 *
 * Аргумент table_id перед первым использованием должен быть инициализирован
 * посредством fpta_table_init(). Однако, предварительный вызов
 * fpta_name_refresh() не обязателен.
 *
 * Гарантируется, что в завершенных транзакциях при ненулевом increment
 * получаемые значения всегда будут увеличиваться, а при increment равном 1
 * будут строго последовательны. По этой причине, после завершения транзакции,
 * нет какой-либо возможности отмотать последовательность назад, без потери
 * всех данных таблицы.
 *
 * Также см описание родственной функции fpta_db_sequence().
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
FPTA_API int fpta_table_sequence(fpta_txn *txn, fpta_name *table_id,
                                 uint64_t *result, uint64_t increment);

/* Очищает таблицу, удаляя все её записи с минимальным количеством операций.
 *
 * Аргументом table_id выбирается требуемая таблица, а аргумент reset_sequence
 * управляет сбросом состояния последовательности выдаваемой функцией
 * fpta_table_sequence().
 *
 * Функция быстро очищает таблицу, не итерируя её записи, а сразу отправляя
 * все страницы с данными в корзину.
 *
 * Аргумент table_id перед первым использованием должен быть инициализирован
 * посредством fpta_table_init(). Однако, предварительный вызов
 * fpta_name_refresh() не обязателен.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
FPTA_API int fpta_table_clear(fpta_txn *txn, fpta_name *table_id,
                              bool reset_sequence);

/* Расширенная информация о таблице */
typedef struct fpta_table_stat {
  uint64_t row_count /* количество строк */;
  uint64_t total_bytes /* занимаемое место */;
  size_t btree_depth /* высота b-tree */;
  size_t branch_pages /* количество не-листьевых страниц (со ссылками)*/;
  size_t leaf_pages /* количество листьевых страниц (с данными) */;
  size_t large_pages /* количество больших (вынужденно склеенных)
                          страниц для хранения длинных записей */;
} fpta_table_stat;

/* Возвращает информацию о таблице, в том числе количестве строк.
 *
 * Аргументом table_id выбирается требуемая таблица. Аргументы row_count
 * и stat опциональны (могут быть nullptr).
 *
 * Аргумент table_id перед первым использованием должен быть инициализирован
 * посредством fpta_table_init(). Однако, предварительный вызов
 * fpta_name_refresh() не обязателен.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
FPTA_API int fpta_table_info(fpta_txn *txn, fpta_name *table_id,
                             size_t *row_count, fpta_table_stat *stat);

/* Возвращает общее количество колонок в таблице и отдельно количество
 * составных колонок. Код ошибки возвращается отдельно от результата.
 *
 * Аргументом table_id выбирается требуемая таблица. Функция не производит
 * отслеживание версии схемы и не обновляет какую-либо информацию, а только
 * возвращает текущее закэшированное значение. Поэтому аргумент table_id
 * должен быть предварительно не только инициализирован, но и обязательно
 * обновлен посредством fpta_name_refresh().
 *
 * Аргументы total_columns и composite_count опциональны и могут быть равны
 * NULL. При их ненулевых значениях по соответствующим указателями будут
 * записаны результирующие значения:
 *  - общее количество колонок в таблице в total_columns;
 *  - количество составных колонок в composite_count.
 *
 * ВАЖНО: Порядок следования обычных и составных колонок не определен.
 *        Проще говоря, все виды колонок перемешаны и могут следовать
 *        в произвольном порядке.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
FPTA_API int fpta_table_column_count_ex(const fpta_name *table_id,
                                        unsigned *total_columns,
                                        unsigned *composite_count);

/* Возвращает общее количество колонок в таблице, включая составные колонки.
 *
 * Аргументом table_id выбирается требуемая таблица. Функция не производит
 * отслеживание версии схемы и не обновляет какую-либо информацию, а только
 * возвращает текущее закэшированное значение. Поэтому аргумент table_id
 * должен быть предварительно не только инициализирован, но и обязательно
 * обновлен посредством fpta_name_refresh().
 *
 * ВАЖНО: Порядок следования обычных и составных колонок не определен.
 *        Проще говоря, все виды колонок перемешаны и могут следовать
 *        в произвольном порядке.
 *
 * В случае успеха возвращает не-отрицательное количество колонок, либо
 * отрицательное значение как индикатор ошибки, при этом код ошибки
 * не специфицируется. */
static __inline int fpta_table_column_count(const fpta_name *table_id) {
  unsigned count;
  return fpta_table_column_count_ex(table_id, &count, nullptr) == FPTA_SUCCESS
             ? (int)count
             : -1;
}

/* Возвращает информацию о колонке в таблице, в том числе о составной колонке.
 *
 * Аргументом table_id выбирается требуемая таблица, а column задает колонку,
 * которые нумеруются подряд начиная с нуля.
 * Результат помещается в column_id, после чего его можно использовать для
 * fpta_name_coltype() и fpta_name_colindex().
 *
 * Функция работает вне контекста транзакции, не производит отслеживание
 * версии схемы и не обновляет какую-либо информацию, а только возвращает
 * текущее закэшированное значение. Поэтому аргумент table_id должен быть
 * предварительно не только инициализирован, но и обязательно обновлен
 * посредством fpta_name_refresh().
 *
 * ВАЖНО: Порядок следования обычных и составных колонок не определен.
 *        Проще говоря, все виды колонок перемешаны и могут следовать
 *        в произвольном порядке.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
FPTA_API int fpta_table_column_get(const fpta_name *table_id, unsigned column,
                                   fpta_name *column_id);

/* Возвращает количество обычных колонок входящих в составную колонку.
 * Код ошибки возвращается отдельно от результата.
 *
 * Аргументом composite_id выбирается требуемая составная колонка. Функция не
 * производит отслеживание версии схемы и не обновляет какую-либо информацию,
 * а только возвращает текущее закэшированное значение. Поэтому аргумент
 * composite_id должен быть предварительно не только инициализирован, но и
 * обязательно обновлен посредством fpta_name_refresh().
 *
 * Для возврата результата используется аргумент count, который не должен быть
 * нулевым.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
FPTA_API int fpta_composite_column_count_ex(const fpta_name *composite_id,
                                            unsigned *count);

/* Возвращает количество обычных колонок входящих в составную колонку.
 *
 * Аргументом composite_id выбирается требуемая составная колонка. Функция не
 * производит отслеживание версии схемы и не обновляет какую-либо информацию,
 * а только возвращает текущее закэшированное значение. Поэтому аргумент
 * composite_id должен быть предварительно не только инициализирован, но и
 * обязательно обновлен посредством fpta_name_refresh().
 *
 * В случае успеха возвращает не-отрицательное количество колонок, либо
 * отрицательное значение как индикатор ошибки, при этом код ошибки
 * не специфицируется. */
static __inline int fpta_composite_column_count(const fpta_name *composite_id) {
  unsigned count;
  return fpta_composite_column_count_ex(composite_id, &count) == FPTA_SUCCESS
             ? (int)count
             : -1;
}

/* Возвращает информацию об обычной колонке входящей в составную колонку.
 *
 * Аргументом composite_id выбирается требуемая составная колонка, а item
 * задает номер элемента внутри составной колонки. Нумерация начинается с нуля.
 * Результат помещается в column_id, после чего его можно использовать для
 * fpta_name_coltype() и fpta_name_colindex().
 *
 * Функция работает вне контекста транзакции, не производит отслеживание
 * версии схемы и не обновляет какую-либо информацию, а только возвращает
 * текущее закэшированное значение. Поэтому аргумент composite_id должен быть
 * предварительно не только инициализирован, но и обязательно обновлен
 * посредством fpta_name_refresh().
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
FPTA_API int fpta_composite_column_get(const fpta_name *composite_id,
                                       unsigned item, fpta_name *column_id);

/* Описание схемы, заполняется функцией fpta_schema_fetch().
 *
 * Фактически является массивом содержащим хэшированные имена таблиц,
 * по которым можно получить остальную информацию.
 *
 * В результате успешного вызова fpta_schema_fetch() будет установлено поле
 * tables_count и будут заполнены элементы массива tables_names[], однако
 * не полностью, а как если-бы для этого использовалась функция
 * fpta_table_init().
 *
 * Тем не менее, этого достаточно для доступа к каждой из таблиц, в том числе
 * вызовов fpta_name_refresh() и последующего получения информации о колонках
 * посредством fpta_table_column_count() и fpta_table_column_get().
 *
 * Непосредственно после вызова fpta_schema_fetch() заполненная структура
 * не требует какого-либо разрушения или дополнительного освобождения ресурсов.
 * Однако, после последующих вызовов fpta_name_refresh() для каждого
 * соответствующего элемента массива tables_names[] требуется вызов
 * fpta_name_destroy() для освобождения внутреннего описания колонок.
 * Поэтому рекомендуется абстрагироваться и для разрушения всегда использовать
 * деструктор fpta_schema_destroy(). Накладные расходы при этом минимальны. */
typedef struct fpta_schema_info {
  unsigned tables_count;
  fpta_name tables_names[fpta_tables_max];
} fpta_schema_info;

/* Позволяет получить информацию о всех созданных таблицах.
 *
 * Следует отметить, что функция не производит вызовов fpta_name_refresh()
 * внутри себя, хотя и работает в контексте транзакции. Какое поведение
 * позволяет не выполнять лишних операций, в том числе не выделять ресурсов.
 * Подробности см выше в описании структуры fpta_schema_info.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
FPTA_API int fpta_schema_fetch(fpta_txn *txn, fpta_schema_info *info);

/* Деструктор экземпляров fpta_schema_info.
 * В случае успеха возвращает ноль, иначе код ошибки. */
FPTA_API int fpta_schema_destroy(fpta_schema_info *info);

//----------------------------------------------------------------------------
/* Управление фильтрами. */

/* Варианты условий (типы узлов) фильтра: НЕ, ИЛИ, И, функция-предикат,
 * меньше, больше, равно, не равно... */
typedef enum fpta_filter_bits {
  fpta_node_not = -4,
  fpta_node_or = -3,
  fpta_node_and = -2,
  fpta_node_fncol = -1, /* предикат/функтор для одной колонки */
  fpta_node_fnrow = 0, /* предикат/функтор для всей строки/кортежа */
  fpta_node_lt = fptu_lt,
  fpta_node_gt = fptu_gt,
  fpta_node_le = fptu_le,
  fpta_node_ge = fptu_ge,
  fpta_node_eq = fptu_eq,
  fpta_node_ne = fptu_ne,
} fpta_filter_bits;

/* Фильтр, формируется пользователем как дерево из узлов-условий.
 *
 * Фильтр может быть пустым, состоять из одного узла или целого дерева
 * начиная с коревого узла типа И или ИЛИ.
 *
 * Текущую реализацию можно считать базовым вариантом для быстрого старта,
 * который в последствии может быть доработан. */
typedef struct fpta_filter {
  fpta_filter_bits type;

  union {
    /* вложенный узел фильтра для "НЕ". */
    struct fpta_filter *node_not;

    /* вложенная пара узлов фильтра для условий "И" и "ИЛИ". */
    struct {
      struct fpta_filter *a;
      struct fpta_filter *b;
    } node_or, node_and;

    /* параметры для вызова функтора/предиката для одной колонки. */
    struct {
      /* идентификатор колонки */
      fpta_name *column_id;
      /* Функция-предикат, получает указатель на найденное поле, либо nullptr
       * если такового не найдено нет, а также параметр arg.
       * Функция должна вернуть true, если значение колонки/поля
       * удовлетворяет критерию фильтрации. */
      bool (*predicate)(const fptu_field *column, void *arg);
      /* дополнительный аргумент для функции-предиката */
      void *arg;
    } node_fncol;

    /* параметры для вызова функтора/предиката для всей строки. */
    struct {
      /* Функция-предикат, получает указатель на строку, а также
       * параметры context и arg.
       * Функция должна вернуть true, если значение строки/кортежа
       * удовлетворяет критерию фильтрации. */
      bool (*predicate)(const fptu_ro *row, void *context, void *arg);
      /* дополнительные аргументы для функции-предиката */
      void *context;
      void *arg;
    } node_fnrow;

    /* параметры для условия больше/меньше/равно/не-равно. */
    struct {
      /* идентификатор колонки */
      fpta_name *left_id;
      /* значение для сравнения */
      fpta_value right_value;
    } node_cmp;
  };
} fpta_filter;

/* Проверка соответствия кортежа условию фильтра.
 *
 * Предполагается внутреннее использование, но функция также
 * доступна извне. */
FPTA_API bool fpta_filter_match(const fpta_filter *fn, fptu_ro tuple);

//----------------------------------------------------------------------------
/* Управление курсорами. */

/* Порядок по индексной колонке для строк видимых через курсор. */
typedef enum fpta_cursor_options {
  /* Без обязательного порядка. Требуется для неупорядоченных индексов,
   * для упорядоченных равносилен fpta_ascending */
  fpta_unsorted = 0,

  /* По-возрастанию */
  fpta_ascending = 1,

  /* По-убыванию */
  fpta_descending = 2,

  /* Дополнительный флаг, предотвращающий чтение и поиск/фильтрацию
   * данных при открытии курсора. Позволяет избежать лишних операций,
   * если известно, что сразу после открытия курсор будет перемещен. */
  fpta_dont_fetch = 4,

  fpta_unsorted_dont_fetch = fpta_unsorted | fpta_dont_fetch,
  fpta_ascending_dont_fetch = fpta_ascending | fpta_dont_fetch,
  fpta_descending_dont_fetch = fpta_descending | fpta_dont_fetch,
} fpta_cursor_options;

/* Создает и открывает курсор для доступа к строкам таблицы,
 * включая их модификацию и удаление. Открытый курсор должен быть
 * закрыт до завершения транзакции посредством fpta_cursor_close().
 *
 * Аргумент column_id определяет "опорную" колонку, по значениям которой
 * будут упорядочены видимые через курсор строки. Опорная колонка должна
 * иметь индекс, причем для курсоров с сортировкой индекс должен быть
 * упорядоченным. Таблица-источник, в которой производится поиск, задается
 * неявно через колонку.
 *
 * Передаваемый через column_id экземпляр fpta_name, как и все экземпляры
 * column_id внутри фильтра, перед первым использованием должны быть
 * инициализированы посредством fpta_column_init(). Но предварительный
 * вызов fpta_name_refresh() не требуется, обновление будет выполнено
 * автоматически.
 *
 * Аргументы range_from и range_to задают диапазон выборки по значению
 * ключевой колонки. При необходимости могут быть заданы значения
 * с псевдо-типами fpta_begin и fpta_end. Следует учитывать, что для
 * неупорядоченных индексов порядок следования (сортировки) строк не
 * определен. Поэтому для неупорядоченных индексов хотя-бы одна из границ
 * диапазона должна совпадать с fpta_begin или fpta_end. Соответственно, либо
 * в range_from должен быть задан fpta_begin, либо в range_to задан fpta_end.
 *
 * Для успешного открытия курсора соответствующая таблица должна быть
 * предварительно создана, а указанная колонка должна иметь индекс.
 *
 * Если курсор открывается без флага fpta_dont_fetch, то при открытии будет
 * произведено позиционирование с поиском и фильтрацией данных. При этом,
 * в случает отсутствия данных удовлетворяющих критерию выборки, будет
 * возвращена ошибка.
 *
 * Фильтр, использованный при открытии курсора, должен существовать и
 * не изменяться до закрытия курсора и всех его клонов/копий.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
FPTA_API int fpta_cursor_open(fpta_txn *txn, fpta_name *column_id,
                              fpta_value range_from, fpta_value range_to,
                              fpta_filter *filter, fpta_cursor_options op,
                              fpta_cursor **cursor);
FPTA_API int fpta_cursor_close(fpta_cursor *cursor);

/* Реализует применение паттерна "visitor" к выборке из таблицы.
 *
 * Используя параметры txn, column_id, range_from, range_to, filter и op
 * открывает внутренний курсор. При этом смысл и назначение всех параметров
 * совпадает с функцией fpta_cursor_open(). За исключением флага fpta_dont_fetch
 * в значении опций op, который теряет смысл и поэтому игнорируется.
 *
 * После открытия курсора, строки попадающие в условие выборки последовательно
 * передаются функтору, задаваемому параметром visitor. При этом параметр skip
 * задает количество строк пропускаемых в начале, а параметр limit ограничивает
 * количество последующих итераций вызова функтора. При необходимости функтор
 * может дополнительно прервать цикл обработки вернув ненулевое, т.е. отличное
 * от FPTA_SUCCESS, значение. В таком случае, цикл обработки будет прерван, а
 * значение полученное от функтора будет возвращено в качестве результата
 * работы всей функции.
 *
 * Параметры visitor_context и visitor_arg являются дополнительными аргументами
 * при вызове функтора и передаются "как есть".
 *
 * Параметры page_top, page_top и count являются выходными и опциональны. При
 * ненулевом значении этих указателей в них будут возвращены соответствующие
 * значения:
 *  1) В count будет сохранено количество основных итераций, иначе говоря
 *     количество строк, которые были переданы на обработку функтору.
 *  2) В page_top значение сортирующей колонки для организации постраничного
 *     "пролистывания" таблицы к началу, а именно:
 *        - fpta_value_null, если при открытии курсора была ошибка.
 *        - fpta_value_begin, если строк в выборке было меньше параметра skip,
 *          в том числе если выборка данных пуста.
 *        - ИНАЧЕ, значение сортирующей колонки из первой строки переданной
 *          функтору, с тем чтобы при формировании предыдущей "страницы"
 *          использовать полученное значение как range_to.
 *  3) В page_bottom значение сортирующей колонки для организации постраничного
 *     "пролистывания" к концу, а именно:
 *        - fpta_value_null, если при открытии курсора была ошибка.
 *        - fpta_value_end, если строк в выборке было меньше суммы параметров
 *          skip и limit, в том числе если выборка данных пуста.
 *        - ИНАЧЕ, значение сортирующей колонки из строки после последней
 *          переданной функтору, с тем чтобы при формировании следующей
 *          "страницы" использовать полученное значение как range_from.
 *          Стоит отметить, что если последняя переданная функтору строка
 *          также была последней в выборке, то в page_bottom также будет
 *          возвращено fpta_value_end.
 *
 * Нулевые значения параметров visitor или limit считаются недопустимыми.
 *
 * При возникновении ошибки возвращается её код. Либо FPTA_NODATA, если
 * в процессе итерирования будет достигнут конец данных. Либо ненулевой
 * результат полученый от функтора, если функтор прервал таким образом цикл
 * обработки.
 * Нулевое значение (FPTA_SUCCESS) возвращается только если цикл обработки
 * успешно завершился из-за достижения ограничения задаваемого параметром
 * limit и в выборке еще оставались необработанные строки. */
FPTA_API
int fpta_apply_visitor(
    fpta_txn *txn, fpta_name *column_id, fpta_value range_from,
    fpta_value range_to, fpta_filter *filter, fpta_cursor_options op,
    size_t skip, size_t limit, fpta_value *page_top, fpta_value *page_bottom,
    size_t *count, int (*visitor)(const fptu_ro *row, void *context, void *arg),
    void *visitor_context, void *visitor_arg);

/* Проверяет наличие за курсором данных.
 *
 * Отсутствие данных означает, что нет возможности их прочитать, изменить
 * или удалить, но не исключает возможности вставки и/или добавления
 * новых данных.
 *
 * При наличии данных возвращает FPTA_SUCCESS (0). При отсутствии данных или
 * неустановленном курсоре FPTA_NODATA (EOF). Иначе код ошибки. */
FPTA_API int fpta_cursor_eof(const fpta_cursor *cursor);

/* Возвращает состояние курсора, в том числе наличие за курсором данных.
 *
 * Отсутствие данных означает, что нет возможности их прочитать, изменить
 * или удалить, но не исключает возможности вставки и/или добавления
 * новых данных.
 *
 * Функция позволяет явно различать ситуации "курсор не был установлен"
 * и "конец данных". В этом её единственное отличие от fpta_cursor_eof().
 *
 * В случае успеха возвращает одно из значений, в зависимости от
 * состояния курсора:
 *  - FPTA_SUCCESS (0) = Если курсор был установлен и за ним есть данные;
 *  - FPTA_NODATA      = Если курсор был установлен и за ним нет данных;
 *  - FPTA_ECURSOR     = Если курсор не был установлен или не был открыт;
 *
 * Иначе возвращается код ошибки. */
FPTA_API int fpta_cursor_state(const fpta_cursor *cursor);

/* Возвращает количество строк попадающих в условие выборки курсора.
 *
 * Подсчет производится путем перестановки и пошагового движения курсора.
 * Операция затратна, стоимость порядка O(log(ALL) + RANGE),
 * где ALL - общее количество строк в таблице, а RANGE - количество строк
 * попадающее под первичный (range from/to) критерий выборки.
 *
 * Текущая позиция курсора не используется и сбрасывается перед возвратом,
 * как если бы курсор был открыл с опцией fpta_dont_fetch.
 *
 * Аргумент limit задает границу, при достижении которой подсчет прекращается.
 * Если limit равен 0, то поиск производится до первой подходящей строки.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
FPTA_API int fpta_cursor_count(fpta_cursor *cursor, size_t *count,
                               size_t limit);

/* Считает и возвращает количество дубликатов для ключа в текущей
 * позиции курсора, БЕЗ учета фильтра заданного при открытии курсора.
 *
 * За курсором должна быть текущая запись. Под дубликатами понимаются
 * запись с одинаковым значением ключевой колонки, что допустимо если
 * соответствующий индекс не требует уникальности.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
FPTA_API int fpta_cursor_dups(fpta_cursor *cursor, size_t *dups);

/* Возвращает строку таблицы, на которой стоит курсор.
 * В случае успеха возвращает ноль, иначе код ошибки. */
FPTA_API int fpta_cursor_get(fpta_cursor *cursor, fptu_ro *tuple);

/* Варианты перемещения курсора. */
typedef enum fpta_seek_operations {
  /* Перемещение по диапазону строк за курсором. */
  fpta_first,
  fpta_last,
  fpta_next,
  fpta_prev,

  /* Перемещение по дубликатам текущего значения ключа, т.е.
   * по набору строк, у которых значение ключевого поля совпадает
   * с текущей строкой. */
  fpta_dup_first,
  fpta_dup_last,
  fpta_dup_next,
  fpta_dup_prev,

  /* Перемещение с пропуском дубликатов, т.е.
   * переход осуществляется к строке со значением ключевого поля
   * отличным от текущего. */
  fpta_key_next,
  fpta_key_prev
} fpta_seek_operations;

/* Относительное перемещение курсора.
 *
 * Курсор перемещается к соответствующей строке в пределах диапазона и
 * с учетом фильтра, заданных при открытии курсора.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
FPTA_API int fpta_cursor_move(fpta_cursor *cursor, fpta_seek_operations op);

/* Перемещение курсора к заданному ключу или к строке с аналогичным
 * значением ключевой колонки.
 *
 * Курсор перемещается в позицию, в которой значение ключевой/сортирующей
 * колонки курсора соответствует задаваемому аргументами критерию
 * позиционирования, а также соответствует фильтру и диапазону, заданным при
 * открытии курсора.
 *
 * Аргумент exactly определяет, требуется ли поиск позиции с точным
 * совпадением значения ключевой колонки (exactly = true), либо курсор
 * достаточно переместить к ближайшей позиции (exactly = false):
 *  - Для "упорядоченных" курсоров (опции fpta_ascending и fpta_descending)
 *    поиск ближайшей позиции выполняется аналогично std::lower_bound(),
 *    с поправкой на заданный порядок сортировки строк.
 *  - Для НЕ "упорядоченных" курсоров возможен поиск только точного
 *    совпадения, но не ближайшего. Поэтому, в зависимости от опции
 *    FPTA_PROHIBIT_NEARBY4UNORDERED, будет выполнено точное позиционирование,
 *    либо будет возвращена ошибка FPTA_EINVAL.
 *
 * Аргументы key или row адресуют позицию, к которой необходимо переместить
 * курсор. Должен быть задан (не равен nullptr) один и только один из них.
 *
 * При этом использование аргумента row требует дополнительного пояснения:
 *  - При поиске используются значения НЕ ВСЕХ КОЛОНОК переданной строки,
 *    а значения ТОЛЬКО ИЗ ОДНОЙ ИЛИ ДВУХ ЕЁ КОЛОНОК, как описано ниже.
 *    Остальные значения могут отсутствовать.
 *  - Если связанный с курсором индекс обеспечивает уникальность значений,
 *    либо этот индекс первичный (primary), то будет использовано только
 *    значение ключевой/сортирующей колонки.
 *  - Если же связанный с курсором индекс допускает наличие дубликатов
 *    и при этом этот индекс вторичный (secondary), то дополнительно будет
 *    использовано значение колонки соответствующей первичному ключу.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
FPTA_API int fpta_cursor_locate(fpta_cursor *cursor, bool exactly,
                                const fpta_value *key, const fptu_ro *row);

/* Возвращает внутреннее значение ключа, которое соответствует
 * текущей позиции курсора.
 *
 * Функцию следует считать вспомогательно-отладочной. Возвращаемое значение
 * может не соответствовать значению в соответствующей колонки строки.
 * Так например, вместо длинной строки будет возвращено обрезанное бинарное
 * значение с хэшем в конце.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
FPTA_API int fpta_cursor_key(fpta_cursor *cursor, fpta_value *key);

//----------------------------------------------------------------------------
/* Манипуляция данными без курсоров. */

/* Возвращает одну строку с соответствующим значением в заданной ключевой
 * колонке. При получении одиночных строк функция дешевле в сравнении
 * с открытием курсора.
 *
 * Указанная посредством column_id колонка должна иметь индекс с контролем
 * уникальности. Таблица-источник, в которой производится поиск, задается
 * неявно через колонку.
 *
 * Аргумент column_id перед первым использованием должен
 * быть инициализированы посредством fpta_column_init().
 * Предварительный вызов fpta_name_refresh() не обязателен.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
FPTA_API int fpta_get(fpta_txn *txn, fpta_name *column_id,
                      const fpta_value *column_value, fptu_ro *row);

/* Опции при помещении или обновлении данных, т.е. для fpta_put(). */
typedef enum fpta_put_options {
  /* Вставить новую запись, т.е. не обновлять существующую.
   *
   * Будет возвращена ошибка, если указанный ключ уже присутствует в
   * таблице и соответствующий индекс требует уникальности. */
  fpta_insert,

  /* Не добавлять новую запись, а обновить существующую запись.
   *
   * Должна быть одна и только одна запись с соответствующим значением
   * ключевой колонке, иначе будет возвращена ошибка. */
  fpta_update,

  /* Обновить существующую запись, либо вставить новую.
   *
   * Будет возвращена ошибка если существует более одной записи
   * с соответствующим значением ключевой колонки. */
  fpta_upsert,

  /* Внутренний флажок для пропуска проверки вставляемой/обновляемой строки
   * на наличие в ней значения для не-nullable колонок.
   * Используется внутри fpta_validate_put() и fpta_cursor_probe_and_update()
   * с тем, чтобы избежать двойной такой проверки. */
  fpta_skip_nonnullable_check = 4

} fpta_put_options;

/* Базовая функция для вставки и обновления строк таблицы. При вставке или
 * обновлении одиночных строк функция дешевле в сравнении с открытием курсора.
 *
 * В зависимости от fpta_put_options выполняет вставку, либо обновление.
 * Для наглядности рекомендуется использовать функции-обертки определенные
 * ниже.
 *
 * ВАЖНО: Нарушение ограничений (constraints) уникальности ключей считается
 * грубой ошибкой в логике приложения и приведет к прерыванию транзакции,
 * т.е. откату всех изменений сделанных в рамках текущей транзакции. При
 * необходимости следует предварительно проверять изменения посредством
 * fpta_validate_put() в самом начале транзакции. Такое поведение имеет
 * две веские причины:
 *  1. Уже сделанные в рамках транзакции изменения могут быть связаны с
 *     попыткой нарушения ограничений уникальности, поэтому для
 *     согласованности данных на уровне приложения следует откатывать
 *     транзакцию целиком.
 *  2. При наличии вторичных индексов вставка/обновления строки требует
 *     обновления служебных "индексных" таблиц. Ради производительности
 *     при таких обновлениях libfpta не производит предварительную проверку
 *     соблюдения ограничений уникальности. Соответственно, нарушение этих
 *     ограничений обнаруживается когда часть изменений уже сделана и поэтому
 *     прерывание транзакции требуется для согласованности данных и индексов.
 *
 * Аргумент table_id перед первым использованием должен
 * быть инициализированы посредством fpta_table_init().
 * Предварительный вызов fpta_name_refresh() не обязателен.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
FPTA_API int fpta_put(fpta_txn *txn, fpta_name *table_id, fptu_ro row_value,
                      fpta_put_options op);

/* Базовая функция для проверки соблюдения ограничений (constraints) перед
 * вставкой и обновлением строк таблицы.
 *
 * В зависимости от fpta_put_options выполняет проверку ограничений
 * (constraints) как если бы выполнялась вставка или обновление, и таким
 * образом позволяет избежать соответствующих ошибок fpta_put(), которые
 * приводят к прерыванию транзакции. Для наглядности рекомендуется
 * использовать функции-обертки определенные ниже.
 *
 * Аргумент table_id перед первым использованием должен
 * быть инициализированы посредством fpta_table_init().
 * Предварительный вызов fpta_name_refresh() не обязателен.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
FPTA_API int fpta_validate_put(fpta_txn *txn, fpta_name *table_id,
                               fptu_ro row_value, fpta_put_options op);

static __inline int fpta_probe_and_put(fpta_txn *txn, fpta_name *table_id,
                                       fptu_ro row_value, fpta_put_options op) {
  int rc =
      fpta_validate_put(txn, table_id, row_value,
                        (fpta_put_options)(op | fpta_skip_nonnullable_check));
  if (rc == FPTA_SUCCESS)
    rc = fpta_put(txn, table_id, row_value, op);
  return rc;
}

/* Обновляет существующую строку таблицы. При обновлении одиночных строк
 * функция дешевле в сравнении с открытием курсора.
 *
 * Для колонок индексируемых с контролем уникальности наличие дубликатов не
 * допускается. Кроме этого, в любом случае, не допускается наличие полностью
 * идентичных строк.
 *
 * ВАЖНО: Нарушение ограничений (constraints) уникальности ключей считается
 * грубой ошибкой в логике приложения и приведет к прерыванию транзакции,
 * т.е. откату всех изменений сделанных в рамках текущей транзакции. При
 * необходимости следует предварительно проверять изменения посредством
 * fpta_validate_update_row() в самом начале транзакции. Такое поведение
 * имеет две веские причины:
 *  1. Уже сделанные в рамках транзакции изменения могут быть связаны с
 *     попыткой нарушения ограничений уникальности, поэтому для
 *     согласованности данных на уровне приложения следует откатывать
 *     транзакцию целиком.
 *  2. При наличии вторичных индексов вставка/обновления строки требует
 *     обновления служебных "индексных" таблиц. Ради производительности
 *     при таких обновлениях libfpta не производит предварительную проверку
 *     соблюдения ограничений уникальности. Соответственно, нарушение этих
 *     ограничений обнаруживается когда часть изменений уже сделана и поэтому
 *     прерывание транзакции требуется для согласованности данных и индексов.
 *
 * Аргумент table_id перед первым использованием должен
 * быть инициализированы посредством fpta_table_init().
 * Предварительный вызов fpta_name_refresh() не обязателен.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
static __inline int fpta_update_row(fpta_txn *txn, fpta_name *table_id,
                                    fptu_ro row_value) {
  return fpta_put(txn, table_id, row_value, fpta_update);
}

/* Проверяет соблюдение ограничений (constraints) перед обновлением
 * существующей строки таблицы.
 *
 * Выполняет проверку ограничений (constraints) как если бы выполнялось
 * обновление. Позволяет избежать соответствующих ошибок fpta_update_row(),
 * которые приводят к прерыванию транзакции.
 *
 * Аргумент table_id перед первым использованием должен
 * быть инициализированы посредством fpta_table_init().
 * Предварительный вызов fpta_name_refresh() не обязателен.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
static __inline int fpta_validate_update_row(fpta_txn *txn, fpta_name *table_id,
                                             fptu_ro row_value) {
  return fpta_validate_put(txn, table_id, row_value, fpta_update);
}

static __inline int fpta_probe_and_update_row(fpta_txn *txn,
                                              fpta_name *table_id,
                                              fptu_ro row_value) {
  return fpta_probe_and_put(txn, table_id, row_value, fpta_update);
}

/* Вставляет в таблицу новую строку. При вставке одиночных строк функция
 * дешевле в сравнении с открытием курсора.
 *
 * Для колонок индексируемых с контролем уникальности вставка дубликатов не
 * допускается. Кроме этого, в любом случае, не допускается вставка полностью
 * идентичных строк.
 *
 * ВАЖНО: Нарушение ограничений (constraints) уникальности ключей считается
 * грубой ошибкой в логике приложения и приведет к прерыванию транзакции,
 * т.е. откату всех изменений сделанных в рамках текущей транзакции. При
 * необходимости следует предварительно проверять изменения посредством
 * fpta_validate_insert_row() в самом начале транзакции. Такое поведение
 * имеет две веские причины:
 *  1. Уже сделанные в рамках транзакции изменения могут быть связаны с
 *     попыткой нарушения ограничений уникальности, поэтому для
 *     согласованности данных на уровне приложения следует откатывать
 *     транзакцию целиком.
 *  2. При наличии вторичных индексов вставка/обновления строки требует
 *     обновления служебных "индексных" таблиц. Ради производительности
 *     при таких обновлениях libfpta не производит предварительную проверку
 *     соблюдения ограничений уникальности. Соответственно, нарушение этих
 *     ограничений обнаруживается когда часть изменений уже сделана и поэтому
 *     прерывание транзакции требуется для согласованности данных и индексов.
 *
 * Аргумент table_id перед первым использованием должен
 * быть инициализированы посредством fpta_table_init().
 * Предварительный вызов fpta_name_refresh() не обязателен.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
static __inline int fpta_insert_row(fpta_txn *txn, fpta_name *table_id,
                                    fptu_ro row_value) {
  return fpta_put(txn, table_id, row_value, fpta_insert);
}

/* Проверяет соблюдение ограничений (constraints) перед вставкой
 * в таблицу новой строки.
 *
 * Выполняет проверку ограничений (constraints) как если бы выполнялась
 * вставка. Позволяет избежать соответствующих ошибок fpta_insert_row(),
 * которые приводят к прерыванию транзакции.
 *
 * Аргумент table_id перед первым использованием должен
 * быть инициализированы посредством fpta_table_init().
 * Предварительный вызов fpta_name_refresh() не обязателен.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
static __inline int fpta_validate_insert_row(fpta_txn *txn, fpta_name *table_id,
                                             fptu_ro row_value) {
  return fpta_validate_put(txn, table_id, row_value, fpta_insert);
}

static __inline int fpta_probe_and_insert_row(fpta_txn *txn,
                                              fpta_name *table_id,
                                              fptu_ro row_value) {
  return fpta_probe_and_put(txn, table_id, row_value, fpta_insert);
}

/* В зависимости от существования строки с заданным первичным ключом либо
 * обновляет её, либо вставляет новую. При вставке или обновлении одиночных
 * строк функция дешевле в сравнении с открытием курсора.
 *
 * Для колонок индексируемых с контролем уникальности наличие дубликатов не
 * допускается. Кроме этого, в любом случае, не допускается наличие полностью
 * идентичных строк.
 *
 * ВАЖНО: Нарушение ограничений (constraints) уникальности ключей считается
 * грубой ошибкой в логике приложения и приведет к прерыванию транзакции,
 * т.е. откату всех изменений сделанных в рамках текущей транзакции. При
 * необходимости следует предварительно проверять изменения посредством
 * fpta_validate_upsert_row() в самом начале транзакции. Такое поведение
 * имеет две веские причины:
 *  1. Уже сделанные в рамках транзакции изменения могут быть связаны с
 *     попыткой нарушения ограничений уникальности, поэтому для
 *     согласованности данных на уровне приложения следует откатывать
 *     транзакцию целиком.
 *  2. При наличии вторичных индексов вставка/обновления строки требует
 *     обновления служебных "индексных" таблиц. Ради производительности
 *     при таких обновлениях libfpta не производит предварительную проверку
 *     соблюдения ограничений уникальности. Соответственно, нарушение этих
 *     ограничений обнаруживается когда часть изменений уже сделана и поэтому
 *     прерывание транзакции требуется для согласованности данных и индексов.
 *
 * Аргумент table_id перед первым использованием должен
 * быть инициализированы посредством fpta_table_init().
 * Предварительный вызов fpta_name_refresh() не обязателен.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
static __inline int fpta_upsert_row(fpta_txn *txn, fpta_name *table_id,
                                    fptu_ro row_value) {
  return fpta_put(txn, table_id, row_value, fpta_upsert);
}

/* Проверяет соблюдение ограничений (constraints) перед upsert-операцией
 * для строки таблицы.
 *
 * Выполняет проверку ограничений (constraints) как если бы выполнялось
 * обновление существующей или вставка новой строки, в зависимости от
 * существования строки с заданным первичным ключом. Позволяет избежать
 * соответствующих ошибок fpta_upsert_row(), которые приводят к прерыванию
 * транзакции.
 *
 * Аргумент table_id перед первым использованием должен
 * быть инициализированы посредством fpta_table_init().
 * Предварительный вызов fpta_name_refresh() не обязателен.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
static __inline int fpta_validate_upsert_row(fpta_txn *txn, fpta_name *table_id,
                                             fptu_ro row_value) {
  return fpta_validate_put(txn, table_id, row_value, fpta_upsert);
}

static __inline int fpta_probe_and_upsert_row(fpta_txn *txn,
                                              fpta_name *table_id,
                                              fptu_ro row_value) {
  return fpta_probe_and_put(txn, table_id, row_value, fpta_upsert);
}

/* Удаляет указанную строку таблицы. При удалении одиночных строк функция
 * дешевле в сравнении с открытием курсора.
 *
 * Аргумент table_id перед первым использованием должен
 * быть инициализированы посредством fpta_table_init().
 * Предварительный вызов fpta_name_refresh() не обязателен.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
FPTA_API int fpta_delete(fpta_txn *txn, fpta_name *table_id, fptu_ro row_value);

//----------------------------------------------------------------------------
/* Манипуляция данными через курсоры. */

/* Обновляет строку в текущей позиции курсора.
 *
 * ВАЖНО-1: При обновлении НЕ допускается изменение значения ключевой колонки,
 * которая была задана посредством column_id при открытии курсора. При попытке
 * выполнить такое обновление будет возвращена ошибка FPTA_KEY_MISMATCH.
 *
 * В противном случае возникает ряд вопросов по состоянию и дальнейшему
 * поведению курсора, в частности:
 *  - на какую запись должен указывать курсор после такого обновления?
 *  - должен ли курсор следовать за измененным ключом?
 *    если да, то как он должен вести себя при последующих перемещениях?
 *  - должен ли курсор переходить на следующую запись?
 *    если да, то каким должно быть поведение без изменения ключевого поля?
 *    в какой момент и как сообщать об отсутствии "следующей" записи?
 *  - должен ли курсор повторно прочитать обновленную запись, если она была
 *    перемещена "вперед" в пределах текущей выборки.
 *
 * ВАЖНО-2: Нарушение ограничений (constraints) уникальности ключей считается
 * грубой ошибкой в логике приложения и приведет к прерыванию транзакции,
 * т.е. откату всех изменений сделанных в рамках текущей транзакции. При
 * необходимости следует предварительно проверять изменения посредством
 * fpta_cursor_validate_update() в самом начале транзакции. Такое поведение
 * имеет две веские причины:
 *  1. Уже сделанные в рамках транзакции изменения могут быть связаны с
 *     попыткой нарушения ограничений уникальности, поэтому для
 *     согласованности данных на уровне приложения следует откатывать
 *     транзакцию целиком.
 *  2. При наличии вторичных индексов вставка/обновления строки требует
 *     обновления служебных "индексных" таблиц. Ради производительности
 *     при таких обновлениях libfpta не производит предварительную проверку
 *     соблюдения ограничений уникальности. Соответственно, нарушение этих
 *     ограничений обнаруживается когда часть изменений уже сделана и поэтому
 *     прерывание транзакции требуется для согласованности данных и индексов.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
FPTA_API int fpta_cursor_update(fpta_cursor *cursor, fptu_ro new_row_value);

/* Проверяет соблюдение ограничений (constraints) перед обновлением
 * строки в текущей позиции курсора.
 *
 * Выполняет проверку ограничений (constraints) как если бы выполнялось
 * обновление. Позволяет избежать соответствующих ошибок fpta_cursor_update(),
 * которые приводят к прерыванию транзакции.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
FPTA_API int fpta_cursor_validate_update_ex(fpta_cursor *cursor,
                                            fptu_ro new_row_value,
                                            fpta_put_options op);

static __inline int fpta_cursor_validate_update(fpta_cursor *cursor,
                                                fptu_ro new_row_value) {
  return fpta_cursor_validate_update_ex(cursor, new_row_value, fpta_update);
}

static __inline int fpta_cursor_probe_and_update(fpta_cursor *cursor,
                                                 fptu_ro new_row_value) {
  int rc = fpta_cursor_validate_update_ex(
      cursor, new_row_value,
      (fpta_put_options)(fpta_update | fpta_skip_nonnullable_check));
  if (rc == FPTA_SUCCESS)
    rc = fpta_cursor_update(cursor, new_row_value);
  return rc;
}

/* Удаляет из таблицы строку соответствующую текущей позиции курсора.
 * После удаления курсор перемещается к следующей записи.
 *
 * За курсором должна быть текущая запись, иначе будет возвращена ошибка.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
FPTA_API int fpta_cursor_delete(fpta_cursor *cursor);

/* Обновляет значение колонки в текущей позиции курсора, выполняя бинарную
 * операцию c аргументом и текущим значением.
 *
 * Аргумент column_id идентифицирует целевую колонку, но не может совпадать
 * с ключевой колонкой курсора.
 * Обновление строки в текущей позиции курсора выполняется с предварительной
 * проверкой ограничений, если таковые связаны с обновляемой колонкой.
 *
 * Требуемая операция задается аргументом op, а второй операнд параметром value.
 * ВАЖНО: Для операции fpta_bes (Basic Exponential Smoothing) необходимо
 * передать дополнительный параметр, который задает коэффициент сглаживания.
 * Этот дополнительные параметр может быть передан в двух вариантах:
 *  - В формате плавающей точки, в диапазоне (0..1), исключая крайние точки.
 *    При этом непосредственно задается значение коэффициент сглаживания.
 *  - В виде отрицательного int64_t значения в диапазоне (-24..0),
 *    также исключая крайние точки. При этом коэффициент сглаживания вычисляется
 *    как "2 в степени N", где N - переданное значение.
 *
 * Возвращаемое значение:
 *  - FPTA_SUCCESS (ноль) если значение колонки было успешно обновлено.
 *  - FPTA_NODATA (-1) если значение не изменилось и не было ошибок.
 *  - Иначе код ошибки. */
FPTA_API int fpta_cursor_inplace(fpta_cursor *cursor, fpta_name *column_id,
                                 const fpta_inplace op, const fpta_value value,
                                 ...);

//----------------------------------------------------------------------------
/* Манипуляция данными внутри строк. */

/* Конвертирует поле кортежа в "контейнер" fpta_value. */
FPTA_API fpta_value fpta_field2value(const fptu_field *pf);

/* При необходимости конвертирует тип и подгоняет числовое значение
 * под возможности колонки, в том числе с учетом её индекса и nullable.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
FPTA_API int fpta_confine_number(fpta_value *value, fpta_name *column_id);

/* Обновляет или добавляет в кортеж значение колонки.
 *
 * Аргумент column_id идентифицирует колонку и должен быть
 * предварительно подготовлен посредством fpta_name_refresh().
 * Внутри функции column_id не обновляется.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
FPTA_API int fpta_upsert_column(fptu_rw *pt, const fpta_name *column_id,
                                fpta_value value);

/* Получает значение указанной колонки из переданной строки таблицы (кортежа),
 * исключая составные колоноки.
 *
 * Аргумент column_id идентифицирует колонку и должен быть
 * предварительно подготовлен посредством fpta_name_refresh().
 * Внутри функции column_id не обновляется.
 *
 * Для численных типов, включая datetime, в value будет помещена
 * копия значения. В остальных случаях в value будет лишь указатель на
 * реальные данные, размещенные в исходной строке.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
FPTA_API int fpta_get_column(fptu_ro row_value, const fpta_name *column_id,
                             fpta_value *value);

/* Получает значение указанной колонки из переданной строки таблицы (кортежа),
 * в том числе для составных колонок.
 *
 * Аргумент column_id идентифицирует колонку и должен быть
 * предварительно подготовлен посредством fpta_name_refresh().
 * Внутри функции column_id не обновляется.
 *
 * Для численных типов, включая datetime, в value будет помещена
 * копия значения, без использования предоставленного буфера.
 * В остальных случаях данные будут скопированы в предоставленный буфер,
 * а внутри value будет указатель на эту копию.
 *
 * Для составных колонок, вне зависимости от количества входящих в неё колонок
 * и суммарного размера данных, требуется буфер фиксированного размера
 * определенного константной fpta_keybuf_len.
 *
 * Если размера предоставленного буфера недостаточно, то функция вернет
 * ошибку FPTA_DATALEN_MISMATCH, а в поле value->length будет сохранен
 * требуемый размер.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
FPTA_API int fpta_get_column2buffer(fptu_ro row, const fpta_name *column_id,
                                    fpta_value *value, void *buffer,
                                    size_t buffer_length);

static __inline int fpta_get_column4key(fptu_ro row, const fpta_name *column_id,
                                        fpta_value4key *value4key) {
  return fpta_get_column2buffer(row, column_id, &value4key->value,
                                value4key->key_buffer,
                                sizeof(value4key->key_buffer));
}

/* Обновляет значение колонки в переданном кортеже-строке, выполняя бинарную
 * операцию c аргументом и текущим значением колонки (поля кортежа).
 *
 * Аргумент column_id идентифицирует целевую колонку и должен быть
 * предварительно подготовлен посредством fpta_name_refresh().
 * Внутри функции column_id не обновляется.
 *
 * Требуемая операция задается аргументом op, а второй операнд параметром value.
 * ВАЖНО: Для операции fpta_bes (Basic Exponential Smoothing) необходимо
 * передать дополнительный параметр, который задает коэффициент сглаживания.
 * Этот дополнительные параметр может быть передан в двух вариантах:
 *  - В формате плавающей точки, в диапазоне (0..1), исключая крайние точки.
 *    При этом непосредственно задается значение коэффициент сглаживания.
 *  - В виде отрицательного int64_t значения в диапазоне (-24..0),
 *    также исключая крайние точки. При этом коэффициент сглаживания вычисляется
 *    как "2 в степени N", где N - переданное значение.
 *
 * Возвращаемое значение:
 *  - FPTA_SUCCESS (ноль) если значение колонки было успешно обновлено.
 *  - FPTA_NODATA (-1) если значение не изменилось и не было ошибок.
 *  - Иначе код ошибки. */
FPTA_API int fpta_column_inplace(fptu_rw *row, const fpta_name *column_id,
                                 const fpta_inplace op, const fpta_value value,
                                 ...);

//----------------------------------------------------------------------------
/* Некоторые внутренние служебные функции.
 * Доступны для специальных случаев, в том числе для тестов. */

FPTA_API int fpta_column_set_validate(fpta_column_set *column_set);
FPTA_API void fpta_pollute(void *ptr, size_t bytes, uintptr_t xormask);
FPTA_API fptu_lge __fpta_filter_cmp(const fptu_field *pf,
                                    const fpta_value *right);
FPTA_API int __fpta_index_value2key(fpta_shove_t shove, const fpta_value *value,
                                    void *key);
FPTA_API void *__fpta_index_shove2comparator(fpta_shove_t shove);

static __inline bool fpta_is_under_valgrind(void) {
  return fptu_is_under_valgrind();
}

FPTA_API uint64_t fpta_umul_64x64_128(uint64_t a, uint64_t b, uint64_t *h);
FPTA_API uint64_t fpta_umul_32x32_64(uint32_t a, uint32_t b);
FPTA_API uint64_t fpta_umul_64x64_high(uint64_t a, uint64_t b);

typedef struct fpta_version_info {
  uint8_t major;
  uint8_t minor;
  uint16_t release;
  uint32_t revision;
  struct {
    const char *datetime;
    const char *tree;
    const char *commit;
    const char *describe;
  } git;
} fpta_version_info;

typedef struct fpta_build_info {
  const char *datetime;
  const char *target;
  const char *cmake_options;
  const char *compiler;
  const char *compile_flags;
} fpta_build_info;

#if HAVE_FPTA_VERSIONINFO
extern FPTA_API const fpta_version_info fpta_version;
#endif /* HAVE_FPTA_VERSIONINFO */
extern FPTA_API const fpta_build_info fpta_build;

#ifdef __cplusplus
}

//----------------------------------------------------------------------------
/* Сервисные функции и классы для C++ (будет пополняться, существенно). */

namespace std {
FPTA_API string to_string(const fpta_error errnum);
FPTA_API string to_string(const fpta_value_type);
FPTA_API string to_string(const fpta_value *);
FPTA_API string to_string(const fpta_durability durability);
FPTA_API string to_string(const fpta_level level);
FPTA_API string to_string(const fpta_index_type index);
FPTA_API string to_string(const fpta_filter_bits bits);
FPTA_API string to_string(const fpta_cursor_options op);
FPTA_API string to_string(const fpta_seek_operations op);
FPTA_API string to_string(const fpta_put_options op);
FPTA_API string to_string(const fpta_name *);
FPTA_API string to_string(const fpta_filter *);
FPTA_API string to_string(const fpta_column_set *);
FPTA_API string to_string(const fpta_db *);
FPTA_API string to_string(const fpta_txn *);
FPTA_API string to_string(const fpta_cursor *);
FPTA_API string to_string(const struct fpta_table_schema *);

inline string to_string(const fpta_column_set &def) { return to_string(&def); }
inline string to_string(const fpta_value &value) { return to_string(&value); }
inline string to_string(const fpta_name &id) { return to_string(&id); }
inline string to_string(const fpta_filter &filter) {
  return to_string(&filter);
}
} // namespace std

inline fpta_regime_flags operator|(const fpta_regime_flags a,
                                   const fpta_regime_flags b) {
  return (fpta_regime_flags)((unsigned)a | (unsigned)b);
}

inline fpta_value fpta_value::negative() const {
  if (type == fpta_signed_int)
    return fpta_value_sint(-sint);
  else if (type == fpta_float_point)
    return fpta_value_float(-fp);
  else {
    assert(false);
    return fpta_value_null();
  }
}

inline fpta_value fpta_value_str(const std::string &str) {
  return fpta_value_string(str.data(), str.length());
}

namespace fpta {

template <typename First, typename Second, typename... More>
inline int
describe_composite_index(const char *composite_name, fpta_index_type index_type,
                         fpta_column_set *column_set, const First &first,
                         const Second &second, const More &... more) {
  const std::array<const char *, sizeof...(More) + 2> array{
      {first, second, more...}};
  return fpta_describe_composite_index(composite_name, index_type, column_set,
                                       array.data(), array.size());
}

} // namespace fpta

#endif /* __cplusplus */

#ifdef _MSC_VER
#pragma pack(pop)
#pragma warning(pop)
#endif /* windows mustdie */

#endif /* FAST_POSITIVE_TABLES_H */

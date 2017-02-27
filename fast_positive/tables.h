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
 * Please see README.md
 *
 * "Позитивные таблицы" предназначены для построения высокоскоростных
 * локальных хранилищ структурированных данных в разделяемой памяти,
 * с целевой производительностью от 100К до 1000К простых SQL-подобных
 * запросов в секунду на каждом ядре процессора.
 *
 * The Future will Positive. Всё будет хорошо.
 *
 * "Positive Tables" is designed to build high-speed local storage of
 * structured data in shared memory, with target performance from 100K
 * to 1000K simple SQL-like requests per second on each CPU core.
 */

#pragma once
#ifndef FAST_POSITIVE_TABLES_H
#define FAST_POSITIVE_TABLES_H

#include "fast_positive/config.h"
#include "fast_positive/defs.h"
#include "fast_positive/tuples.h"

#include <assert.h> // for assert()
#include <errno.h>  // for error codes
#include <limits.h> // for INT_MAX
#include <string.h> // for strlen()

#if defined(HAVE_SYS_STAT_H) && !defined(_WIN32) && !defined(_WIN64)
#include <sys/stat.h> // for mode_t
#else
typedef unsigned mode_t;
#endif

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(                                                             \
    disable : 4201 /* нестандартное расширение: структура (объединение) без имени */)
#pragma warning(                                                             \
    disable : 4820 /* timespec: "4"-байтовые поля добавлены после данные-член "timespec::tv_nsec" */)
#pragma warning(                                                             \
    disable : 4514 /* memmove_s: подставляемая функция, не используемая в ссылках, была удалена */)
#pragma warning(                                                             \
    disable : 4710 /* sprintf_s(char *const, const std::size_t, const char *const, ...): функция не является встроенной */)
#pragma warning(                                                             \
    disable : 4061 /* перечислитель "xyz" в операторе switch с перечислением "XYZ" не обрабатывается явно меткой выбора при наличии "default:" */)
#pragma warning(disable : 4127 /* условное выражение является константой */)
#pragma warning(                                                             \
    disable : 4711 /* function 'fptu_init' selected for automatic inline expansion*/)
#pragma pack(push, 1)
#endif /* windows mustdie */

//----------------------------------------------------------------------------
/* Опции конфигурации управляющие внутренним поведением libfpta, т.е
 * их изменение требует пересборки библиотеки.
 *
 * Чуть позже эти определения передедут в fpta_config.h */

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
#define FPTA_PROHIBIT_NEARBY4UNORDERED 0
#endif /* FPTA_PROHIBIT_NEARBY4UNORDERED */

#ifndef FPTA_PROHIBIT_LOSS_PRECISION
/* При обслуживании индексированных колонок типа float/fptu_fp32
 * double значения из fpta_value будут преобразованы в float.
 * Опция определяет, считать ли этом потерю точности ошибкой или нет. */
#define FPTA_PROHIBIT_LOSS_PRECISION 0
#endif /* FPTA_PROHIBIT_LOSS_PRECISION */

#ifndef FPTA_ENABLE_RETURN_INTO_RANGE
/* Опция определяет, поддерживать ли для курсора возврат в диапазон строк
 * после его исчерпания при итерировании. Например, позволить ли возврат
 * к последней строке посредством move(prev), после того как
 * move(next) вернул NODATA, так как курсор уже был на последней строке. */
#define FPTA_ENABLE_RETURN_INTO_RANGE 1
#endif /*FPTA_ENABLE_RETURN_INTO_RANGE */

#ifndef FPTA_ENABLE_ABORT_ON_PANIC
/* Опция определяет, что делать при фатальных ошибках, например в случае
 * ошибки отката/прерывания транзакции.
 *
 * Как правило, такие ошибки возникают при "росписи" памяти или при серьезных
 * нарушениях соглашений API. Лучшим выходом, в таких случаях, обычно является
 * скорейшее прерывания выполнения процесса. Согласованность данных при этом
 * обеспечивается откатом (не фиксацией) транзакции внутри MDBX.
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

  /* Максимальная длина ключа и/или индексируемого поля. Ограничение
   * актуально только для строк и бинарных данных переменной длины.
   *
   * При превышении заданной величины в индекс попадет только
   * помещающаяся часть ключа, с дополнением 64-битным хэшем остатка.
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

  /* Минимальная длина имени/идентификатора */
  fpta_name_len_min = 1,
  /* Максимальная длина имени/идентификатора */
  fpta_name_len_max = 42,

  /* Далее внутренние технические детали. */
  fpta_id_bits = 64,

  fpta_column_typeid_bits = fptu_typeid_bits,
  fpta_column_typeid_shift = 0,
  fpta_column_typeid_mask = (1 << fptu_typeid_bits) - 1,

  fpta_column_index_bits = 4,
  fpta_column_index_shift = fpta_column_typeid_bits,
  fpta_column_index_mask = ((1 << fpta_column_index_bits) - 1)
                           << fpta_column_index_shift,

  fpta_name_hash_bits =
      fpta_id_bits - fpta_column_typeid_bits - fpta_column_index_bits,
  fpta_name_hash_shift = fpta_column_index_shift + fpta_column_index_bits,

  /* Максимальное кол-во индексов для одной таблице (порядка 500) */
  fpta_max_indexes = (1 << (fpta_id_bits - fpta_name_hash_bits))
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
 * чтение-записи, для изменения схемы БД.
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
 * Открывается курсов посредством fpta_cursor_open(), а закрывается
 * соответственно через fpta_cursor_close(). */
typedef struct fpta_cursor fpta_cursor;

//----------------------------------------------------------------------------

/* Типы данных для ключей (проиндексированных полей) и значений
 * для сравнения в условиях фильтров больше/меньше/равно/не-равно. */
typedef enum fpta_value_type {
  fpta_null,       /* "Пусто", в том числе для проверки присутствия
                    * или отсутствия колонки/поля в строке. */
  fpta_signed_int, /* Integer со знаком, задается в int64_t */
  fpta_unsigned_int, /* Беззнаковый integer, задается в uint64_t */
  fpta_datetime, /* Время в форме fptu_time */
  fpta_float_point, /* Плавающая точка, задается в double */
  fpta_string, /* Строка utf8, задается адресом и длиной,
                * без терминирующего нуля!
                * (объяснение см внутри fpta_value) */
  fpta_binary, /* Бинарные данные, задается адресом и длиной */
  fpta_shoved, /* Преобразованный длинный ключ из индекса. */
  fpta_begin,  /* Псевдо-тип, всегда меньше любого значения.
                * Используется при открытии курсора для выборки
                * первой записи посредством range_from. */
  fpta_end,    /* Псевдо-тип, всегда больше любого значения.
                * Используется при открытии курсора для выборки
                * последней записи посредством range_to. */
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
    double fp;
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
static __inline fpta_value fpta_value_float(double value) {
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
static __inline fpta_value fpta_value_string(const char *text,
                                             size_t length) {
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
static __inline fpta_value fpta_value_binary(const void *data,
                                             size_t length) {
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

//----------------------------------------------------------------------------

/* Коды ошибок.
 * Список будет пополнен, а описания уточнены. */
enum fpta_error {
  FPTA_SUCCESS = 0,
  FPTA_OK = FPTA_SUCCESS,
  FPTA_ERRROR_BASE = 4242,

  FPTA_EOOPS /* Internal unexpected Oops */,
  FPTA_SCHEMA_CORRUPTED /* */,
  FPTA_ETYPE /* Type mismatch */,
  FPTA_DATALEN_MISMATCH,
  FPTA_ROW_MISMATCH /* Row schema is mismatch */,
  FPTA_INDEX_CORRUPTED,
  FPTA_NO_INDEX,
  FPTA_ETXNOUT /* Transaction should be restared */,
  FPTA_ECURSOR /* Cursor not positioned */,
  FPTA_TOOMANY /* Too many columns or indexes */,
  FPTA_WANNA_DIE,

  FPTA_EINVAL = EINVAL,
  FPTA_ENOFIELD = ENOENT,
  FPTA_ENOMEM = ENOMEM,
  FPTA_EVALUE = EDOM /* Numeric value out of range*/,
  FPTA_NODATA = -1 /* No data or EOF was reached */,
  FPTA_ENOIMP = ENOSYS,

  FPTA_DEADBEEF = 0xDeadBeef /* Pseudo error for results by pointer,
    mean `no value` returned */,

  /************************************************* MDBX's error codes ***/
  FPTA_KEYEXIST = -30799 /* key/data pair already exists */,

  FPTA_NOTFOUND = -30798 /* key/data pair not found */,

  FPTA_DB_REF = -30797 /* wrong page address/number,
    this usually indicates corruption */,

  FPTA_DB_DATA = -30796 /* Located page was wrong data */,

  FPTA_DB_PANIC = -30795 /* Update of meta page failed
    or environment had fatal error */,

  FPTA_DB_MISMATCH = -30794 /* DB version mismatch with library */,

  FPTA_DB_INVALID = -30793 /* File is not a valid LMDB file */,

  FPTA_DB_FULL = -30792 /* Environment mapsize reached */,

  FPTA_DBI_FULL = -30791 /* Too may DBI (maxdbs reached) */,

  FPTA_READERS_FULL = -30790 /* Too many readerd (maxreaders reached) */,

  FPTA_TXN_FULL = -30788 /* Transaction has too many dirty pages,
    e.g. a lot of changes. */,

  FPTA_CURSOR_FULL = -30787 /* Cursor stack too deep (mdbx internal) */,

  FPTA_PAGE_FULL = -30786 /* Page has not enough space (mdbx internal) */,

  FPTA_DB_RESIZED = -30785 /* Database contents grew
    beyond environment mapsize */,

  FPTA_DB_INCOMPAT = -30784 /* Operation and DB incompatible (mdbx internal),
   This can mean:
     - The operation expects an MDB_DUPSORT/MDB_DUPFIXED database.
     - Opening a named DB when the unnamed DB has MDB_DUPSORT/MDB_INTEGERKEY.
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
const char *fpta_strerror(int error);

/* Потоко-безопасный вариант fpta_strerror().
 *
 * Функция потоко-безопасна в том числе в случае системной ошибки, так
 * как при этом вызывается потоко-безопасная системная strerror_r(). */
int fpta_strerror_r(int errnum, char *buf, size_t buflen);

/* Внутренняя функция, которая вызывается при фатальных ошибках и может
 * быть замещена на платформах с поддержкой __attribute__((weak)).
 * При таком замещении:
 *  - в err будет передан первичный код ошибки, в результате которой
 *    потребовалась отмена транзакции.
 *  - в fatal будет передан вторичный код ошибки, которая случилась
 *    при отмене транзакции.
 *  - возврат НУЛЯ приведет к вызову системной abort(), ИНАЧЕ выполнение
 *    будет продолжено с генерацией кода ошибки FPTA_WANNA_DIE. */
int fpta_panic(int err, int fatal);

//----------------------------------------------------------------------------
/* Открытие и закрытие БД */

/* Режим сохранности для изменений и БД в целом.
 *
 * Одновременно выбирает компромисс между производительностью по записи
 * и durability. */
typedef enum fpta_durability {

  fpta_readonly, /* Только чтение, изменения запрещены. */

  fpta_sync, /* Полностью синхронная запись на диск.
              * Самый надежный и самый медленный режим.
              *
              * По завершению транзакции выполняется fdatasync().
              * Производительность по записи определяется
              * скоростью диска (порядка 500 TPS для SSD). */

  fpta_lazy, /* "Ленивый" режим записи посредством файловой
              * системы. Детали поведения и сохранность данных
              * полностью определяются видом файловой системой
              * и её настройками. Производительность по записи
              * в основном определяется скоростью диска (порядка
              * 50K TPS для SSD).
              *
              * Изменения будут записываться через файловый
              * дескриптор в обычном режиме, без O_SYNC, O_DSYNC
              * и/или O_DIRECT.
              * В случае аварии могут быть потеряны последние
              * транзакции, при это целостность БД определяется
              * соблюдением data ordered со стороны ФС.
              *
              * Другими словами, кроме потери последних изменений
              * БД может быть разрушена, если ФС переупорядочивает
              * порядок операций записи (см режим data=writeback
              * для ext4). */

  fpta_async /* Самый быстрый режим. Образ БД отображается в
              * память в режиме read-write и изменения
              * производятся только в памяти.
              *
              * Ядро ОС, по своему усмотрению, асинхронно
              * записывает измененные страницы на диск.
              * При этом ядро ОС обещает запись всех изменений
              * при сбое приложения, OOM или при штатной
              * остановке. Но НЕ при сбое в ядре или при
              * отключении питания.
              *
              * Также, БД может быть повреждена в результате
              * некорректных действий приложения (роспись памяти).
              *
              * Производительность по записи в основном
              * определяется скоростью CPU и RAM (более 100K TPS). */
} fpta_durability;

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
 * позволяет отказаться от захвата pthread_rwlock_t в процессе работы.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
int fpta_db_open(const char *path, fpta_durability durability,
                 mode_t file_mode, size_t megabytes, bool alterable_schema,
                 fpta_db **db);

/* Закрывает ранее открытую базу.
 *
 * На момент закрытия базы должны быть закрыты все ранее открытые
 * курсоры и завершены все транзакции.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
int fpta_db_close(fpta_db *db);

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
                   * глобальный мьютех в разделяемой памяти.
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
                   * своего процесса посредством pthread_rwlock_t.
                   * Такая блокировка обусловлена двумя причинами:
                   *  - спецификой движков libmdbx/LMDB (удаление
                   *    таблицы приводит к закрытию её разделяемого
                   *    дескриптора, т.е. к нарушению MVCC);
                   *  - ради упрощения реализации "Позитивных Таблиц";
                   *
                   * Инициация транзакции изменяющей схему возможна,
                   * только если при БД была открыта в соответствующем
                   * режиме (было задано alterable_schema = true).
                   *
                   * С другой стороны, обещание не менять схему
                   * (указание alterable_schema = false) позволяет
                   * экономить на захвате pthread_rwlock_t при старте
                   * читающих транзакций. */
} fpta_level;

/* Инициация транзакции заданного уровня.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
int fpta_transaction_begin(fpta_db *db, fpta_level level, fpta_txn **txn);

/* Завершение транзакции.
 *
 * Аргумент abort для пишущих транзакций (уровней fpta_write и fpta_schema)
 * определяет будет ли транзакция зафиксирована или отменена.
 *
 * На момент завершения транзакции должны быть закрыты все связанные
 * с ней курсоры.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
int fpta_transaction_end(fpta_txn *txn, bool abort);

/* Получение версии данных и схемы.
 *
 * Для снимка данных (которая читается транзакцией)
 * и версию схемы (которая действует внутри транзакции).
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
int fpta_transaction_versions(fpta_txn *txn, uint64_t *data,
                              uint64_t *schema);

//----------------------------------------------------------------------------
/* Управление схемой:
 *  - Изменение схемы происходит в рамках "пишущей" транзакции
 *    уровня fpta_schema.
 *  - Можем создавать таблицы, указывая имя и "набор колонок".
 *  - Можем удалять таблицы по имени.
 *  - Изменения вступают в силу и видны другим процессам только
 *    после фиксации транзакции.
 *
 *  - "Набор колонок" задается в виде массива, каждый элемент которого
 *    создается отдельной вспомогательной функцией.
 *  - При описании колонки указывается её имя, тип данных и признак
 *    индексируемости (primary, secondary, none).
 *  - Для каждой таблицы ровно одна колонка должна быть помечена
 *    как primary.
 *
 * Хэш-коллизии в именах:
 *  - Имена таблиц и колонок подвергаются компактификации (сжатию).
 *  - Можно сказать, что из исходного имени формируется 55-битное
 *    хэш-значение. Соответственно, с вероятностью примерно
 *    1 к 190 миллионам возможна хэш-коллизия.
 *  - При возникновении такой коллизии, при создании таблицы будет
 *    возвращаться ошибка. Как если бы такая таблица уже существовала,
 *    или среди её колонок есть дубликат.
 */

/* Режимы индексирования для колонок таблиц.
 *
 * Ради достижения максимальной производительности libfpta не расходует
 * ресурсы на служебные колонки, в том числе на идентификаторы строк (ROW_ID).
 * Проще говоря, в libfpta в качестве ROW_ID используется первичный ключ.
 * Поэтому вторичные индексы возможны только при первичном индексе с контролем
 * уникальности. Если естественным образом в таблице нет поля/колонки
 * подходящего на роль первичного ключа, то такую колонку необходимо добавить
 * и заполнять искусственными уникальными значениями. Например, посмотреть
 * в сторону fptu_datetime и fptu_now().
 *
 * Неупорядоченные индексы:
 *   - fpta_primary_unique_unordered, fpta_primary_withdups_unordered;
 *   - fpta_secondary_unique_unordered, fpta_secondary_withdups_unordered.
 *
 *   Строятся посредством хеширования значений ключа. Такой индекс позволяет
 *   искать данные только по конкретному значению ключа. Основной бонус
 *   неупорядоченных индексов в минимизации накладных расходов для значений
 *   длиннее 8 байт, так как внутри БД все ключи становятся одинакового
 *   фиксированного размера.
 *
 * Индексы со сравнением ключей с конца:
 *   - fpta_primary_unique_reversed, fpta_primary_withdups_reversed;
 *   - fpta_secondary_unique_reversed, fpta_secondary_withdups_reversed.
 *
 *   Индексы этого типа применимы только для строк и бинарных данных (типы
 *   fptu_96..fptu_256, fptu_cstr и fptu_opaque При этом значения ключей
 *   сравниваются в обратном порядке байт. Не от первых к последним,
 *   а от последних к первым. Не следует пусть с обратным порядком сортировки
 *   или упорядочения величин.
 */
typedef enum fpta_index_type {
  /* служебные флажки/битики для комбинаций */
  fpta_index_funique = 1 << fpta_column_index_shift,
  fpta_index_fordered = 2 << fpta_column_index_shift,
  fpta_index_fobverse = 4 << fpta_column_index_shift,
  fpta_index_fsecondary = 8 << fpta_column_index_shift,

  /* Колонка НЕ индексируется и в последствии не может быть указана
   * при открытии курсора как опорная. */
  fpta_index_none = 0,

  /* Первичный ключ/индекс.
   *
   * Колонка будет использована как первичный ключ таблицы. При создании
   * таблицы такая колонка должна быть задана одна, и только одна. Вторичные
   * ключи/индексы допустимы только при уникальности по первичному ключу. */

  /* с повторами */
  fpta_primary_withdups = fpta_index_fordered + fpta_index_fobverse,
  fpta_primary_withdups_obverse = fpta_primary_withdups,

  /* с контролем уникальности */
  fpta_primary_unique = fpta_primary_withdups + fpta_index_funique,
  fpta_primary_unique_obverse = fpta_primary_unique,

  /* неупорядоченный, с контролем уникальности */
  fpta_primary_unique_unordered = fpta_primary_unique - fpta_index_fordered,

  /* неупорядоченный с повторами */
  fpta_primary_withdups_unordered =
      fpta_primary_withdups - fpta_index_fordered,

  /* строки и binary сравниваются с конца, с контролем уникальности */
  fpta_primary_unique_reversed = fpta_primary_unique - fpta_index_fobverse,

  /* строки и binary сравниваются с конца, с повторами */
  fpta_primary_withdups_reversed =
      fpta_primary_withdups - fpta_index_fobverse,

  /* базовый вариант для основного индекса */
  fpta_primary = fpta_primary_unique_obverse,

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
  fpta_secondary_withdups = fpta_primary_withdups + fpta_index_fsecondary,
  fpta_secondary_withdups_obverse = fpta_secondary_withdups,

  /* с контролем уникальности */
  fpta_secondary_unique = fpta_secondary_withdups + fpta_index_funique,
  fpta_secondary_unique_obverse = fpta_secondary_unique,

  /* неупорядоченный, с контролем уникальности */
  fpta_secondary_unique_unordered =
      fpta_secondary_unique - fpta_index_fordered,

  /* неупорядоченный с повторами */
  fpta_secondary_withdups_unordered =
      fpta_secondary_withdups - fpta_index_fordered,

  /* строки и binary сравниваются с конца, с контролем уникальности */
  fpta_secondary_unique_reversed =
      fpta_secondary_unique - fpta_index_fobverse,

  /* строки и binary сравниваются с конца, с повторами */
  fpta_secondary_withdups_reversed =
      fpta_secondary_withdups - fpta_index_fobverse,

  /* базовый вариант для вторичных индексов */
  fpta_secondary = fpta_secondary_withdups_obverse,
} fpta_index_type;

/* Внутренний тип для сжатых описаний идентификаторов. */
typedef uint64_t fpta_shove_t;

/* Набор колонок для создания таблицы */
typedef struct fpta_column_set {
  /* Счетчик заполненных описателей. */
  unsigned count;
  /* Упакованное внутреннее описание колонок. */
  fpta_shove_t shoves[fpta_max_cols];
} fpta_column_set;

/* Вспомогательная функция, проверяет корректность имени */
bool fpta_validate_name(const char *name);

/* Вспомогательная функция для создания описания колонок.
 *
 * Добавляет описание колонки в column_set.
 * Аргумент column_name задает имя колонки. Для совместимости в именах
 * таблиц и колонок допускаются символы: 0-9 A-Z a-z _
 * Начинаться имя должно с буквы. Регистр символов не различается.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
int fpta_column_describe(const char *column_name, enum fptu_type data_type,
                         enum fpta_index_type index_type,
                         fpta_column_set *column_set);

/* Инициализирует column_set перед заполнением посредством
 * fpta_column_describe(). */
void fpta_column_set_init(fpta_column_set *column_set);

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
int fpta_table_create(fpta_txn *txn, const char *table_name,
                      fpta_column_set *column_set);

/* Удаление таблицы.
 *
 * Требуется транзакция уровня fpta_schema. Изменения становятся
 * видимыми из других транзакций и процессов только после успешной
 * фиксации транзакции.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
int fpta_table_drop(fpta_txn *txn, const char *table_name);

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
 *  - Функция fpta_name_refresh() вызывается автоматически, поэтому
 *    как правило в явном ручном вызове необходимости нет.
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
    struct {
      struct fpta_table_schema *def;
      unsigned pk; /* вид индекса и тип данных для primary key */
    } table;

    /* для колонки */
    struct {
      struct fpta_name *table;
      int num; /* номер поля в кортеже. */
    } column;
  };
  unsigned mdbx_dbi; /* дескриптор движка */
} fpta_name;

/* Возвращает тип данных колонки из дескриптора имени */
static __inline fptu_type fpta_name_coltype(const fpta_name *column_id) {
  return (fptu_type)(column_id->shove & fpta_column_typeid_mask);
}

/* Возвращает тип индекса колонки из дескриптора имени */
static __inline fpta_index_type
fpta_name_colindex(const fpta_name *column_id) {
  return (fpta_index_type)(column_id->shove & fpta_column_index_mask);
}

/* Получение и актуализация идентификаторов таблицы и колонки.
 *
 * Функция работает в семантике кэширования с отслеживанием версии
 * схемы.
 *
 * Перед первым обращением name_id должен быть инициализирован
 * посредством fpta_table_init() или fpta_column_init().
 *
 * Аргумент column_id может быть нулевым. В этом случае он
 * игнорируется, и обрабатывается только table_id.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
int fpta_name_refresh(fpta_txn *txn, fpta_name *name_id);
int fpta_name_refresh_couple(fpta_txn *txn, fpta_name *table_id,
                             fpta_name *column_id);

/* Сбрасывает закэшированное состояние идентификатора.
 *
 * Функция может быть полезна в некоторых специфических сценариях,
 * когда движок fpta не может самостоятельно сделать вывод о необходимости
 * обновления информации. Например, если идентификатор используется повторно
 * без полной инициализации, но с новым экземпляром базы.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
int fpta_name_reset(fpta_name *name_id);

/* Инициализирует операционный идентификатор таблицы.
 *
 * Подготавливает идентификатор таблицы к последующему использованию.
 *
 * Следует считать, что стоимость операции сопоставима с вычислением
 * дайджеста MD5 для переданного имени.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
int fpta_table_init(fpta_name *table_id, const char *name);

/* Инициализирует операционный идентификатор колонки.
 *
 * Подготавливает идентификатор колонки к последующему использованию,
 * при этом связывает его с идентификатором таблицы задаваемым в table_id.
 * В свою очередь table_id должен быть предварительно подготовлен
 * посредством fpta_table_init().
 *
 * Следует считать, что стоимость операции сопоставима с вычислением
 * дайджеста MD5 для переданного имени.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
int fpta_column_init(const fpta_name *table_id, fpta_name *column_id,
                     const char *name);

/* Разрушает операционный идентификаторы таблиц и колонок. */
void fpta_name_destroy(fpta_name *id);

/* Возвращает количество колонок в таблице.
 *
 * Аргументом table_id выбирается требуемая таблица. Функция не производит
 * отслеживание версии схемы и не обновляет какую-либо информацию, а только
 * возвращает текущее закэшированное значение. Поэтому аргумент table_id
 * должен быть предварительно не только инициализирован, но и обязательно
 * обновлен посредством fpta_name_refresh().
 *
 * В случае успеха возвращает не-отрицательное количество колонок, либо
 * отрицательное значение как индикатор ошибки, при этом код ошибки
 * не специфицируется. */
int fpta_table_column_count(const fpta_name *table_id);

/* Возвращает информацию о колонке в таблице.
 *
 * Аргументом table_id выбирается требуемая таблица, а column задает колонку,
 * которые нумеруются подряд начиная с нуля.
 * Результат помещается в column_id, после чего его можно использовать для
 * fpta_name_coltype() и fpta_name_colindex().
 *
 * Функция не производит отслеживание версии схемы и не обновляет какую-либо
 * информацию, а только возвращает текущее закэшированное значение. Поэтому
 * аргумент table_id должен быть предварительно не только инициализирован,
 * но и обязательно обновлен посредством fpta_name_refresh().
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
int fpta_table_column_get(const fpta_name *table_id, unsigned column,
                          fpta_name *column_id);

//----------------------------------------------------------------------------
/* Управление фильтрами. */

/* Варианты условий (типы узлов) фильтра: НЕ, ИЛИ, И, функция-предикат,
 * меньше, больше, равно, не равно... */
typedef enum fpta_filter_bits {
  fpta_node_not = -3,
  fpta_node_or = -2,
  fpta_node_and = -1,
  fpta_node_fn = 0,
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

    /* параметры для вызова функтора/предиката. */
    struct {
      /* идентификатор колонки */
      const fpta_name *column_id;
      /* функция-предикат, получает указатель на найденное поле,
       * или nullptr если такового нет. А также опциональные
       * параметры context и arg.
       * Функция должна вернуть true, если значение колонки/поля
       * удовлетворяет критерию фильтрации.
       */
      bool (*predicate)(const fptu_field *field, const fpta_name *column_id,
                        void *context, void *arg);
      /* дополнительные аргументы для функции-предиката */
      void *context;
      void *arg;
    } node_fn;

    /* параметры для условия больше/меньше/равно/не-равно. */
    struct {
      /* идентификатор колонки */
      const fpta_name *left_id;
      /* значение для сравнения */
      fpta_value right_value;
    } node_cmp;
  };
} fpta_filter;

/* Проверка соответствия кортежа условию фильтра.
 *
 * Предполагается внутреннее использование, но функция также
 * доступна извне. */
bool fpta_filter_match(fpta_filter *fn, fptu_ro tuple);

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
 * Передаваемый через column_id экземпляр fpta_name перед первым
 * использованием должен быть инициализированы посредством fpta_column_init().
 * Предварительный вызов fpta_name_refresh() не обязателен.
 *
 * Аргументы range_from и range_to задают диапазон выборки по значению
 * ключевой колонки. При необходимости могут быть заданы значения
 * с псевдо-типами fpta_begin и fpta_end. Следует учитывать, что для
 * неупорядоченных индексов
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
int fpta_cursor_open(fpta_txn *txn, fpta_name *column_id,
                     fpta_value range_from, fpta_value range_to,
                     fpta_filter *filter, fpta_cursor_options op,
                     fpta_cursor **cursor);
int fpta_cursor_close(fpta_cursor *cursor);

/* Проверяет наличие за курсором данных.
 *
 * Отсутствие данных означает что нет возможности их прочитать, изменить
 * или удалить, но не исключает возможности вставки и/или добавления
 * новых данных.
 *
 * При наличии данных возвращает 0. При отсутствии данных или неустановленном
 * курсоре FPTA_NODATA (EOF). Иначе код ошибки.
 */
int fpta_cursor_eof(fpta_cursor *cursor);

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
int fpta_cursor_count(fpta_cursor *cursor, size_t *count, size_t limit);

/* Считает и возвращает количество дубликатов для ключа в текущей
 * позиции курсора, БЕЗ учета фильтра заданного при открытии курсора.
 *
 * За курсором должна быть текущая запись. Под дубликатами понимаются
 * запись с одинаковым значением ключевой колонки, что допустимо если
 * соответствующий индекс не требует уникальности.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
int fpta_cursor_dups(fpta_cursor *cursor, size_t *dups);

/* Возвращает строку таблицы, на которой стоит курсор.
 * В случае успеха возвращает ноль, иначе код ошибки. */
int fpta_cursor_get(fpta_cursor *cursor, fptu_ro *tuple);

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
int fpta_cursor_move(fpta_cursor *cursor, fpta_seek_operations op);

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
int fpta_cursor_locate(fpta_cursor *cursor, bool exactly,
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
int fpta_cursor_key(fpta_cursor *cursor, fpta_value *key);

//----------------------------------------------------------------------------
/* Манипуляция данными через курсоры. */

/* Обновляет строку в текущей позиции курсора.
 *
 * ВАЖНО-1: При обновлении НЕ допускается изменение значения ключевой колонки,
 * которая была задана посредством column_id при открытии курсора.
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
int fpta_cursor_update(fpta_cursor *cursor, fptu_ro new_row_value);

/* Проверяет соблюдение ограничений (constraints) перед обновлением
 * строки в текущей позиции курсора.
 *
 * Выполняет проверку ограничений (constraints) как если бы выполнялось
 * обновление. Позволяет избежать соответствующих ошибок fpta_cursor_update(),
 * которые приводят к прерыванию транзакции.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
int fpta_cursor_validate_update(fpta_cursor *cursor, fptu_ro new_row_value);

static __inline int fpta_cursor_probe_and_update(fpta_cursor *cursor,
                                                 fptu_ro new_row_value) {
  int rc = fpta_cursor_validate_update(cursor, new_row_value);
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
int fpta_cursor_delete(fpta_cursor *cursor);

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
int fpta_get(fpta_txn *txn, fpta_name *column_id,
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
  fpta_upsert
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
int fpta_put(fpta_txn *txn, fpta_name *table_id, fptu_ro row_value,
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
int fpta_validate_put(fpta_txn *txn, fpta_name *table_id, fptu_ro row_value,
                      fpta_put_options op);

static __inline int fpta_probe_and_put(fpta_txn *txn, fpta_name *table_id,
                                       fptu_ro row_value,
                                       fpta_put_options op) {
  int rc = fpta_validate_put(txn, table_id, row_value, op);
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
static __inline int fpta_validate_update_row(fpta_txn *txn,
                                             fpta_name *table_id,
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
static __inline int fpta_validate_insert_row(fpta_txn *txn,
                                             fpta_name *table_id,
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
static __inline int fpta_validate_upsert_row(fpta_txn *txn,
                                             fpta_name *table_id,
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
int fpta_delete(fpta_txn *txn, fpta_name *table_id, fptu_ro row_value);

//----------------------------------------------------------------------------
/* Манипуляция данными внутри строк. */

/* Конвертирует поле кортежа в "контейнер" fpta_value. */
fpta_value fpta_field2value(const fptu_field *pf);

/* Обновляет или добавляет в кортеж значение колонки.
 *
 * Аргумент column_id идентифицирует колонку и должен быть
 * предварительно подготовлен посредством fpta_name_refresh().
 * Внутри функции column_id не обновляется.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
int fpta_upsert_column(fptu_rw *pt, const fpta_name *column_id,
                       fpta_value value);

/* Получает значение указанной колонки из переданной строки.
 *
 * Аргумент column_id идентифицирует колонку и должен быть
 * предварительно подготовлен посредством fpta_name_refresh().
 * Внутри функции column_id не обновляется.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
int fpta_get_column(fptu_ro row_value, const fpta_name *column_id,
                    fpta_value *value);

#ifdef __cplusplus
}

//----------------------------------------------------------------------------
/* Сервисные функции и классы для C++ (будет пополнять, существенно). */

namespace std {
string to_string(fpta_error);
string to_string(const fptu_time &);
string to_string(fpta_value_type);
string to_string(const fpta_value &);
string to_string(fpta_durability);
string to_string(fpta_level);
string to_string(const fpta_db *);
string to_string(const fpta_txn *);
string to_string(fpta_index_type);
string to_string(const fpta_column_set &);
string to_string(const fpta_name &);
string to_string(fpta_filter_bits);
string to_string(const fpta_filter &);
string to_string(fpta_cursor_options);
string to_string(const fpta_cursor *);
string to_string(fpta_seek_operations);
string to_string(fpta_put_options);
}

static __inline fpta_value fpta_value_str(const std::string &str) {
  return fpta_value_string(str.data(), str.length());
}

#endif /* __cplusplus */

#ifdef _MSC_VER
#pragma pack(pop)
#pragma warning(pop)
#endif /* windows mustdie */

#endif /* FAST_POSITIVE_TABLES_H */

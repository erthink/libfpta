libfpta
==============================================
Fast Positive Tables, aka "Позитивные Таблицы"
by [Positive Technologies](https://www.ptsecurity.ru).

Ultra fast, compact, embeddable storage engine for (semi)structured data:
multiprocessing with zero-overhead, full ACID semantics with MVCC,
variety of indexes, saturation, sequences and much more.

"Позитивные таблицы" предназначены для построения высокоскоростных
локальных хранилищ (полу)структурированных данных, с целевой
производительностью до 1.000.000 запросов в секунду на каждое ядро
процессора.

*The Future will Positive. Всё будет хорошо.*
[![Build Status](https://travis-ci.org/leo-yuriev/libfpta.svg?branch=devel)](https://travis-ci.org/leo-yuriev/libfpta)
[![Build status](https://ci.appveyor.com/api/projects/status/wiixsody1o9474g9/branch/devel?svg=true)](https://ci.appveyor.com/project/leo-yuriev/libfpta/branch/devel)
[![CircleCI](https://circleci.com/gh/leo-yuriev/libfpta/tree/devel.svg?style=svg)](https://circleci.com/gh/leo-yuriev/libfpta/tree/devel)
[![Coverity Scan Status](https://scan.coverity.com/projects/12920/badge.svg)](https://scan.coverity.com/projects/leo-yuriev-libfpta)

English version [by Google](https://translate.googleusercontent.com/translate_c?act=url&ie=UTF8&sl=ru&tl=en&u=https://github.com/leo-yuriev/libfpta/tree/devel)
and [by Yandex](https://translate.yandex.ru/translate?url=https%3A%2F%2Fgithub.com%2Fleo-yuriev%2Flibfpta%2Ftree%2Fdevel&lang=ru-en).


## Кратко

_libfpta_ или "Позитивные Таблицы" - это экстремально производительный,
встраиваемый движок (библиотека) для манипуляций простыми таблицами в
локальной базе данных, которая может использоваться совместно
несколькими процессами.

_libfpta_ отличается взвешенным набором компромиссов между
производительностью и универсальностью, что дает уникальных набор
возможностей:

1. Одновременный многопоточный доступ к данным из нескольких процессов на
одном сервере.
  > Поддерживаются операционные системы
  > Linux (kernel >= 2.6.32, GNU libc >= 2.12, GCC >= 4.2) и
  > Windows (Windows 7/8/10, Windows Server 2008/2012/2016, MSVC 2013/2015/2017).

2. Обслуживание нескольких читателей без блокировок с линейным
масштабированием производительности по ядрам CPU.
  > Для читателей блокировки используются только при подключении и
  > отключении от базы данных. Операции изменения данных никак не блокируют
  > читателей.

3. Строго последовательные изменения без затрат на конкурирующие
блокировки (livelock) и с гарантией от взаимоблокировки (deadlock).
  > В каждый момент времени может быть только один писатель (процесс
  > изменяющий данные).

4. Прямой доступ к данным без накладных расходов.
  > База данных отображается в память. Доступ к данным возможен без
  > лишнего копирования, без выделения памяти, без обращения к сервисам
  > операционной системы.

5. Полная поддержка [ACID](https://ru.wikipedia.org/wiki/ACID) на основе строгой [MVCC](https://ru.wikipedia.org/wiki/MVCC) и
[COW](https://ru.wikipedia.org/wiki/%D0%9A%D0%BE%D0%BF%D0%B8%D1%80%D0%BE%D0%B2%D0%B0%D0%BD%D0%B8%D0%B5_%D0%BF%D1%80%D0%B8_%D0%B7%D0%B0%D0%BF%D0%B8%D1%81%D0%B8).
Устойчивость к сбоям и отсутствие фазы восстановления. Возможность
изменения данных только в памяти с отложенной асинхронной фиксацией на диске.

"Позитивные Таблицы" опираются на [libfptu](https://github.com/leo-yuriev/libfptu) (aka "Позитивные Кортежи")
для представления данных и на [libmdbx](https://github.com/ReOpen/libmdbx) (aka "eXtended LDMB")
для их хранения, а также используют [t1ha](https://github.com/PositiveTechnologies/t1ha) (aka "Позитивный Хэш").

Однако, "Позитивные Таблицы" не являются серебряной пулей и вероятно не
подойдут, если:

 * Размер одной записи (строки в таблице) больше 250 килобайт.
 * В запросах требуется обращаться одновременно к нескольким таблицам, подобно JOIN в SQL.
 * Объем данных существенно превышает RAM, либо сценарии использования
   требуют наличие WAL (Write Ahead Log) или журнала транзакций.

--------------------------------------------------------------------------------

Более подробная информация пока доступна только в виде [заголовочного файла API](fast_positive/tables.h).

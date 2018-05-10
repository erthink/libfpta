libfpta
==============================================
Fast Positive Tables, aka "Позитивные Таблицы"
by [Positive Technologies](https://www.ptsecurity.ru) -- Ultra fast, compact, [embedded database](https://en.wikipedia.org/wiki/Embedded_database)
for tabular and semistructured data:
multiprocessing with zero-overhead, full ACID semantics with MVCC,
variety of indexes, saturation, sequences and much more.


*The Future will Positive. Всё будет хорошо.*
[![License: LGPL v3](https://img.shields.io/badge/License-LGPL%20v3-blue.svg)](https://www.gnu.org/licenses/lgpl-3.0)
[![Build Status](https://travis-ci.org/leo-yuriev/libfpta.svg?branch=devel)](https://travis-ci.org/leo-yuriev/libfpta)
[![Build status](https://ci.appveyor.com/api/projects/status/wiixsody1o9474g9/branch/devel?svg=true)](https://ci.appveyor.com/project/leo-yuriev/libfpta/branch/devel)
[![CircleCI](https://circleci.com/gh/leo-yuriev/libfpta/tree/devel.svg?style=svg)](https://circleci.com/gh/leo-yuriev/libfpta/tree/devel)
[![Coverity Scan Status](https://scan.coverity.com/projects/12920/badge.svg)](https://scan.coverity.com/projects/leo-yuriev-libfpta)

English version [by Google](https://translate.googleusercontent.com/translate_c?act=url&ie=UTF8&sl=ru&tl=en&u=https://github.com/leo-yuriev/libfpta/tree/devel)
and [by Yandex](https://translate.yandex.ru/translate?url=https%3A%2F%2Fgithub.com%2Fleo-yuriev%2Flibfpta%2Ftree%2Fdevel&lang=ru-en).


## Кратко

_"Позитивные Таблицы"_ или _libfpta_ -- это Ультрабыстрая, компактная, [встраиваемая СУБД](https://ru.wikipedia.org/wiki/%D0%92%D1%81%D1%82%D1%80%D0%B0%D0%B8%D0%B2%D0%B0%D0%B5%D0%BC%D0%B0%D1%8F_%D0%A1%D0%A3%D0%91%D0%94)
для табличных и полуструктурированных данных.
_"Позитивные Таблицы"_ основываются на [B+Tree](https://ru.wikipedia.org/wiki/B%2B-%D0%B4%D0%B5%D1%80%D0%B5%D0%B2%D0%BE) и отличается взвешенным набором компромиссов,
благодаря чему достигается предельная производительность в целевых сценариях использования.

1. Одновременный многопоточный доступ к данным из нескольких процессов на
одном сервере.
  > Поддерживаются операционные системы
  > Linux (kernel >= 2.6.32, GNU libc >= 2.12, GCC >= 4.2) и
  > Windows (Windows 7/8/10, Windows Server 2008/2012/2016, MSVC 2015/2017).

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
для представления данных и на [libmdbx](https://github.com/ReOpen/libmdbx)
для их хранения, а также используют [t1ha](https://github.com/PositiveTechnologies/t1ha) (aka "Позитивный Хэш").

Однако, "Позитивные Таблицы" не являются серебряной пулей и вероятно не
подойдут, если:

 * Размер одной записи (строки в таблице) больше 250 килобайт.
 * В запросах требуется обращаться одновременно к нескольким таблицам, подобно JOIN в SQL.
 * Сценарии использования требуют наличие [WAL](https://ru.wikipedia.org/wiki/%D0%97%D0%B0%D0%BF%D0%B8%D1%81%D1%8C_%D1%81_%D0%BF%D1%80%D0%B5%D0%B4%D0%B2%D0%B0%D1%80%D0%B8%D1%82%D0%B5%D0%BB%D1%8C%D0%BD%D1%8B%D0%BC_%D0%B6%D1%83%D1%80%D0%BD%D0%B0%D0%BB%D0%B8%D1%80%D0%BE%D0%B2%D0%B0%D0%BD%D0%B8%D0%B5%D0%BC).

--------------------------------------------------------------------------------

Более подробная информация пока доступна только в виде [заголовочного файла API](fast_positive/tables.h).

--------------------------------------------------------------------------------

```
$ objdump -f -h -j .text libfpta.so

libfpta.so:     file format elf64-x86-64
architecture: i386:x86-64, flags 0x00000150:
HAS_SYMS, DYNAMIC, D_PAGED
start address 0x00007380

Sections:
Idx Name          Size      VMA       LMA       File off  Algn
 11 .text         0002a96e  00007380  00007380  00007380  2**4
                  CONTENTS, ALLOC, LOAD, READONLY, CODE
```

```
$ ldd libfpta.so
	linux-vdso.so.1 =>  (0x0000effc092e1000)
	libfptu.so.0.0.3 => ../lib/libfptu.so.0.0.3 (0x0000eff54322a000)
	libpthread.so.0 => /lib/x86_64-linux-gnu/libpthread.so.0 (0x0000eff542fe8000)
	libstdc++.so.6 => /usr/lib/x86_64-linux-gnu/libstdc++.so.6 (0x0000eff542c5e000)
	libgcc_s.so.1 => /lib/x86_64-linux-gnu/libgcc_s.so.1 (0x0000eff542a47000)
	libc.so.6 => /lib/x86_64-linux-gnu/libc.so.6 (0x0000eff54267c000)
	librt.so.1 => /lib/x86_64-linux-gnu/librt.so.1 (0x0000eff542474000)
	/lib64/ld-linux-x86-64.so.2 (0x0000e623d17f8000)
	libm.so.6 => /lib/x86_64-linux-gnu/libm.so.6 (0x0000eff54216b000)
```

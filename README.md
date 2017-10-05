Ждём всех на [Highload++2017](http://www.highload.ru/2017/abstracts/2837.html)!
=============================

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
[![Build Status](https://travis-ci.org/leo-yuriev/libfpta.svg?branch=master)](https://travis-ci.org/leo-yuriev/libfpta)
[![Build status](https://ci.appveyor.com/api/projects/status/wiixsody1o9474g9/branch/master?svg=true)](https://ci.appveyor.com/project/leo-yuriev/libfpta/branch/master)
[![CircleCI](https://circleci.com/gh/leo-yuriev/libfpta/tree/master.svg?style=svg)](https://circleci.com/gh/leo-yuriev/libfpta/tree/master)
[![Coverity Scan Status](https://scan.coverity.com/projects/12920/badge.svg)](https://scan.coverity.com/projects/leo-yuriev-libfpta)

English version [by Google](https://translate.googleusercontent.com/translate_c?act=url&ie=UTF8&sl=ru&tl=en&u=https://github.com/leo-yuriev/libfpta/tree/master)
and [by Yandex](https://translate.yandex.ru/translate?url=https%3A%2F%2Fgithub.com%2Fleo-yuriev%2Flibfpta%2Ftree%2Fmaster&lang=ru-en).


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

```
$ objdump -C -T libfpta.so | grep fpta | sort

00007444 g    DF .text	00000015  Base        std::to_string[abi:cxx11](fpta_key const&)
00007459 g    DF .text	00000021  Base        std::to_string[abi:cxx11](fpta_txn const*)
0000747a g    DF .text	00000021  Base        std::to_string[abi:cxx11](fpta_db const*)
0000749b g    DF .text	0000001c  Base        std::to_string[abi:cxx11](fpta_column_set const*)
000074b7 g    DF .text	00000056  Base        std::to_string[abi:cxx11](fpta_put_options)
0000750d g    DF .text	000000c4  Base        std::to_string[abi:cxx11](fpta_seek_operations)
000075d1 g    DF .text	0000008e  Base        std::to_string[abi:cxx11](fpta_cursor_options)
0000765f g    DF .text	0000008a  Base        std::to_string[abi:cxx11](fpta_filter_bits)
000076e9 g    DF .text	00000371  Base        std::to_string[abi:cxx11](fpta_index_type)
00007a5a g    DF .text	00000282  Base        std::to_string[abi:cxx11](fpta_table_schema const*)
00007cdc g    DF .text	00000471  Base        std::to_string[abi:cxx11](fpta_name const*)
0000814d g    DF .text	00000059  Base        std::to_string[abi:cxx11](fpta_level)
000081a6 g    DF .text	000004c7  Base        std::to_string[abi:cxx11](fpta_filter const*)
0000866d g    DF .text	000007cf  Base        std::to_string[abi:cxx11](fpta_cursor const*)
00008e3c g    DF .text	00000069  Base        std::to_string[abi:cxx11](fpta_durability)
00008ea5 g    DF .text	00000142  Base        std::to_string[abi:cxx11](fpta_value const*)
00008fe7 g    DF .text	000000c4  Base        std::to_string[abi:cxx11](fpta_value_type)
000090ab g    DF .text	00000022  Base        std::to_string[abi:cxx11](fpta_error)
0000e340 g    DF .text	00000146  Base        fpta_filter_match
000108f0 g    DF .text	00001051  Base        fpta_column_set_validate
00011950 g    DF .text	00000066  Base        fpta_schema_destroy
000119c0 g    DF .text	00000285  Base        fpta_schema_fetch
00011c50 g    DF .text	00000394  Base        fpta_column_describe
00012110 g    DF .text	0000000a  Base        __fpta_filter_cmp
00012120 g    DF .text	00000008  Base        fpta_panic
00012130 g    DF .text	00000087  Base        fpta_db_sequence
000121c0 g    DF .text	00000077  Base        fpta_transaction_versions
00012660 g    DF .text	0000032a  Base        fpta_cursor_move
00012990 g    DF .text	00000092  Base        fpta_cursor_close
00012b60 g    DF .text	00000295  Base        fpta_cursor_open
00012e00 g    DF .text	00000058  Base        fpta_table_sequence
00012e60 g    DF .text	000000c4  Base        fpta_table_info
000130a0 g    DF .text	0000016d  Base        fpta_table_clear
00013210 g    DF .text	0000011b  Base        fpta_transaction_end
00013330 g    DF .text	00000185  Base        fpta_transaction_begin
000134c0 g    DF .text	000000f4  Base        fpta_db_close
000135c0 g    DF .text	00000226  Base        fpta_db_open
000137e6 g    DF .text	0000048a  Base        fpta_upsert_column
00013c70 g    DF .text	00000194  Base        fpta_get_column
00013e04 g    DF .text	00000161  Base        fpta_field2value
00013f65 g    DF .text	0000017e  Base        fpta_strerror_r
000140e3 g    DF .text	0000017e  Base        fpta_strerror
000147b3 g    DF .text	0000015c  Base        fpta_get
0001490f g    DF .text	00000267  Base        fpta_put
00014b76 g    DF .text	00000143  Base        fpta_delete
00014cb9 g    DF .text	000001c2  Base        fpta_validate_put
00014e7b g    DF .text	0000007c  Base        fpta_cursor_key
00014ef7 g    DF .text	00000069  Base        fpta_cursor_state
00014f60 g    DF .text	0000005b  Base        fpta_cursor_eof
00014fbb g    DF .text	0000009b  Base        fpta_validate_name
00015056 g    DF .text	00000073  Base        fpta_cursor_count
000150c9 g    DF .text	0000040b  Base        fpta_cursor_update
000154d4 g    DF .text	0000023a  Base        fpta_cursor_validate_update
0001570e g    DF .text	00000108  Base        fpta_cursor_get
00015816 g    DF .text	00000217  Base        fpta_apply_visitor
00015a2d g    DF .text	000002db  Base        fpta_cursor_delete
00015d08 g    DF .text	0000009d  Base        fpta_cursor_dups
00015da5 g    DF .text	00000345  Base        fpta_cursor_locate
00016fe0 g    DF .text	00000027  Base        fpta_name_reset
00017010 g    DF .text	000000a7  Base        fpta_table_column_get
000170c0 g    DF .text	0000003f  Base        fpta_table_column_count
00017100 g    DF .text	00000063  Base        fpta_name_destroy
00017170 g    DF .text	00000086  Base        fpta_table_drop
00017200 g    DF .text	000000b7  Base        fpta_column_init
000172c0 g    DF .text	00000087  Base        fpta_table_init
00017350 g    DF .text	000000d6  Base        fpta_table_create
00017430 g    DF .text	0000040c  Base        fpta_name_refresh_couple
00017840 g    DF .text	00000047  Base        fpta_name_refresh
00017dd0 g    DF .text	0000000a  Base        __fpta_index_shove2comparator
000186f0 g    DF .text	0000000f  Base        __fpta_index_value2key
0001c0f0 g    DF .text	000012da  Base        fpta_inplace_column
0001f3e0 g    DF .text	00001670  Base        fpta_cursor_inplace
00031800 g    DF .text	0000031a  Base        fpta_confine_number
00031b20 g    DF .text	000001ce  Base        fpta_pollute
```

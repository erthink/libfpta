﻿libfptu
==============================================
Fast Positive Tuples, aka "Позитивные Кортежи"
by [Positive Technologies](https://www.ptsecurity.ru).

*The Future will Positive. Всё будет хорошо.*
[![Build Status](https://travis-ci.org/leo-yuriev/libfptu.svg?branch=master)](https://travis-ci.org/leo-yuriev/libfptu)
[![Build status](https://ci.appveyor.com/api/projects/status/8617mtix9paivmkx/branch/master?svg=true)](https://ci.appveyor.com/project/leo-yuriev/libfptu/branch/master)
[![CircleCI](https://circleci.com/gh/leo-yuriev/libfptu/tree/master.svg?style=svg)](https://circleci.com/gh/leo-yuriev/libfptu/tree/master)
[![Coverity Scan Status](https://scan.coverity.com/projects/12919/badge.svg)](https://scan.coverity.com/projects/leo-yuriev-libfptu)

English version [by Google](https://translate.googleusercontent.com/translate_c?act=url&ie=UTF8&sl=ru&tl=en&u=https://github.com/leo-yuriev/libfptu/tree/master)
and [by Yandex](https://translate.yandex.ru/translate?url=https%3A%2F%2Fgithub.com%2Fleo-yuriev%2Flibfptu%2Ftree%2Fmaster&lang=ru-en).

## Кратко

"Позитивные Кортежи" (_ПК_) предназначены как для представления
небольших JSON-подобных структур, так и для хранения данных в парах
ключ-значение, в том числе в разделяемой памяти.

Целью дизайна _ПК_ была эффективная машинная обработка в памяти
небольших порций данных (до 100 полей, до 10 килобайт). При этом
совместное использование (чтение) кортежей в разделяемой памяти является
одним из основных и целеполагающих сценариев (проект 1Hippeus).

В _ПК_ нет встроенной схемы данных. Поэтому у полей нет имен, они
идентифицируются по коротким целочисленным тегам. Это очень быстро и
достаточно компактно. Тем не менее, поддержка вложенных кортежей
позволяет легко хранить пары имя-значение.


### Отличия от MessagePack, Protocol Buffers, BJSON

1. Простота и легковесность. Объем кода минимален, а внутреннее
устройство очаровывает простотой (мы к этому стремимся).

2. Нужно чуть больше места. Мы не сжимаем данные, а храним их в нативном
машинном представлении с выравниванием. Поэтому в _ПК_ для каждого поля
требуется примерно на 3-4 байта больше.
 > Тем не менее, следует аккуратно интерпретировать эти цифры. Если у вас
 > много 64-битных целочисленных полей с близкими к нулю значениями, то
 > может оказаться так, что представление в _ПК_ потребует в 12 раз больше
 > памяти в сравнении с MessagePack (1 байт в MessagePack, против 12 байт в
 > _ПК_).

3. Очень быстрый доступ. У нас есть простейший индекс. Поэтому поиск
полей и доступ к данным в _ПК_ очень быстрые.
 > Для доступа к содержимому кортежа достаточно его сырого сериализованного
 > представления в линейном участке памяти, без каких-либо преобразований
 > или модификаций. Получения поля из кортежа сводится к поиску его
 > дескриптора в заголовке. Что равнозначно чтению одной кэш-линии на
 > первые 15 полей и далее на каждые 16 последующих.

4. Очень быстрая сериализация. Мы формируем и растим кортеж как линейную
последовательность байт просто добавлением (дозаписью) данных. Поэтому
сериализация в _ПК_ может быть магически быстрой.
 > Заполнение кортежа происходит без лишних операций, просто
 > однократным копированием данных в заранее выделенный буфер
 > достаточного размера. При этом сериализованное представление
 > всегда готово, доступ к нему сводится к получению указателя и
 > размера.

5. Формируемые кортежи можно изменять. Можно удалять и перезаписывать
поля, в том числе с изменением размера.
 > При этом в линейном представлении могут образовываться неиспользуемые
 > фрагменты, а у вас появляется выбор: пожертвовать местом или
 > использовать процедуру дефрагментации. При этом выполняется перемещение
 > полезных данных с вытеснением неиспользуемых зазоров. Соответственно, в
 > худшем случае, дефрагментация не дороже однократного копирования
 > содержимого кортежа.


Однако, "Позитивные Кортежи" не являются серебряной пулей и вероятно не
подойдут, если:

 * В структурах более 500 полей;
 * Размер одной структуры более 64 килобайт;
 * Минимизация объема намного важнее скорости доступа
   и затрат на сериализацию.

************************************************************************


## Обзор

Физически кортеж представляет собой линейный массив 32-битных элементов,
в начале которого расположены дескрипторы полей/колонок, а затем их
данные. Таким образом, как сериализация, так десериализация кортежа
сводятся к однократной операции чтения/записи линейного участка памяти.

Выборка поля из кортежа сводится к поиску соответствующего элемента
среди дескрипторов, а далее обращение к данным посредством хранимого в
дескрипторе смещения.

Текущая реализация ориентирована на небольшое число полей, когда их
дескрипторы помещаются в несколько кэш-линий, а двоичный поиск не дает
выигрыша в сравнении с линейным. Для эффективной поддержки больших
кортежей предусмотрено добавление сортировки дескрипторов полей, а также
прямой доступ по их тегам в качестве индексов.

Добавление поля в кортеж сводится к добавлению дескриптора в начало
кортежа и дозаписи данных в конец. При этом резервирование места
позволяет обойтись без перемещения уже размещенных в кортеже данных.

Удаление полей, а также обновление значений полей вариативной длины
(строки, бинарные строки, массивы, вложенные кортежи), может приводить к
образованию внутри кортежа неиспользуемых участков, которые
ликвидируются дефрагментацией.


### Поля

"Позитивные кортежи" состоят из типизированных полей. Каждое поле
идентифицируется тэгом (номером колонки) и типом хранимых данных. Могут
быть несколько полей с одинаковым тэгом, но разными типами, при этом они
различаются.

Поля всегда опциональны, могут отсутствовать, а при необходимости любое
поле может многократно повторяться. Таким образом, можно получить
коллекцию (аналогично `repeated` в Protocol Buffers).


### Типы

Набор базовых типов фиксирован: null, int 32/64, float point 32/64,
c-string, unsigned int 16/32/64, бинарные блоки 96/128/160/192/256 бит,
бинарные строки, вложенные кортежи, одномерные массивы базовых типов
(кроме null).

Текущая реализация допускает вложенность кортежей, но в угоду
легковесности и производительности не предлагает для этого элегантной
автоматизации. В целом, для представления вложенных структур возможны
два подхода:

1. Расширение имен:
   делаем `{ "ФИО.Имя": "Иван", "ФИО.Фамилия": "Петров" }`
   вместо `{ "ФИО": { "Имя": "Иван", "Фамилия": "Петров" } }`

2. Вложенная сериализация, когда сначала отдельно сериализуется `"ФИО"`,
   а затем целиком вкладывается в родительский кортеж.

Для хранения многомерных массивов доступны три варианта:
 * Коллекции, если просто давить одномерный массив несколько раз;
 * Вложенные кортежи;
 * Бинарные строки.


### Итераторы

Можно проитерировать все поля в кортеже, это быстро и дешево:

 * Можно итерировать с фильтрацией по тегу/номеру
   и битовой маске типов;
 * При итерации у каждого поля можно спросить тэг/номер,
   тип и значение;
 * Итератор остается валидным до разрушения или до
   компактификации кортежа;
 * При итерации любое количество полей можно как удалить,
   так и добавить;
 * Добавленные в процессе итерации поля можно как увидеть
   через итератор, так и не увидеть.

Однако, следует считать, что порядок полей при итерации не определен и
никак не связан с их порядком добавления или удаления. В часности,
поэтому нет и не будет итерации в обратном порядке.



### Устойчивость к некорректным данным

Постоянная проверка корректности данных слишком дорога и как-правило
избыточна. С другой стороны, любые нарушениях в десериализуемых данных
не должны приводить к авариям.

Поэтому в _ПК_ эксплуатируется следующий принцип:

1. Доступны функции верификации сериализованной и изменяемой форм
кортежа, которые вы используете по своему усмотрению.

2. В угоду производительности, основные функции выполняют только
минимальный контроль корректности аргументов и предоставляемых данных.
Поэтому при мусорных (не валидных) данных их поведение не определено.

3. Гарантируется (мы стремимся к этому), что прошедшие проверку данные
не вызовут нарушений при дальнейшей работе с ними.


************************************************************************


## Внутри

### Формат

Формат представления кортежей ориентирован на машину. Все данные в бинарном
машинном виде, порядок байт определяется архитектурой/режимом CPU (пока
только `little endian`):

 * сначала идет "заголовок", представляющий собой массив из 32-битных
   дескрипторов полей;
 * за заголовком следуют данные полей/колонок;
 * каждый элемент-дескриптор в массиве-заголовке содержит идентификатор
   колонки/поля, **тип данных** и **смещение** к ним относительно дескриптора;
 * каждый дескриптор и связанные с ним данные выровнены на 4х-байтовую границу.


Для полей типа `fptu_null` смещение внутри дескриптора не используется, но устанавливается
в ненулевое значение. Этим исключается равенство дескриптора нулю.

Для полей типа `fptu_uint16` смещение используется для хранения непосредственно самого
значения поля.

Строки хранятся только в ASCII-7/UTF-8 с терминирующим `'\0'` без явной длины.
Это позволяет иметь классический "C" API и экономить на хранении длины,
а в остальных случаях использовать бинарные строки.

Для всех полей переменной длины (массивов, бинарных строк, вложенных кортежей),
за исключением C-строк, в первом 32-битном слове данных
хранится их размер. Причем в первом полуслове хранится брутто-размер
поля в 32-битных словах, а во втором в зависимости от типа:
 * точный размер для бинарных строк;
 * количество элементов для массивов и кортежей;
 * дополнительные признаки для кортежей;

Массивы хранятся как линейная последовательность образующих их
элементов. При этом их элементы выравниваются на 4-байтную границу,
кроме строк и `uint16`. Строки в массивах располагаются в стык, а `uint16_t`
просто последовательно.

Формат первого слова для вложенных кортежей и корневого кортежа
полностью совпадает с небольшой оговоркой:
  * В самостоятельном виде пустой кортеж может быть представлен
    как `ноль байт` (пустой строкой байт), так и минимальным заголовком,
    в котором указано `ноль элементов`.
  * Вложенный кортеж является полем, поэтому всегда обязан иметь
    заголовок.


### Изменяемая и сериализованная формы

Сериализуемая форма представляет собой линейный массив 32-битных элементов,
в первом из которых хранится кол-вол полей/колонок, т.е. фактически длина
заголовка.

Формирование кортежа предполагает выделение места с некоторым запасом,
а также поддержку нескольких дополнительных счетчиков. Поэтому обновляемое
представление является надстройкой над его сериализуемой формой:

 * обновляемая форма кортежа живет в буфере, который выделяется в расчете
   на максимальный размер (как по количеству элементов, так и по их данным);
 * внутри выделенного буфера располагаются служебные счетчики, а также
   плавает/растет сериализуемое представление;
 * получение сериализуемой формы из обновляемой сводится к записи первого
   элемента, а затем к выдаче указателя на начало кортежа и его полного
   размера байтах;
 * получение обновляемой формы из сериализуемой сводится к копированию
   кортежа внутрь выделенного буфера, размер которого должен включать запас
   на служебные счетчики и добавляемые данные.


         buffer of sufficient size
        |<=======================================================>|
        |                                                         |
        |   head         pivot                             tail   |
        |   <-----~~~~~~~~~|~~~~~~~~~~~~~~~~~~---------------->   |
        |       descriptors|payload                               |
                           |
                  #_D_C_B_A_aaa_bb_cccccc_dddd
                  |                          |
                  |<========================>|
                    linear uint32_t sequence
                       for serialization

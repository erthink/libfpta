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

#include "fpta_test.h"
#include "keygen.hpp"

/* Кол-во проверочных точек в диапазонах значений индексируемых типов.
 *
 * Значение не может быть больше чем 65536, так как это предел кол-ва
 * уникальных значений для fptu_uint16.
 *
 * Но для парного генератора не может быть больше 65536/NDUP,
 * так как для не-уникальных вторичных индексов нам требуются дубликаты,
 * что требует больше уникальных значений для первичного ключа.
 *
 * Использовать тут большие значения смысла нет. Время работы тестов
 * растет примерно линейно (чуть быстрее), тогда как вероятность
 * проявления каких-либо ошибок растет в лучшем случае как Log(NNN),
 * а скорее даже как SquareRoot(Log(NNN)).
 */
static constexpr int NDUP = 5;
#ifdef FPTA_CURSOR_UT_LONG
static constexpr int NNN = 13103; // около часа в /dev/shm/
#else
static constexpr int NNN = 41; // порядка 10-15 секунд в /dev/shm/
#endif

static const char testdb_name[] = TEST_DB_DIR "ut_cursor_secondary2.fpta";
static const char testdb_name_lck[] =
    TEST_DB_DIR "ut_cursor_secondary2.fpta" MDBX_LOCK_SUFFIX;

#include "cursor_secondary.hpp"

//----------------------------------------------------------------------------

/* Другое имя класса требуется для инстанцирования другого (меньшего)
 * набора комбинаций в INSTANTIATE_TEST_CASE_P. */
class CursorSecondaryDups : public CursorSecondary {};

TEST_P(CursorSecondaryDups, dupMoves) {
  /* Проверка перемещений курсора по дубликатами во вторичном (secondary)
   * индексе.
   *
   * Сценарий (общий для всех комбинаций всех типов полей, всех видов
   * первичных и вторичных индексов, всех видов курсоров):
   *  1. Создается тестовая база с одной таблицей, в которой пять колонок:
   *      - "col_pk" (primary key) с типом, для которого производится
   *        тестирование работы первичного индекса.
   *      - "col_se" (secondary key) с типом, для которого производится
   *        тестирование работы вторичного индекса.
   *      - Колонка "order", в которую записывается контрольный (ожидаемый)
   *        порядковый номер следования строки, при сортировке по col_se и
   *        проверяемому виду индекса.
   *      - Колонка "dup_id", которая используется для нумерации дубликатов.
   *      - Колонка "t1ha", в которую записывается "контрольная сумма" от
   *        ожидаемого порядка строки, типа col_se и вида индекса.
   *        Принципиальной необходимости в этой колонке нет, она используется
   *        как "утяжелитель", а также для дополнительного контроля.
   *
   *  2. Для валидных комбинаций вида индекса и типов данных таблица
   *     заполняется строками, значения col_pk и col_se в которых
   *     генерируется соответствующими генераторами значений:
   *      - Сами генераторы проверяются в одном из тестов 0corny.
   *      - Для каждого значения ключа вставляется 5 строк с разным dup_id.
   *      - FIXME: Дополнительно, для тестирования межстраничных переходов,
   *        генерируется длинная серия повторов, которая более чем в три раза
   *        превышает размер страницы БД.
   *
   *  3. Перебираются все комбинации индексов, типов колонок и видов курсора.
   *     Для НЕ валидных комбинаций контролируются коды ошибок.
   *
   *  4. Для валидных комбинаций индекса и типа курсора, после заполнения
   *     в отдельной транзакции формируется "карта" верификации перемещений:
   *      - "карта" строится как неупорядоченное отображение линейных номеров
   *        строк в порядке просмотра через курсор, на ожидаемые (контрольные)
   *        значения колонки "order".
   *      - при построении "карты" все строки читаются последовательно через
   *        проверяемый курсор.
   *      - для построенной "карты" проверяется размер (что прочитали все
   *        строки и только по одному разу) и соответствия порядка строк
   *        типу курсора (возрастание/убывание).
   *
   *  5. После формирования "карты" верификации перемещений выполняется ряд
   *     базовых перемещений курсора по дубликатам:
   *      - переход к первой/последней строке,
   *        к первому и последнему дубликату.
   *      - попытки перейти за последнюю и за первую строки,
   *        за первый/последний дубликат.
   *      - переход в начало с отступлением к концу.
   *      - переход к концу с отступление к началу.
   *      - при каждом перемещении проверяется корректность кода ошибки,
   *        соответствие текущей строки ожидаемому порядку, включая
   *        содержимое строки и номера дубликата.
   *
   *  6. Завершаются операции и освобождаются ресурсы.
   */
  CHECK_RUNTIME_LIMIT_OR_SKIP();
  if (!valid_index_ops || !valid_cursor_ops || fpta_index_is_unique(se_index))
    return;

  SCOPED_TRACE("pk_type " + std::to_string(pk_type) + ", pk_index " +
               std::to_string(pk_index) + ", se_type " +
               std::to_string(se_type) + ", se_index " +
               std::to_string(se_index) +
               (valid_index_ops ? ", (valid case)" : ", (invalid case)"));

  SCOPED_TRACE(
      "ordering " + std::to_string(ordering) + ", index " +
      std::to_string(se_index) +
      (valid_cursor_ops ? ", (valid cursor case)" : ", (invalid cursor case)"));

  ASSERT_LT(5u, n_records);
  fpta_cursor *const cursor = cursor_guard.get();
  ASSERT_NE(nullptr, cursor);

  /* переходим туда-сюда и к первой строке, такие переходы уже проверялись
   * в предыдущем тесте, здесь же для проверки жизнеспособности курсора. */
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_first));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 0));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_last));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, -1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_first));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 0));

  // к последнему, затем к первому дубликату первой строки
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_dup_last));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, -1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_dup_first));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 0));

  // вперед по дубликатам первой строки
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_dup_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_dup_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 2));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_dup_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 3));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_dup_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 4));
  // пробуем выйти за последний дубликат
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_dup_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 4));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_dup_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 4));

  // назад по дубликатам первой строки
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_dup_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 3));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_dup_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 2));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_dup_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_dup_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 0));
  // пробуем выйти за первый дубликат
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_dup_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 0));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_dup_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 0));

  // вперед в обход дубликатов, ко второй строке, затем к третьей и четвертой
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_key_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(1, 0));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_key_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(2, 0));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_key_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(3, 0));

  // назад в обход дубликатов, до первой строки
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_key_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(2, -1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_key_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(1, -1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_key_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, -1));
  // пробуем выйти за первую строку
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_key_prev));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
#if FPTA_ENABLE_RETURN_INTO_RANGE
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_key_prev));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
#else
  ASSERT_EQ(FPTA_ECURSOR, fpta_cursor_move(cursor, fpta_key_prev));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
#endif

// последовательно вперед от начала по каждому дубликату
#if FPTA_ENABLE_RETURN_INTO_RANGE
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
#else
  ASSERT_EQ(FPTA_ECURSOR, fpta_cursor_move(cursor, fpta_next));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
  ASSERT_EQ(FPTA_ECURSOR, fpta_cursor_move(cursor, fpta_first));
#endif
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 0));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 2));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 3));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 4));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(1, 0));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(1, 1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(1, 2));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(1, 3));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(1, 4));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(2, 0));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(2, 1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(2, 2));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(2, 3));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(2, 4));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(3, 0));

  // последовательно назад к началу по каждому дубликату
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(2, 4));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(2, 3));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(2, 2));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(2, 1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(2, 0));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(1, 4));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(1, 3));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(1, 2));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(1, 1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(1, 0));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 4));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 3));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 2));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(0, 0));
  // пробуем выйти за первую строку
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
#if FPTA_ENABLE_RETURN_INTO_RANGE
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
#else
  ASSERT_EQ(FPTA_ECURSOR, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
#endif

  //--------------------------------------------------------------------------

  // к последней строке
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_last));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, -1));
  // к первому, затем к последнему дубликату последней строки
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_dup_first));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 0));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_dup_last));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, -1));

  // назад по дубликатам последней строки
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_dup_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 3));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_dup_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 2));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_dup_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_dup_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 0));
  // пробуем выйти за первый дубликат
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_dup_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 0));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_dup_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 0));

  // вперед по дубликатам первой строки
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_dup_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_dup_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 2));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_dup_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 3));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_dup_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 4));
  // пробуем выйти за последний дубликат
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_dup_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, -1));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_dup_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, -1));

  // назад в обход дубликатов, к предпоследней строке,
  // затем к пред-предпоследней...
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_key_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-2, -1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_key_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-3, -1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_key_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-4, -1));

  // вперед в обход дубликатов, до последней строки
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_key_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-3, 0));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_key_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-2, 0));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_key_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 0));
  // пробуем выйти за первую строку
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_key_next));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
#if FPTA_ENABLE_RETURN_INTO_RANGE
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_key_next));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
#else
  ASSERT_EQ(FPTA_ECURSOR, fpta_cursor_move(cursor, fpta_key_next));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
#endif

// последовательно назад от конца по каждому дубликату
#if FPTA_ENABLE_RETURN_INTO_RANGE
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
#else
  ASSERT_EQ(FPTA_ECURSOR, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
  ASSERT_EQ(FPTA_ECURSOR, fpta_cursor_move(cursor, fpta_last));
#endif
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 4));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 3));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 2));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 0));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-2, 4));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-2, 3));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-2, 2));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-2, 1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-2, 0));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-3, 4));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-3, 3));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-3, 2));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-3, 1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-3, 0));

  // последовательно вперед до конца по каждому дубликату
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-3, 1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-3, 2));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-3, 3));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-3, 4));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-2, 0));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-2, 1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-2, 2));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-2, 3));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-2, 4));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 0));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 1));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 2));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 3));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 4));
  // пробуем выйти за последнюю строку
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_next));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
#if FPTA_ENABLE_RETURN_INTO_RANGE
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_next));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_prev));
#else
  ASSERT_EQ(FPTA_ECURSOR, fpta_cursor_move(cursor, fpta_next));
  ASSERT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
  ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_last));
#endif
  ASSERT_NO_FATAL_FAILURE(CheckPosition(-1, 4));
}

//----------------------------------------------------------------------------

#if GTEST_HAS_COMBINE

INSTANTIATE_TEST_CASE_P(
    Combine, CursorSecondary,
    ::testing::Combine(
        ::testing::Values(fpta_primary_unique_ordered_obverse,
                          fpta_primary_unique_ordered_reverse,
                          fpta_primary_unique_unordered),
        ::testing::Values(fptu_null, fptu_uint16, fptu_int32, fptu_uint32,
                          fptu_fp32, fptu_int64, fptu_uint64, fptu_fp64,
                          fptu_96, fptu_128, fptu_160, fptu_datetime, fptu_256,
                          fptu_cstr, fptu_opaque
                          /*, fptu_nested, fptu_farray */),
        ::testing::Values(fpta_secondary_withdups_ordered_obverse,
                          fpta_secondary_withdups_ordered_reverse,
                          fpta_secondary_withdups_unordered),
        ::testing::Values(fptu_null, fptu_uint16, fptu_int32, fptu_uint32,
                          fptu_fp32, fptu_int64, fptu_uint64, fptu_fp64,
                          fptu_96, fptu_128, fptu_160, fptu_datetime, fptu_256,
                          fptu_cstr, fptu_opaque
                          /*, fptu_nested, fptu_farray */),
        ::testing::Values(fpta_unsorted, fpta_ascending, fpta_descending)));

INSTANTIATE_TEST_CASE_P(
    Combine, CursorSecondaryDups,
    ::testing::Combine(
        ::testing::Values(fpta_primary_unique_ordered_obverse,
                          fpta_primary_unique_ordered_reverse,
                          fpta_primary_unique_unordered),
        ::testing::Values(fptu_null, fptu_uint16, fptu_int32, fptu_uint32,
                          fptu_fp32, fptu_int64, fptu_uint64, fptu_fp64,
                          fptu_datetime, fptu_96, fptu_128, fptu_160, fptu_256,
                          fptu_cstr, fptu_opaque
                          /*, fptu_nested, fptu_farray */),
        ::testing::Values(fpta_secondary_withdups_ordered_obverse,
                          fpta_secondary_withdups_ordered_reverse,
                          fpta_secondary_withdups_unordered),
        ::testing::Values(fptu_null, fptu_uint16, fptu_int32, fptu_uint32,
                          fptu_fp32, fptu_int64, fptu_uint64, fptu_fp64,
                          fptu_datetime, fptu_96, fptu_128, fptu_160, fptu_256,
                          fptu_cstr, fptu_opaque
                          /*, fptu_nested, fptu_farray */),
        ::testing::Values(fpta_unsorted, fpta_ascending, fpta_descending)));
#else

TEST(CursorSecondary, GoogleTestCombine_IS_NOT_Supported_OnThisPlatform) {}
TEST(CursorSecondaryDups, GoogleTestCombine_IS_NOT_Supported_OnThisPlatform) {}

#endif /* GTEST_HAS_COMBINE */

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

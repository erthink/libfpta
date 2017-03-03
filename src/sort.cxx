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

#include "fast_positive/tuples_internal.h"
#include <algorithm>
#include <functional>

__hot bool fptu_is_ordered(const fptu_field *begin, const fptu_field *end) {
  if (likely(end > begin + 1)) {
    /* При формировании кортежа дескрипторы полей физически размещаются
     * в обратном порядке. Считаем что они в правильном порядке, когда
     * сначала добавляются поля с меньшими номерами. Соответственно,
     * при движении от begin к end, при правильном порядке номера полей
     * будут уменьшаться.
     *
     * Сканируем дескрипторы в направлении от begin к end, от недавно
     * добавленных к первым, ибо предположительно порядок чаще будет
     * нарушаться в результате последних изменений. */
    --end;
    auto scan = begin;
    do {
      auto next = scan;
      ++next;
      if (scan->ct < next->ct)
        return false;
      scan = next;
    } while (scan < end);
  }
  return true;
}

//----------------------------------------------------------------------------

/* Подзадача:
 *  - необходимо формировать сортированный список тегов (тип и номер) полей;
 *  - с фильтрацией дубликатов;
 *  - быстро, с минимумом накладных расходов.
 *  - есть вероятность наличия порядка в полях по тегам;
 *  - высока вероятность что часть полей (начало или конец) отсортированы.
 *
 * Решение:
 * 1) Двигаемся по дескрипторам полей, пока они упорядочены, при этом:
 *    - сразу формируем результирующий вектор;
 *    - фильтруем дубликаты просто пропуская повторяющиеся значения.
 *
 * 2) Встречая нарушения порядка переходим на slowpath:
 *    - фильтруем дубликаты посредством битовой карты;
 *    - в конце сортируем полученный вектор.
 *
 * 3) Оптимизация:
 *    - уменьшаем размер битовой карты опираясь на разброс значений тэгов;
 *    - для этого дизъюнкцией собираем значения тегов в битовой маске;
 *    - велика вероятность что в кортеже нет массивов и не используется
 *      резерный бит, поэтому вычисляем сдвиг для пропуска этих нулей.
 *    - по старшему биту получаем верхний предел для размера битовой карты.
 */

static __noinline uint16_t *fptu_tags_slowpath(uint16_t *const first,
                                               uint16_t *tail,
                                               const fptu_field *const pos,
                                               const fptu_field *const end,
                                               unsigned have) {
  assert(std::is_sorted(first, tail, std::less_equal<uint16_t>()));

  /* собираем в маску оставшиеся значения */
  for (auto i = pos; i < end; ++i)
    have |= i->ct;

  /* вполне вероятно, что резерный бит всегда нулевой, также возможно что
   * нет массивов, тогда размер карты можно сократить в 4 раза. */
  const unsigned blank =
      (have & fptu_fr_mask) ? 0u : (unsigned)fptu_ct_reserve_bits +
                                       ((have & fptu_farray) ? 0u : 1u);
  const unsigned lo_part = (1 << (fptu_typeid_bits + fptu_ct_reserve_bits)) - 1;
  const unsigned hi_part = lo_part ^ UINT16_MAX;
  assert((lo_part >> blank) >= (have & lo_part));
  const auto top = (have & lo_part) + ((have & hi_part) >> blank) + 1;
  const auto word_bits = sizeof(size_t) * 8;

  /* std::bitset прекрасен, но требует инстанцирования под максимальный
   * размер. При этом на стеке будет выделено и заполнено нулями 4K,
   * в итоге расходы превысят экономию. */

  const size_t n_words = (top + word_bits - 1) / word_bits;
#ifdef __GNUC__
  size_t bm[n_words];
#else
  size_t *bm = (size_t *)_alloca(sizeof(size_t) * n_words);
#endif
  memset(bm, 0, sizeof(size_t) * n_words);

  /* отмечаем обработанное */
  for (auto i = first; i < tail; ++i) {
    size_t n = *i;
    assert((lo_part >> blank) >= (n & lo_part));
    n = (n & lo_part) + ((n & hi_part) >> blank);
    assert(n < top);

    bm[n / word_bits] |= (size_t)1 << (n % word_bits);
  }

  /* обрабатываем неупорядоченный остаток */
  for (auto i = pos; i < end; ++i) {
    size_t n = i->ct;
    assert((lo_part >> blank) >= (n & lo_part));
    n = (n & lo_part) + ((n & hi_part) >> blank);
    assert(n < top);

    if ((bm[n / word_bits] & ((size_t)1 << (n % word_bits))) == 0) {
      bm[n / word_bits] |= (size_t)1 << (n % word_bits);
      *tail++ = i->ct;
    }
  }

  std::sort(first, tail);
  assert(std::is_sorted(first, tail, std::less_equal<uint16_t>()));
  return tail;
}

uint16_t *fptu_tags(uint16_t *const first, const fptu_field *const begin,
                    const fptu_field *const end) {
  /* Формирует в буфере упорядоченный список тегов полей без дубликатов. */
  uint16_t *tail = first;
  if (end > begin) {
    const fptu_field *i;
    unsigned have = 0;

    /* Пытаемся угадать текущий порядок и переливаем в буфер
     * пропуская дубликаты. */
    if (begin->ct >= end[-1].ct) {
      for (i = end - 1, *tail++ = i->ct; --i >= begin;) {
        if (i->ct != tail[-1]) {
          if (unlikely(i->ct < tail[-1]))
            return fptu_tags_slowpath(first, tail, begin, i + 1, have);
          have |= (*tail++ = i->ct);
        }
      }
    } else {
      for (i = begin, *tail++ = i->ct; ++i < end;) {
        if (i->ct != tail[-1]) {
          if (unlikely(i->ct < tail[-1]))
            return fptu_tags_slowpath(first, tail, i, end, have);
          have |= (*tail++ = i->ct);
        }
      }
    }
    assert(std::is_sorted(first, tail, std::less_equal<uint16_t>()));
  }
  return tail;
}

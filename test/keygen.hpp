/*
 * Copyright 2016 libfpta authors: please see AUTHORS file.
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

#include "fast_positive/tables_internal.h"
#include <gtest/gtest.h>

#include <array>
#include <cmath>
#include <limits>
#include <map>
#include <type_traits>
#include <typeinfo>
#include <utility>
#include <vector>

/* "хорошие" значения float: близкие к представимым, но НЕ превосходящие их */
static constexpr float flt_neg_below =
    -FLT_MAX + (double)FLT_MAX * FLT_EPSILON;
static constexpr float flt_pos_below =
    FLT_MAX - (double)FLT_MAX * FLT_EPSILON;
static_assert(flt_neg_below > -FLT_MAX, "unexpected precision loss");
static_assert(flt_pos_below < FLT_MAX, "unexpected precision loss");

/* "плохие" значения float: немного вне представимого диапазона */
static constexpr double flt_neg_over =
    -FLT_MAX - (double)FLT_MAX * FLT_EPSILON;
static constexpr double flt_pos_over =
    FLT_MAX + (double)FLT_MAX * FLT_EPSILON;
static_assert(flt_neg_over <= -FLT_MAX, "unexpected precision loss");
static_assert(flt_pos_over >= FLT_MAX, "unexpected precision loss");

//----------------------------------------------------------------------------

bool is_valid4primary(fptu_type type, fpta_index_type index) {
  if (index == fpta_index_none || fpta_index_is_secondary(index))
    return false;

  if (type <= fptu_null || type >= fptu_farray)
    return false;

  if (fpta_index_is_reverse(index) && type < fptu_96)
    return false;

  return true;
}

bool is_valid4cursor(fpta_index_type index, fpta_cursor_options cursor) {
  if (index == fpta_index_none)
    return false;

  if (fpta_cursor_is_ordered(cursor) && !fpta_index_is_ordered(index))
    return false;

  return true;
}

bool is_valid4secondary(fptu_type pk_type, fpta_index_type pk_index,
                        fptu_type se_type, fpta_index_type se_index) {
  (void)pk_type;
  if (!fpta_index_is_unique(pk_index))
    return false;

  if (se_index == fpta_index_none || fpta_index_is_primary(se_index))
    return false;

  if (se_type <= fptu_null || se_type >= fptu_farray)
    return false;

  if (fpta_index_is_reverse(se_index) && se_type < fptu_96)
    return false;

  return true;
}

template <typename container>
bool is_properly_ordered(const container &probe, bool descending = false) {
  if (!descending) {
    return std::is_sorted(probe.begin(), probe.end(),
                          [](const typename container::value_type &left,
                             const typename container::value_type &right) {
                            EXPECT_GT(left.second, right.second);
                            return left.second < right.second;
                          });
  } else {
    return std::is_sorted(probe.begin(), probe.end(),
                          [](const typename container::value_type &left,
                             const typename container::value_type &right) {
                            EXPECT_LT(left.second, right.second);
                            return left.second > right.second;
                          });
  }
}

fpta_value order_checksum(int order, fptu_type type, fpta_index_type index) {
  auto signature = fpta_column_shove(0, type, index);
  return fpta_value_uint(t1ha(&signature, sizeof(signature), order));
}

template <fptu_type data_type, fpta_index_type index_type> struct probe_key {
  fpta_key key;

  static fpta_shove_t shove() {
    return fpta_column_shove(0, data_type, index_type);
  }

  probe_key(const fpta_value &value) {
    fpta_pollute(&key, sizeof(key));
    EXPECT_EQ(FPTA_OK, fpta_index_value2key(shove(), value, key, true));
  }

  const probe_key &operator=(const probe_key &) = delete;
  probe_key(const probe_key &ones) = delete;
  const probe_key &operator=(const probe_key &&) = delete;
  probe_key(const probe_key &&ones) = delete;

  int compare(const probe_key &right) const {
    auto comparator = fpta_index_shove2comparator(shove());
    return comparator(&key.mdbx, &right.key.mdbx);
  }

  bool operator==(const probe_key &right) const {
    return compare(right) == 0;
  }

  bool operator!=(const probe_key &right) const {
    return compare(right) != 0;
  }

  bool operator<(const probe_key &right) const { return compare(right) < 0; }

  bool operator>(const probe_key &right) const { return compare(right) > 0; }
};

template <fptu_type data_type> struct probe_triplet {
  typedef probe_key<data_type, fpta_primary_unique> obverse_key;
  typedef std::map<obverse_key, int> obverse_map;

  typedef probe_key<data_type, fpta_primary_unique_unordered> unordered_key;
  typedef std::map<unordered_key, int> unordered_map;

  typedef probe_key<data_type, fpta_primary_unique_reversed> reverse_key;
  typedef std::map<reverse_key, int> reverse_map;

  obverse_map obverse;
  unordered_map unordered;
  reverse_map reverse;
  unsigned n;

  probe_triplet() : n(0) {}

  static bool has_reverse() { return data_type >= fptu_96; }

  void operator()(const fpta_value &key, int order, bool duplicate = false) {
    if (!duplicate)
      ++n;
    obverse.emplace(std::piecewise_construct, std::forward_as_tuple(key),
                    std::forward_as_tuple(order));
    unordered.emplace(std::piecewise_construct, std::forward_as_tuple(key),
                      std::forward_as_tuple(order));
    // повторяем для проверки сравнения (эти вставки не должны произойти)
    obverse.emplace(std::piecewise_construct, std::forward_as_tuple(key),
                    std::forward_as_tuple(INT_MIN));
    unordered.emplace(std::piecewise_construct, std::forward_as_tuple(key),
                      std::forward_as_tuple(INT_MIN));

    if (has_reverse()) {
      assert(key.type == fpta_binary || key.type == fpta_string);
      uint8_t *begin = (uint8_t *)key.binary_data;
      uint8_t *end = begin + key.binary_length;
      std::reverse(begin, end);
      reverse.emplace(std::piecewise_construct, std::forward_as_tuple(key),
                      std::forward_as_tuple(order));
      // повторяем для проверки сравнения (эта вставка не должна
      // произойти)
      reverse.emplace(std::piecewise_construct, std::forward_as_tuple(key),
                      std::forward_as_tuple(INT_MIN));
    }
  }

  void check(unsigned expected) {
    EXPECT_EQ(expected, n);

    // паранойя на случай повреждения ключей
    ASSERT_TRUE(std::is_sorted(obverse.begin(), obverse.end()));
    ASSERT_TRUE(std::is_sorted(reverse.begin(), reverse.end()));

    // наборы должны содержать все значения
    EXPECT_EQ(expected, unordered.size());
    EXPECT_EQ(expected, obverse.size());
    if (has_reverse())
      EXPECT_EQ(expected, reverse.size());

    // а ordered должны быть также упорядочены
    EXPECT_TRUE(is_properly_ordered(obverse));
    EXPECT_TRUE(is_properly_ordered(reverse));
  }

  void check() { check(n); }
};

//----------------------------------------------------------------------------

template <bool printable>
/* Кошмарная функция для генерации ключей в виде байтовых строк различной
 * длины, которые при этом упорядоченные по параметру order при сравнении
 * посредством memcmp().
 *
 * Для этого старшие биты первого символа кодирует исходную "ширину" order,
 * а младшие биты и последующие символы - значение order в порядке от старших
 * разрядов к младшим. */
bool string_keygen(const size_t len, uint32_t order, uint8_t *buf,
                   uint32_t tailseed = 0) {
  /* параметры алфавита */
  constexpr int alphabet_bits = printable ? 7 : 8;
  constexpr unsigned alphabet_mask = (1 << alphabet_bits) - 1;
  constexpr unsigned alphabet_base = printable ? ' ' : 0;

  /* кол-во бит под кодирование "ширины" */
  constexpr unsigned rle_bits = 5;
  /* максимальная "ширина" order */
  constexpr int max_bits = 1 << rle_bits;
  /* максимальнное значение order */
  constexpr uint64_t max_order = (1ull << max_bits) - 1;
  /* остаток битов в первом символе после "ширины" */
  constexpr int first_left = alphabet_bits - rle_bits;
  constexpr unsigned first_mask = (1 << first_left) - 1;
  assert(len > 0);
  assert(order <= max_order);

  /* считаем ширину order */
  int width = 0;
  while (order >> width) {
    ++width;
    assert(width <= max_bits);
  }

  /* кодируем длину */
  uint8_t rle_val = (width ? width - 1 : 0) << first_left;
  /* вычисляем сколько битов значения остается для остальных символов */
  int left = (width > first_left) ? width - first_left : 0;
  /* первый символ с длиной и самыми старшими разрядами значения */
  *buf = alphabet_base + rle_val + ((order >> left) & first_mask);

  for (auto end = buf + len; ++buf < end;) {
    if (left > 0) {
      left = (left > alphabet_bits) ? left - alphabet_bits : 0;
      *buf = alphabet_base + ((order >> left) & alphabet_mask);
    } else {
      /* дополняем но нужной длины псевдослучайными данными */
      tailseed = tailseed * 1664525u + 1013904223u;
      *buf = alphabet_base + ((tailseed >> 23) & alphabet_mask);
    }
  }

  return left > 0;
}

template <bool printable>
/* Тест поясненного выше кошмара. */
void string_keygen_test(const size_t keylen_min, const size_t keylen_max) {
  assert(keylen_min > 0);
  assert(keylen_max >= keylen_min);

  SCOPED_TRACE(std::string("string_keygen_test: ") +
               (printable ? "string" : "binary") + ", keylen " +
               std::to_string(keylen_min) + "..." +
               std::to_string(keylen_max));

  uint8_t buffer_a[keylen_max + 1];
  uint8_t *prev = buffer_a;
  memset(buffer_a, 0xAA, sizeof(buffer_a));

  uint8_t buffer_b[keylen_max + 1];
  uint8_t *next = buffer_b;
  memset(buffer_b, 0xBB, sizeof(buffer_b));

  size_t keylen = keylen_min;
  EXPECT_FALSE((string_keygen<printable>(keylen, 0, prev)));

  uint8_t buffer_c[keylen_max + 1 + printable];
  memset(buffer_c, 0xCC, sizeof(buffer_c));

  if (keylen < keylen_max) {
    EXPECT_FALSE((string_keygen<printable>(keylen + 1, 0, buffer_c)));
    ASSERT_LE(0, memcmp(prev, buffer_c, keylen));
  }

  unsigned order = 1;
  while (keylen <= keylen_max && order < INT32_MAX) {
    memset(next, 0, keylen_max);
    bool key_is_too_short = string_keygen<printable>(keylen, order, next);
    if (key_is_too_short) {
      keylen++;
      continue;
    }

    auto memcmp_result = memcmp(prev, next, keylen_max);
    if (memcmp_result >= 0) {
      SCOPED_TRACE("keylen " + std::to_string(keylen) + ", order " +
                   std::to_string(order));
      ASSERT_LT(0, memcmp_result);
    }

    if (keylen < keylen_max) {
      memset(buffer_c, -1, keylen + 1);
      EXPECT_FALSE((string_keygen<printable>(keylen + 1, order, buffer_c)));
      memcmp_result = memcmp(buffer_c, next, keylen);
      if (memcmp_result < 0) {
        SCOPED_TRACE("keylen " + std::to_string(keylen) + ", order " +
                     std::to_string(order));
        ASSERT_GE(0, memcmp_result);
      }
    }

    std::swap(prev, next);
    order += (order & (1024 + 2048 + 4096)) ? (113 + order / 16) : 1;
    if (order >= INT32_MAX / keylen_max * keylen)
      keylen++;
  }
}

//----------------------------------------------------------------------------

template <typename type, unsigned N>
/* Позволяет за N шагов "простучать" весь диапазон значений type,
 * явно включая крайние точки, нуль и бесконечности (при наличии). */
struct scalar_range_stepper {
  static constexpr long scope_neg =
      std::is_signed<type>()
          ? (N - 1) / 2 - std::numeric_limits<type>::has_infinity
          : 0;
  static constexpr long scope_pos =
      N - scope_neg - 1 - std::numeric_limits<type>::has_infinity * 2;
  static_assert(!std::is_signed<type>() ||
                    std::numeric_limits<type>::lowest() < 0,
                "expected lowest() < 0 for signed types");
  static_assert(scope_pos > 1, "seems N is too small");
  static_assert(std::numeric_limits<type>::max() > scope_pos,
                "seems N is too big");

  static constexpr int order_from = 0;
  static constexpr int order_to = N - 1;
  typedef std::map<type, int> container4test;

  static type value(int order) {
    if (std::is_signed<type>()) {
      if (std::numeric_limits<type>::has_infinity && order-- == 0)
        return -std::numeric_limits<type>::infinity();
      if (order < scope_neg) {
        type offset =
            (std::numeric_limits<type>::max() < INT_MAX)
                ? std::numeric_limits<type>::lowest() * order / scope_neg
                : std::numeric_limits<type>::lowest() / scope_neg * order;

        return std::numeric_limits<type>::lowest() - offset;
      }
      order -= scope_neg;
    }

    if (order == 0)
      return type(0);

    if (std::numeric_limits<type>::has_infinity && order > scope_pos)
      return std::numeric_limits<type>::infinity();
    if (order == scope_pos)
      return std::numeric_limits<type>::max();
    return (std::numeric_limits<type>::max() < INT_MAX)
               ? std::numeric_limits<type>::max() * order / scope_pos
               : std::numeric_limits<type>::max() / scope_pos * order;
  }

  static void test() {
    SCOPED_TRACE(std::string("scalar_range_stepper: ") +
                 std::string(::testing::internal::GetTypeName<type>()) +
                 ", N=" + std::to_string(N));

    container4test probe;

    unsigned n = 0;
    for (auto i = order_from; i <= order_to; ++i) {
      probe[value(i)] = i;
      ++n;
    }

    bool is_properly_ordered =
        std::is_sorted(probe.begin(), probe.end(),
                       [](const typename container4test::value_type &left,
                          const typename container4test::value_type &right) {
                         return left.second < right.second;
                       });

    EXPECT_TRUE(is_properly_ordered);
    if (std::numeric_limits<type>::has_infinity) {
      EXPECT_EQ(1, probe.count(-std::numeric_limits<type>::infinity()));
      EXPECT_EQ(1, probe.count(std::numeric_limits<type>::infinity()));
    }
    EXPECT_EQ(N, n);
    EXPECT_EQ(n, probe.size());
    EXPECT_EQ(1, probe.count(type(0)));
    EXPECT_EQ(1, probe.count(std::numeric_limits<type>::max()));
    // EXPECT_EQ(1, probe.count(std::numeric_limits<type>::min()));
    EXPECT_EQ(1, probe.count(std::numeric_limits<type>::lowest()));
  }
};

//----------------------------------------------------------------------------

template <fpta_index_type index, fptu_type type> struct keygen_base {

  static fpta_value invalid(int order) {
    switch (order) {
    case 0:
      return fpta_value_null();
    case 1:
      if (type == fptu_int32 || type == fptu_int64)
        break;
      return fpta_value_sint(-1);
    case 2:
      if (type == fptu_int32 || type == fptu_int64 || type == fptu_uint32 ||
          type == fptu_uint64)
        break;
      return fpta_value_uint(INT16_MAX + 1);
    case 3:
      if (type == fptu_fp32 || type == fptu_fp64)
        break;
      return fpta_value_float(42);
    case 4:
      if (type == fptu_cstr)
        break;
      return fpta_value_cstr("42");
    case 5:
      if (type == fptu_opaque)
        break;
      return fpta_value_binary("42", 2);
    default:
      // возвращаем end как признак окончания итерации по order
      return fpta_value_end();
    }
    // возвращаем begin как признак пропуска текущей итерации по order
    return fpta_value_begin();
  }
};

template <fpta_index_type index, fptu_type type, unsigned N>
struct keygen : public keygen_base<index, type> {
  static constexpr int order_from = 0;
  static constexpr int order_to = 0;

  static fpta_value make(int order) {
    SCOPED_TRACE("FIXME: make(), type " + std::to_string(type) + ", index " +
                 std::to_string(index) + ", " __FILE__
                                         ": " FPT_STRINGIFY(__LINE__));
    (void)order;
    ADD_FAILURE();
    return fpta_value_end();
  }
};

//----------------------------------------------------------------------------

template <unsigned keylen, unsigned N> struct fixbin_stepper {
  static constexpr long scope = N - 2;
  static constexpr int order_from = 0;
  static constexpr int order_to = N - 1;
  typedef std::array<uint8_t, keylen> fixbin_type;

  static fpta_value make(int order, bool reverse) {
    /* нужен static, ибо внутри fpta_value только указатель на данные */
    static fixbin_type holder;

    if (order == 0)
      memset(&holder, 0, keylen);
    else if (order > scope)
      memset(&holder, ~0, keylen);
    else {
      bool key_is_too_short = string_keygen<false>(
          keylen, INT32_MAX / scope * (order - 1), holder.begin());
      EXPECT_FALSE(key_is_too_short);
    }

    if (reverse)
      std::reverse(holder.begin(), holder.end());

    return fpta_value_binary(&holder, sizeof(holder));
  }

  static void test() {
    SCOPED_TRACE(std::string("fixbin_stepper: keylen ") +
                 std::to_string(keylen) + ", N=" + std::to_string(N));

    std::map<fixbin_type, int> probe;

    unsigned n = 0;
    for (auto i = order_from; i <= order_to; ++i) {
      fixbin_type *value = (fixbin_type *)make(i, false).binary_data;
      probe[*value] = i;
      ++n;
    }

    EXPECT_TRUE(is_properly_ordered(probe));
    EXPECT_EQ(N, n);
    EXPECT_EQ(n, probe.size());

    fixbin_type value;
    memset(&value, 0, sizeof(value));
    EXPECT_EQ(1, probe.count(value));

    memset(&value, 0xff, sizeof(value));
    EXPECT_EQ(1, probe.count(value));

    memset(&value, 0x42, sizeof(value));
    EXPECT_EQ(0, probe.count(value));
  }
};

//----------------------------------------------------------------------------

template <fptu_type data_type>
fpta_value fpta_value_binstr(const void *pattern, size_t length) {
  return (data_type == fptu_cstr)
             ? fpta_value_string((const char *)pattern, length)
             : fpta_value_binary(pattern, length);
}

template <fptu_type data_type, unsigned N> struct varbin_stepper {
  static constexpr long scope = N - 2;
  static constexpr int order_from = 0;
  static constexpr int order_to = N - 1;
  static constexpr size_t keylen_max = fpta_max_keylen * 3 / 2;
  typedef std::array<uint8_t, keylen_max> varbin_type;

  static fpta_value make(int order, bool reverse) {
    /* нужен static, ибо внутри fpta_value только указатель на данные */
    static varbin_type holder;

    if (order == 0)
      return fpta_value_binstr<data_type>(nullptr, 0);

    if (order > scope) {
      memset(&holder, ~0, keylen_max);
      return fpta_value_binstr<data_type>(holder.begin(), keylen_max);
    }

    unsigned keylen = 1 + ((order - 1) % 37) * (keylen_max - 1) / 37;
    while (keylen <= keylen_max) {
      bool key_is_too_short = string_keygen<data_type == fptu_cstr>(
          keylen, INT32_MAX / scope * (order - 1), holder.begin());
      if (!key_is_too_short)
        break;
      keylen++;
    }

    EXPECT_TRUE(keylen <= keylen_max);
    if (reverse)
      std::reverse(holder.begin(), holder.begin() + keylen);

    return fpta_value_binstr<data_type>(holder.begin(), keylen);
  }

  static void test() {
    SCOPED_TRACE(std::string("varbin_stepper: ") + std::to_string(data_type) +
                 ", N=" + std::to_string(N));

    std::map<std::vector<uint8_t>, int> probe;

    unsigned n = 0;
    for (auto i = order_from; i <= order_to; ++i) {
      auto value = make(i, false);
      uint8_t *ptr = (uint8_t *)value.binary_data;
      probe[std::vector<uint8_t>(ptr, ptr + value.binary_length)] = i;
      ++n;
    }

    EXPECT_TRUE(is_properly_ordered(probe));
    EXPECT_EQ(N, n);
    EXPECT_EQ(n, probe.size());

    std::vector<uint8_t> value;
    EXPECT_EQ(1, probe.count(value));

    value.resize(keylen_max, 255);
    EXPECT_EQ(1, probe.count(value));

    value.clear();
    value.resize(keylen_max / 2, 42);
    EXPECT_EQ(0, probe.count(value));
  }
};

//----------------------------------------------------------------------------

template <fpta_index_type index, unsigned N>
struct keygen<index, fptu_uint16, N>
    : public scalar_range_stepper<uint16_t, N> {
  static constexpr fptu_type type = fptu_uint16;
  static fpta_value make(int order) {
    return fpta_value_uint(scalar_range_stepper<uint16_t, N>::value(order));
  }
};

template <fpta_index_type index, unsigned N>
struct keygen<index, fptu_uint32, N>
    : public scalar_range_stepper<uint32_t, N> {
  static constexpr fptu_type type = fptu_uint32;
  static fpta_value make(int order) {
    return fpta_value_uint(scalar_range_stepper<uint32_t, N>::value(order));
  }
};

template <fpta_index_type index, unsigned N>
struct keygen<index, fptu_uint64, N>
    : public scalar_range_stepper<uint64_t, N> {
  static constexpr fptu_type type = fptu_uint64;
  static fpta_value make(int order) {
    return fpta_value_uint(scalar_range_stepper<uint64_t, N>::value(order));
  }
};

template <fpta_index_type index, unsigned N>
struct keygen<index, fptu_int32, N>
    : public scalar_range_stepper<int32_t, N> {
  static constexpr fptu_type type = fptu_int32;
  static fpta_value make(int order) {
    return fpta_value_sint(scalar_range_stepper<int32_t, N>::value(order));
  }
};

template <fpta_index_type index, unsigned N>
struct keygen<index, fptu_int64, N>
    : public scalar_range_stepper<int64_t, N> {
  static constexpr fptu_type type = fptu_int64;
  static fpta_value make(int order) {
    return fpta_value_sint(scalar_range_stepper<int64_t, N>::value(order));
  }
};

template <fpta_index_type index, unsigned N>
struct keygen<index, fptu_fp32, N> : public scalar_range_stepper<float, N> {
  static constexpr fptu_type type = fptu_fp32;
  static fpta_value make(int order) {
    return fpta_value_float(scalar_range_stepper<float, N>::value(order));
  }
};

template <fpta_index_type index, unsigned N>
struct keygen<index, fptu_fp64, N> : public scalar_range_stepper<double, N> {
  static constexpr fptu_type type = fptu_fp64;
  static fpta_value make(int order) {
    return fpta_value_float(scalar_range_stepper<double, N>::value(order));
  }
};

template <fpta_index_type index, unsigned N>
struct keygen<index, fptu_96, N> : public fixbin_stepper<96 / 8, N>,
                                   keygen_base<index, fptu_96> {
  static constexpr fptu_type type = fptu_96;
  static fpta_value make(int order) {
    return fixbin_stepper<96 / 8, N>::make(order,
                                           fpta_index_is_reverse(index));
  }
};

template <fpta_index_type index, unsigned N>
struct keygen<index, fptu_128, N> : public fixbin_stepper<128 / 8, N> {
  static constexpr fptu_type type = fptu_128;
  static fpta_value make(int order) {
    return fixbin_stepper<128 / 8, N>::make(order,
                                            fpta_index_is_reverse(index));
  }
};

template <fpta_index_type index, unsigned N>
struct keygen<index, fptu_160, N> : public fixbin_stepper<160 / 8, N> {
  static constexpr fptu_type type = fptu_160;
  static fpta_value make(int order) {
    return fixbin_stepper<160 / 8, N>::make(order,
                                            fpta_index_is_reverse(index));
  }
};

template <fpta_index_type index, unsigned N>
struct keygen<index, fptu_192, N> : public fixbin_stepper<192 / 8, N> {
  static constexpr fptu_type type = fptu_192;
  static fpta_value make(int order) {
    return fixbin_stepper<192 / 8, N>::make(order,
                                            fpta_index_is_reverse(index));
  }
};

template <fpta_index_type index, unsigned N>
struct keygen<index, fptu_256, N> : public fixbin_stepper<256 / 8, N> {
  static constexpr fptu_type type = fptu_256;
  static fpta_value make(int order) {
    return fixbin_stepper<256 / 8, N>::make(order,
                                            fpta_index_is_reverse(index));
  }
};

template <fpta_index_type index, unsigned N>
struct keygen<index, fptu_cstr, N> : public varbin_stepper<fptu_cstr, N> {
  static constexpr fptu_type type = fptu_cstr;
  static fpta_value make(int order) {
    return varbin_stepper<fptu_cstr, N>::make(order,
                                              fpta_index_is_reverse(index));
  }
};

template <fpta_index_type index, unsigned N>
struct keygen<index, fptu_opaque, N> : public varbin_stepper<fptu_opaque, N> {
  static constexpr fptu_type type = fptu_opaque;
  static fpta_value make(int order) {
    return varbin_stepper<fptu_opaque, N>::make(order,
                                                fpta_index_is_reverse(index));
  }
};

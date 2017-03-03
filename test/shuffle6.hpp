// https://en.wikipedia.org/wiki/Gray_code
unsigned gray_code(unsigned n) { return n ^ (n >> 1); }

/* Итерирующий генератор перестановок из 6 элементов (720 вариантов).
 *
 * В отличии от std::next_permination() и std::prev_permination()
 * позволяет получить перестановку непосредственно по номеру. */
class shuffle6 {
  // набор элементов для перестановок в виде 4-х битовых групп
  static const unsigned item_set = 0x543210;

  // оставшиеся элементы в виде битовой строки
  unsigned element_bits;

  // состояние генератора от номера перестановки
  unsigned factor_state;

  // количество оставшихся элементов
  unsigned left;

  // из битовой строки bits выкусывает n-ую 4-битовую группу
  unsigned cutout(unsigned n) {
    assert(n < 6);
    const unsigned all1 = ~0u;
    const unsigned shift = 4 * n;
    const unsigned group = (element_bits >> shift) & 15;
    const unsigned higher = element_bits & ((all1 << 4) << shift);
    const unsigned lower = element_bits & ~(all1 << shift);
    element_bits = lower | (higher >> 4);
    return group;
  }

public:
  // количество перестановок из 6 элементов (6! == 720)
  static const unsigned factorial = 1 * 2 * 3 * 4 * 5 * 6;

  /* инициализирует генератор для выдачи последовательности
   * с заданным номером. */
  shuffle6(unsigned shuffle_order = 0) { setup(shuffle_order); }

  void setup(unsigned shuffle_order) {
    factor_state = shuffle_order % factorial;
    element_bits = item_set;
    left = 6;
  }

  // выдает очередной элемент в конкретной перестановке
  unsigned next() {
    assert(left > 0);
    unsigned n = factor_state % left;
    factor_state /= left;
    --left;
    return cutout(n);
  }

  unsigned operator()() { return next(); }

  bool empty() const { return left == 0; }

  static bool selftest() {
    // сначала проверяем порядок
    shuffle6 first(0), last(factorial - 1);
    for (unsigned n = 0; n < 6; n++) {
      if (first.next() != n)
        return false;
      if (last.next() != 5 - n)
        return false;
    }

    // теперь полный перебор комбинаций (6! == 720)
    for (unsigned n = 0; n < factorial; ++n) {
      shuffle6 here(n);

      unsigned probe, i;
      for (probe = 0, i = 0; i < 6; ++i)
        probe |= 1 << here.next();

      if (probe != 63)
        return false;
    }
    return true;
  }
};

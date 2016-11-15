// https://en.wikipedia.org/wiki/Gray_code
unsigned gray_code(unsigned n) { return n ^ (n >> 1); }

// итерирующий генератор перестановок из 6 элементов
class shuffle6
{
    // элементы для перестановок в виде 4-х битовых групп
    static const unsigned item_set = 0x543210;

    // оставшиеся элементы в виде битовой строки
    unsigned element_bits;

    // состояние генератора от номера перестановки
    unsigned factor_state;

    // количество оставшихся элементов
    unsigned left;

    // из битовой строки bits выкусывает n-ую 4-битовую группу
    unsigned cutout(unsigned n)
    {
        assert(n < 6);
        unsigned s = 4 * n;
        unsigned g = (element_bits >> s) & 15;
        unsigned h = element_bits & (-16 << s);
        unsigned l = element_bits & ~(-1 << s);
        element_bits = l | (h >> 4);
        return g;
    }

  public:
    // количество перестановок из 6 элементов
    static const unsigned factorial = 1 * 2 * 3 * 4 * 5 * 6;

    shuffle6(unsigned shuffle_order = 0) { setup(shuffle_order); }

    void setup(unsigned shuffle_order)
    {
        factor_state = shuffle_order % factorial;
        element_bits = item_set;
        left = 6;
    }

    // выдает очередной элемент в конкретной перестановке
    unsigned next()
    {
        assert(left > 0);
        unsigned n = factor_state % left;
        factor_state /= left;
        --left;
        return cutout(n);
    }

    bool empty() const { return left == 0; }

    static bool selftest()
    {
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

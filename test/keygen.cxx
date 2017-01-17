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

#include "keygen.hpp"

template <fpta_index_type _index, fptu_type _type>
void any_keygen::init_tier::glue() {
  type = _type;
  index = _index;
  maker = keygen<_index, _type>::make;
}

template <fpta_index_type _index>
void any_keygen::init_tier::unroll(fptu_type _type) {
  switch (_type) {
  default /* arrays */:
  case fptu_null:
    assert(false && "wrong type");
    return stub(_type, _index);
  case fptu_uint16:
    return glue<_index, fptu_uint16>();
  case fptu_int32:
    return glue<_index, fptu_int32>();
  case fptu_uint32:
    return glue<_index, fptu_uint32>();
  case fptu_fp32:
    return glue<_index, fptu_fp32>();
  case fptu_int64:
    return glue<_index, fptu_int64>();
  case fptu_uint64:
    return glue<_index, fptu_uint64>();
  case fptu_fp64:
    return glue<_index, fptu_fp64>();
  case fptu_96:
    return glue<_index, fptu_96>();
  case fptu_128:
    return glue<_index, fptu_128>();
  case fptu_160:
    return glue<_index, fptu_160>();
  case fptu_datetime:
    return glue<_index, fptu_datetime>();
  case fptu_256:
    return glue<_index, fptu_256>();
  case fptu_cstr:
    return glue<_index, fptu_cstr>();
  case fptu_opaque:
    return glue<_index, fptu_opaque>();
  case fptu_nested:
    return glue<_index, fptu_nested>();
  }
}

any_keygen::init_tier::init_tier(fptu_type _type, fpta_index_type _index) {
  switch (_index) {
  default:
    assert(false && "wrong index");
    stub(_type, _index);
    break;
  case fpta_primary_withdups:
    unroll<fpta_primary_withdups>(_type);
    break;
  case fpta_primary_unique:
    unroll<fpta_primary_unique>(_type);
    break;
  case fpta_primary_withdups_unordered:
    unroll<fpta_primary_withdups_unordered>(_type);
    break;
  case fpta_primary_unique_unordered:
    unroll<fpta_primary_unique_unordered>(_type);
    break;
  case fpta_primary_withdups_reversed:
    unroll<fpta_primary_withdups_reversed>(_type);
    break;
  case fpta_primary_unique_reversed:
    unroll<fpta_primary_unique_reversed>(_type);
    break;
  case fpta_secondary_withdups:
    unroll<fpta_secondary_withdups>(_type);
    break;
  case fpta_secondary_unique:
    unroll<fpta_secondary_unique>(_type);
    break;
  case fpta_secondary_withdups_unordered:
    unroll<fpta_secondary_withdups_unordered>(_type);
    break;
  case fpta_secondary_unique_unordered:
    unroll<fpta_secondary_unique_unordered>(_type);
    break;
  case fpta_secondary_withdups_reversed:
    unroll<fpta_secondary_withdups_reversed>(_type);
    break;
  case fpta_secondary_unique_reversed:
    unroll<fpta_secondary_unique_reversed>(_type);
    break;
  }
}

any_keygen::any_keygen(const init_tier &init, fptu_type type,
                       fpta_index_type index)
    : maker(init.maker) {
  // страховка от опечаток в параметрах при инстанцировании шаблонов.
  assert(init.type == type);
  assert(init.index == index);
}

any_keygen::any_keygen(fptu_type type, fpta_index_type index)
    : any_keygen(init_tier(type, index), type, index) {}

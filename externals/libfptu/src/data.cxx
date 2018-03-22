/*
 * Copyright 2016-2018 libfptu authors: please see AUTHORS file.
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

const char fptu_empty_cstr[1] = {'\0'};

const uint8_t fptu_internal_map_t2b[fptu_cstr] = {
    /* void */ 0,
    /* uint16 */ 0,

    /* int32 */ 4,
    /* unt32 */ 4,
    /* fp32 */ 4,

    /* int64 */ 8,
    /* uint64 */ 8,
    /* fp64 */ 8,
    /* datetime */ 8,

    /* 96 */ 12,
    /* 128 */ 16,
    /* 160 */ 20,
    /* 256 */ 32};

const uint8_t fptu_internal_map_t2u[fptu_cstr] = {
    /* void */ 0,
    /* uint16 */ 0,

    /* int32 */ 1,
    /* unt32 */ 1,
    /* fp32 */ 1,

    /* int64 */ 2,
    /* uint64 */ 2,
    /* fp64 */ 2,
    /* datetime */ 2,

    /* 96 */ 3,
    /* 128 */ 4,
    /* 160 */ 5,
    /* 256 */ 8};

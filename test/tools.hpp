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

#pragma once

struct db_deleter : public std::unary_function<void, fpta_db *> {
  void operator()(fpta_db *db) const {
    if (db)
      EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));
  }
};

struct txn_deleter : public std::unary_function<void, fpta_txn *> {
  void operator()(fpta_txn *txn) const {
    if (txn)
      ASSERT_EQ(FPTA_OK, fpta_transaction_end(txn, true));
  }
};

struct cursor_deleter : public std::unary_function<void, fpta_cursor *> {
  void operator()(fpta_cursor *cursor) const {
    if (cursor)
      ASSERT_EQ(FPTA_OK, fpta_cursor_close(cursor));
  }
};

typedef std::unique_ptr<fpta_db, db_deleter> scoped_db_guard;
typedef std::unique_ptr<fpta_txn, txn_deleter> scoped_txn_guard;
typedef std::unique_ptr<fpta_cursor, cursor_deleter> scoped_cursor_guard;

//----------------------------------------------------------------------------

/* простейший медленный тест на простоту */
bool isPrime(unsigned number);

//----------------------------------------------------------------------------

static __inline int value2key(fpta_shove_t shove, const fpta_value &value,
                              fpta_key &key) {
  return __fpta_index_value2key(shove, &value, &key);
}

static __inline MDB_cmp_func *shove2comparator(fpta_shove_t shove) {
  return (MDB_cmp_func *)__fpta_index_shove2comparator(shove);
}

//----------------------------------------------------------------------------

inline bool is_valid4primary(fptu_type type, fpta_index_type index) {
  if (index == fpta_index_none || fpta_index_is_secondary(index))
    return false;

  if (type <= fptu_null || type >= fptu_farray)
    return false;

  if (fpta_index_is_reverse(index) && type < fptu_96)
    return false;

  return true;
}

inline bool is_valid4cursor(fpta_index_type index, fpta_cursor_options cursor) {
  if (index == fpta_index_none)
    return false;

  if (fpta_cursor_is_ordered(cursor) && !fpta_index_is_ordered(index))
    return false;

  return true;
}

inline bool is_valid4secondary(fptu_type pk_type, fpta_index_type pk_index,
                               fptu_type se_type, fpta_index_type se_index) {
  (void)pk_type;
  if (pk_index == fpta_index_none || !fpta_index_is_unique(pk_index))
    return false;

  if (se_index == fpta_index_none || fpta_index_is_primary(se_index))
    return false;

  if (se_type <= fptu_null || se_type >= fptu_farray)
    return false;

  if (fpta_index_is_reverse(se_index) && se_type < fptu_96)
    return false;

  return true;
}

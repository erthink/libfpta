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

#include <memory>

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

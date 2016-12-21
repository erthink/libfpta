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

int fpta_secondary_upsert(fpta_txn *txn, fpta_name *table_id,
                          fpta_key &pk_key, const fptu_ro &row_old,
                          const fptu_ro &row_new)
{
    (void)txn;
    (void)table_id;
    (void)pk_key;
    (void)row_old;
    (void)row_new;
    return FPTA_ENOIMP;
}

int fpta_secondary_remove(fpta_txn *txn, fpta_name *table_id,
                          fpta_key &pk_key, const fptu_ro &row_old)
{
    (void)txn;
    (void)table_id;
    (void)pk_key;
    (void)row_old;
    return FPTA_ENOIMP;
}

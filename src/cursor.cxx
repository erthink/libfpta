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

#include "details.h"

/* Подставляется в качестве адреса для ключей нулевой длины,
 * с тем чтобы отличать от nullptr */
static char NIL;

int fpta_cursor_close(fpta_cursor *cursor) {
  int rc = fpta_cursor_validate(cursor, fpta_read);

  if (likely(rc == FPTA_SUCCESS) || rc == FPTA_TXN_CANCELLED) {
    mdbx_cursor_close(cursor->mdbx_cursor);
    fpta_cursor_free(cursor->db, cursor);
  }

  return rc;
}

int fpta_cursor_open(fpta_txn *txn, fpta_name *column_id, fpta_value range_from,
                     fpta_value range_to, fpta_filter *filter,
                     fpta_cursor_options op, fpta_cursor **pcursor) {
  if (unlikely(pcursor == nullptr))
    return FPTA_EINVAL;
  *pcursor = nullptr;

  switch (op) {
  default:
    return FPTA_EFLAG;

  case fpta_descending:
  case fpta_descending_dont_fetch:
  case fpta_unsorted:
  case fpta_unsorted_dont_fetch:
  case fpta_ascending:
  case fpta_ascending_dont_fetch:
    break;
  }

  int rc = fpta_id_validate(column_id, fpta_column);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  fpta_name *table_id = column_id->column.table;
  rc = fpta_name_refresh_couple(txn, table_id, column_id);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (unlikely(!fpta_is_indexed(column_id->shove)))
    return FPTA_NO_INDEX;

  const fpta_index_type index = fpta_shove2index(column_id->shove);
  if (!fpta_index_is_ordered(index)) {
    if (unlikely(fpta_cursor_is_ordered(op)))
      return FPTA_NO_INDEX;
    if (unlikely(range_from.type != fpta_begin && range_to.type != fpta_end))
      return FPTA_NO_INDEX;
  }

  if (unlikely(!fpta_index_is_compat(column_id->shove, range_from) ||
               !fpta_index_is_compat(column_id->shove, range_to)))
    return FPTA_ETYPE;

  if (unlikely(range_from.type == fpta_end || range_to.type == fpta_begin))
    return FPTA_EINVAL;

  if (unlikely(!fpta_filter_validate(filter)))
    return FPTA_EINVAL;

  rc = fpta_name_refresh_filter(txn, column_id->column.table, filter);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  MDBX_dbi tbl_handle, idx_handle;
  rc = fpta_open_column(txn, column_id, tbl_handle, idx_handle);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  fpta_db *db = txn->db;
  fpta_cursor *cursor = fpta_cursor_alloc(db);
  if (unlikely(cursor == nullptr))
    return FPTA_ENOMEM;

  cursor->options = op;
  cursor->txn = txn;
  cursor->filter = filter;
  cursor->table_id = table_id;
  cursor->column_number = column_id->column.num;
  cursor->tbl_handle = tbl_handle;
  cursor->idx_handle = idx_handle;

  if (range_from.type != fpta_begin) {
    rc = fpta_index_value2key(cursor->index_shove(), range_from,
                              cursor->range_from_key, true);
    if (unlikely(rc != FPTA_SUCCESS))
      goto bailout;
    assert(cursor->range_from_key.mdbx.iov_base != nullptr);
  }

  if (range_to.type != fpta_end) {
    rc = fpta_index_value2key(cursor->index_shove(), range_to,
                              cursor->range_to_key, true);
    if (unlikely(rc != FPTA_SUCCESS))
      goto bailout;
    assert(cursor->range_to_key.mdbx.iov_base != nullptr);
  }

  rc =
      mdbx_cursor_open(txn->mdbx_txn, cursor->idx_handle, &cursor->mdbx_cursor);
  if (unlikely(rc != MDBX_SUCCESS))
    goto bailout;

  if ((op & fpta_dont_fetch) == 0) {
    rc = fpta_cursor_move(cursor, fpta_first);
    if (unlikely(rc != MDBX_SUCCESS))
      goto bailout;
  }

  *pcursor = cursor;
  return FPTA_SUCCESS;

bailout:
  if (cursor->mdbx_cursor)
    mdbx_cursor_close(cursor->mdbx_cursor);
  fpta_cursor_free(db, cursor);
  return rc;
}

//----------------------------------------------------------------------------

static int fpta_cursor_seek(fpta_cursor *cursor, MDBX_cursor_op mdbx_seek_op,
                            MDBX_cursor_op mdbx_step_op,
                            const MDBX_val *mdbx_seek_key,
                            const MDBX_val *mdbx_seek_data) {
  assert(mdbx_seek_key != &cursor->current);
  int rc;
  fptu_ro mdbx_data;
  mdbx_data.sys.iov_base = nullptr;
  mdbx_data.sys.iov_len = 0;

  if (likely(mdbx_seek_key == NULL)) {
    assert(mdbx_seek_data == NULL);
    rc = mdbx_cursor_get(cursor->mdbx_cursor, &cursor->current, &mdbx_data.sys,
                         mdbx_seek_op);
  } else {
    /* Помещаем целевой ключ и данные (адреса и размеры)
     * в cursor->current и mdbx_data, это требуется для того чтобы:
     *   - после возврата из mdbx_cursor_get() в cursor->current и в mdbx_data
     *     уже были указатели на ключ и данные в БД, без необходимости
     *     еще одного вызова mdbx_cursor_get(MDBX_GET_CURRENT).
     *   - если передать непосредственно mdbx_seek_key и mdbx_seek_data,
     *     то исходные значения будут потеряны (перезаписаны), что создаст
     *     сложности при последующей корректировке позиции. Например, для
     *     перемещения за lower_bound для descending в fpta_cursor_locate().
     */
    cursor->current.iov_len = mdbx_seek_key->iov_len;
    cursor->current.iov_base =
        /* Замещаем nullptr для ключей нулевой длинны, так чтобы
         * в курсоре стоящем на строке с ключом нулевой длины
         * cursor->current.iov_base != nullptr, и тем самым курсор
         * не попадал под критерий is_poor() */
        mdbx_seek_key->iov_base ? mdbx_seek_key->iov_base : &NIL;

    if (!mdbx_seek_data) {
      rc = mdbx_cursor_get(cursor->mdbx_cursor, &cursor->current,
                           &mdbx_data.sys, mdbx_seek_op);
    } else {
      mdbx_data.sys = *mdbx_seek_data;
      rc = mdbx_cursor_get(cursor->mdbx_cursor, &cursor->current,
                           &mdbx_data.sys, mdbx_seek_op);
      if (likely(rc == MDBX_SUCCESS))
        rc = mdbx_cursor_get(cursor->mdbx_cursor, &cursor->current,
                             &mdbx_data.sys, MDBX_GET_CURRENT);
    }

    if (rc == MDBX_SUCCESS) {
      assert(cursor->current.iov_base != mdbx_seek_key->iov_base);
      if (mdbx_seek_data)
        assert(mdbx_data.sys.iov_base != mdbx_seek_data->iov_base);
    }

    if (fpta_cursor_is_descending(cursor->options) &&
        (mdbx_seek_op == MDBX_GET_BOTH_RANGE ||
         mdbx_seek_op == MDBX_SET_RANGE)) {
      /* Корректировка перемещения для курсора с сортировкой по-убыванию.
       *
       * Внутри mdbx_cursor_get() выполняет позиционирование аналогично
       * std::lower_bound() при сортировке по-возрастанию. Поэтому при
       * поиске для курсора с сортировкой в обратном порядке необходимо
       * выполнить махинации:
       *  - Если ключ в фактически самой последней строке оказался меньше
       *    искомого, то при результате MDBX_NOTFOUND от mdbx_cursor_get()
       *    следует к последней строке, что будет соответствовать переходу
       *    к самой первой позиции при обратной сортировке.
       *  - Если искомый ключ не найден и курсор стоит на фактически самой
       *    первой строке, то следует вернуть результат "нет данных", что
       *    будет соответствовать поведению lower_bound при сортировке
       *    в обратном порядке.
       *  - Если искомый ключ найден, то перейти к "первой" равной строке
       *    в порядке курсора, что означает перейти к последнему дубликату.
       *    По эстетическим соображениям этот переход реализован не здесь,
       *    а в fpta_cursor_locate().
       */
      if (rc == MDBX_SUCCESS &&
          mdbx_cursor_on_first(cursor->mdbx_cursor) == MDBX_RESULT_TRUE &&
          mdbx_cmp(cursor->txn->mdbx_txn, cursor->idx_handle, &cursor->current,
                   mdbx_seek_key) < 0) {
        goto eof;
      } else if (rc == MDBX_NOTFOUND &&
                 mdbx_cursor_on_last(cursor->mdbx_cursor) == MDBX_RESULT_TRUE) {
        rc = mdbx_cursor_get(cursor->mdbx_cursor, &cursor->current,
                             &mdbx_data.sys, MDBX_LAST);
      }
    }
  }

  while (rc == MDBX_SUCCESS) {
    MDBX_cursor_op step_op = mdbx_step_op;

    if (cursor->range_from_key.mdbx.iov_base &&
        mdbx_cmp(cursor->txn->mdbx_txn, cursor->idx_handle, &cursor->current,
                 &cursor->range_from_key.mdbx) < 0) {
      /* задана нижняя граница диапазона и текущий ключ меньше её */
      switch (step_op) {
      default:
        assert(false);
      case MDBX_PREV_DUP:
      case MDBX_NEXT_DUP:
        /* нет смысла идти по дубликатам (без изменения значения ключа) */
        break;
      case MDBX_PREV:
        step_op = MDBX_PREV_NODUP;
      // fall through
      case MDBX_PREV_NODUP:
        /* идти в сторону уменьшения ключа есть смысл только в случае
         * unordered (хэшированного) индекса, при этом логично пропустить
         * все дубликаты, так как они заведомо не попадают в диапазон курсора */
        if (!fpta_index_is_ordered(cursor->index_shove()))
          goto next;
        break;
      case MDBX_NEXT:
        /* при движении в сторону увеличения ключа логично пропустить все
         * дубликаты, так как они заведомо не попадают в диапазон курсора */
        step_op = MDBX_NEXT_NODUP;
      // fall through
      case MDBX_NEXT_NODUP:
        goto next;
      }
      goto eof;
    }

    if (cursor->range_to_key.mdbx.iov_base &&
        mdbx_cmp(cursor->txn->mdbx_txn, cursor->idx_handle, &cursor->current,
                 &cursor->range_to_key.mdbx) >= 0) {
      /* задана верхняя граница диапазона и текущий ключ больше её */
      switch (step_op) {
      default:
        assert(false);
      case MDBX_PREV_DUP:
      case MDBX_NEXT_DUP:
        /* нет смысла идти по дубликатам (без изменения значения ключа) */
        break;
      case MDBX_PREV:
        /* при движении в сторону уменьшения ключа логично пропустить все
         * дубликаты, так как они заведомо не попадают в диапазон курсора */
        step_op = MDBX_PREV_NODUP;
      // fall through
      case MDBX_PREV_NODUP:
        goto next;
      case MDBX_NEXT:
        step_op = MDBX_NEXT_NODUP;
      // fall through
      case MDBX_NEXT_NODUP:
        /* идти в сторону увелияения ключа есть смысл только в случае
         * unordered (хэшированного) индекса, при этом логично пропустить
         * все дубликаты, так как они заведомо не попадают в диапазон курсора */
        if (!fpta_index_is_ordered(cursor->index_shove()))
          goto next;
        break;
      }
      goto eof;
    }

    if (!cursor->filter)
      return FPTA_SUCCESS;

    if (fpta_index_is_secondary(cursor->index_shove())) {
      MDBX_val pk_key = mdbx_data.sys;
      mdbx_data.sys.iov_base = nullptr;
      mdbx_data.sys.iov_len = 0;
      rc = mdbx_get(cursor->txn->mdbx_txn, cursor->tbl_handle, &pk_key,
                    &mdbx_data.sys);
      if (unlikely(rc != MDBX_SUCCESS))
        return (rc != MDBX_NOTFOUND) ? rc : (int)FPTA_INDEX_CORRUPTED;
    }

    if (fpta_filter_match(cursor->filter, mdbx_data))
      return FPTA_SUCCESS;

  next:
    rc = mdbx_cursor_get(cursor->mdbx_cursor, &cursor->current, &mdbx_data.sys,
                         step_op);
  }

  if (unlikely(rc != MDBX_NOTFOUND)) {
    cursor->set_poor();
    return rc;
  }

eof:
  switch (mdbx_seek_op) {
  default:
    cursor->set_poor();
    return FPTA_NODATA;

  case MDBX_NEXT:
  case MDBX_NEXT_NODUP:
    cursor->set_eof(fpta_cursor::after_last);
    return FPTA_NODATA;

  case MDBX_PREV:
  case MDBX_PREV_NODUP:
    cursor->set_eof(fpta_cursor::before_first);
    return FPTA_NODATA;

  case MDBX_PREV_DUP:
  case MDBX_NEXT_DUP:
    return FPTA_NODATA;
  }
}

int fpta_cursor_move(fpta_cursor *cursor, fpta_seek_operations op) {
  int rc = fpta_cursor_validate(cursor, fpta_read);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (unlikely(op < fpta_first || op > fpta_key_prev)) {
    cursor->set_poor();
    return FPTA_EFLAG;
  }

  if (fpta_cursor_is_descending(cursor->options))
    op = (fpta_seek_operations)(op ^ 1);

  MDBX_val *mdbx_seek_key = nullptr;
  MDBX_cursor_op mdbx_seek_op, mdbx_step_op;
  switch (op) {
  default:
    assert(false && "unexpected seek-op");
    cursor->set_poor();
    return FPTA_EOOPS;

  case fpta_first:
    if (cursor->range_from_key.mdbx.iov_base == nullptr ||
        !fpta_index_is_ordered(cursor->index_shove())) {
      mdbx_seek_op = MDBX_FIRST;
    } else {
      mdbx_seek_key = &cursor->range_from_key.mdbx;
      mdbx_seek_op = MDBX_SET_RANGE;
    }
    mdbx_step_op = MDBX_NEXT;
    break;

  case fpta_last:
    if (cursor->range_to_key.mdbx.iov_base == nullptr ||
        !fpta_index_is_ordered(cursor->index_shove())) {
      mdbx_seek_op = MDBX_LAST;
    } else {
      mdbx_seek_key = &cursor->range_to_key.mdbx;
      mdbx_seek_op = MDBX_SET_RANGE;
    }
    mdbx_step_op = MDBX_PREV;
    break;

  case fpta_next:
    if (unlikely(cursor->is_poor()))
      return FPTA_ECURSOR;
    mdbx_seek_op = unlikely(cursor->is_before_first()) ? MDBX_FIRST : MDBX_NEXT;
    mdbx_step_op = MDBX_NEXT;
    break;
  case fpta_prev:
    if (unlikely(cursor->is_poor()))
      return FPTA_ECURSOR;
    mdbx_seek_op = unlikely(cursor->is_after_last()) ? MDBX_LAST : MDBX_PREV;
    mdbx_step_op = MDBX_PREV;
    break;

  /* Перемещение по дубликатам значения ключа, в случае если
   * соответствующий индекс был БЕЗ флага fpta_index_uniq */
  case fpta_dup_first:
    if (unlikely(!cursor->is_filled()))
      return cursor->unladed_state();
    if (unlikely(fpta_index_is_unique(cursor->index_shove())))
      return FPTA_SUCCESS;
    mdbx_seek_op = MDBX_FIRST_DUP;
    mdbx_step_op = MDBX_NEXT_DUP;
    break;

  case fpta_dup_last:
    if (unlikely(!cursor->is_filled()))
      return cursor->unladed_state();
    if (unlikely(fpta_index_is_unique(cursor->index_shove())))
      return FPTA_SUCCESS;
    mdbx_seek_op = MDBX_LAST_DUP;
    mdbx_step_op = MDBX_PREV_DUP;
    break;

  case fpta_dup_next:
    if (unlikely(!cursor->is_filled()))
      return cursor->unladed_state();
    if (unlikely(fpta_index_is_unique(cursor->index_shove())))
      return FPTA_NODATA;
    mdbx_seek_op = MDBX_NEXT_DUP;
    mdbx_step_op = MDBX_NEXT_DUP;
    break;

  case fpta_dup_prev:
    if (unlikely(!cursor->is_filled()))
      return cursor->unladed_state();
    if (unlikely(fpta_index_is_unique(cursor->index_shove())))
      return FPTA_NODATA;
    mdbx_seek_op = MDBX_PREV_DUP;
    mdbx_step_op = MDBX_PREV_DUP;
    break;

  case fpta_key_next:
    if (unlikely(cursor->is_poor()))
      return FPTA_ECURSOR;
    mdbx_seek_op =
        unlikely(cursor->is_before_first()) ? MDBX_FIRST : MDBX_NEXT_NODUP;
    mdbx_step_op = MDBX_NEXT_NODUP;
    break;

  case fpta_key_prev:
    if (unlikely(cursor->is_poor()))
      return FPTA_ECURSOR;
    mdbx_seek_op =
        unlikely(cursor->is_after_last()) ? MDBX_LAST : MDBX_PREV_NODUP;
    mdbx_step_op = MDBX_PREV_NODUP;
    break;
  }

  return fpta_cursor_seek(cursor, mdbx_seek_op, mdbx_step_op, mdbx_seek_key,
                          nullptr);
}

int fpta_cursor_locate(fpta_cursor *cursor, bool exactly, const fpta_value *key,
                       const fptu_ro *row) {
  int rc = fpta_cursor_validate(cursor, fpta_read);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (unlikely((key && row) || (!key && !row))) {
    /* Должен быть выбран один из режимов поиска. */
    cursor->set_poor();
    return FPTA_EINVAL;
  }

  if (!fpta_cursor_is_ordered(cursor->options)) {
    if (FPTA_PROHIBIT_NEARBY4UNORDERED && !exactly) {
      /* Отвергаем неточный поиск для неупорядоченного курсора (и индекса). */
      cursor->set_poor();
      return FPTA_EFLAG;
    }
    /* Принудительно включаем точный поиск для курсора без сортировки. */
    exactly = true;
  }

  /* устанавливаем базовый режим поиска */
  MDBX_cursor_op mdbx_seek_op = exactly ? MDBX_SET_KEY : MDBX_SET_RANGE;
  const MDBX_val *mdbx_seek_data = nullptr;

  fpta_key seek_key, pk_key;
  if (key) {
    /* Поиск по значению проиндексированной колонки, конвертируем его в ключ
     * для поиска по индексу. Дополнительных данных для поиска нет. */
    rc = fpta_index_value2key(cursor->index_shove(), *key, seek_key, false);
    if (unlikely(rc != FPTA_SUCCESS)) {
      cursor->set_poor();
      return rc;
    }
    /* базовый режим поиска уже выставлен. */
  } else {
    /* Поиск по "образу" строки, получаем из строки-кортежа значение
     * проиндексированной колонки в формате ключа для поиска по индексу. */
    rc = fpta_index_row2key(cursor->table_schema(), cursor->column_number, *row,
                            seek_key, false);
    if (unlikely(rc != FPTA_SUCCESS)) {
      cursor->set_poor();
      return rc;
    }

    if (fpta_index_is_secondary(cursor->index_shove())) {
      /* Курсор связан со вторичным индексом. Для уточнения поиска можем
       * использовать только значение PK. */
      if (fpta_index_is_unique(cursor->index_shove())) {
        /* Не используем PK если вторичный индекс обеспечивает уникальность.
         * Базовый режим поиска уже был выставлен. */
      } else {
        /* Извлекаем и используем значение PK только если связанный с
         * курсором индекс допускает дубликаты. */
        rc = fpta_index_row2key(cursor->table_schema(), 0, *row, pk_key, false);
        if (rc == FPTA_SUCCESS) {
          /* Используем уточняющее значение PK только если в строке-образце
           * есть соответствующая колонка. При этом игнорируем отсутствие
           * колонки (ошибку FPTA_COLUMN_MISSING). */
          mdbx_seek_data = &pk_key.mdbx;
          mdbx_seek_op = exactly ? MDBX_GET_BOTH : MDBX_GET_BOTH_RANGE;
        } else if (rc != FPTA_COLUMN_MISSING) {
          cursor->set_poor();
          return rc;
        } else {
          /* в строке нет колонки с PK, базовый режим поиска уже выставлен. */
        }
      }
    } else {
      /* Курсор связан с первичным индексом. Для уточнения поиска можем
       * использовать только данные (значение) всей строки. Однако,
       * делаем это ТОЛЬКО при неточном поиске по индексу с дубликатами,
       * так как только в этом случае это выглядит рациональным:
       *  - При точном поиске, отличие в значении любой колонки, включая
       *    её отсутствие, даст отрицательный результат.
       *    Соответственно, это породит кардинальные отличия от поведения
       *    в других лучаях. Например, когда используется вторичный индекс.
       *  - Фактически будет выполнятется не позиционирование курсора, а
       *    некая комплекстная операция "найти заданную строку таблицы",
       *    полезность которой сомнительна. */
      if (!exactly && !fpta_index_is_unique(cursor->index_shove())) {
        /* базовый режим поиска уже был выставлен, переключаем только
         * для нечеткого поиска среди дубликатов (как описано выше). */
        mdbx_seek_data = &row->sys;
        mdbx_seek_op = MDBX_GET_BOTH_RANGE;
      }
    }
  }

  rc = fpta_cursor_seek(cursor, mdbx_seek_op,
                        fpta_cursor_is_descending(cursor->options) ? MDBX_PREV
                                                                   : MDBX_NEXT,
                        &seek_key.mdbx, mdbx_seek_data);
  if (unlikely(rc != FPTA_SUCCESS)) {
    cursor->set_poor();
    return rc;
  }

  if (!fpta_cursor_is_descending(cursor->options))
    return FPTA_SUCCESS;

  /* Корректируем позицию при обратном порядке строк (fpta_descending) */
  while (!exactly) {
    /* При неточном поиске для курсора с обратной сортировкой нужно перейти
     * на другую сторону от lower_bound, т.е. идти в обратном порядке
     * до значения меньшего или равного целевому (с учетом фильтра). */
    int cmp = mdbx_cmp(cursor->txn->mdbx_txn, cursor->idx_handle,
                       &cursor->current, &seek_key.mdbx);

    if (cmp < 0)
      return FPTA_SUCCESS;

    if (cmp == 0) {
      if (!mdbx_seek_data) {
        /* Поиск без уточнения по дубликатам. Если индекс допускает
         * дубликаты, то следует перейти к последнему, что будет
         * сделао ниже. */
        break;
      }

      /* Неточный поиск с уточнением по дубликатам. Переход на другую
       * сторону lower_bound следует делать с учетом сравнения данных. */
      MDBX_val mdbx_data;
      rc = mdbx_cursor_get(cursor->mdbx_cursor, &cursor->current, &mdbx_data,
                           MDBX_GET_CURRENT);
      if (unlikely(rc != FPTA_SUCCESS)) {
        cursor->set_poor();
        return rc;
      }

      cmp = mdbx_dcmp(cursor->txn->mdbx_txn, cursor->idx_handle, &mdbx_data,
                      mdbx_seek_data);
      if (cmp <= 0)
        return FPTA_SUCCESS;
    }

    rc = fpta_cursor_seek(cursor, MDBX_PREV, MDBX_PREV, nullptr, nullptr);
    if (unlikely(rc != FPTA_SUCCESS)) {
      cursor->set_poor();
      return rc;
    }
  }

  /* Для индекса с дубликатами нужно перейти к последней позиции с текущим
   * ключом. */
  if (!fpta_index_is_unique(cursor->index_shove())) {
    size_t dups;
    if (unlikely(mdbx_cursor_count(cursor->mdbx_cursor, &dups) !=
                 MDBX_SUCCESS)) {
      cursor->set_poor();
      return FPTA_EOOPS;
    }

    if (dups > 1) {
      /* Переходим к последнему дубликату (последнему мульти-значению
       * для одного значения ключа), а если значение не подходит под
       * фильтр, то двигаемся в обратном порядке дальше. */
      rc = fpta_cursor_seek(cursor, MDBX_LAST_DUP, MDBX_PREV, nullptr, nullptr);
      if (unlikely(rc != FPTA_SUCCESS)) {
        cursor->set_poor();
        return rc;
      }
    }
  }

  return FPTA_SUCCESS;
}

//----------------------------------------------------------------------------

int fpta_cursor_eof(const fpta_cursor *cursor) {
  int rc = fpta_cursor_validate(cursor, fpta_read);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (likely(cursor->is_filled()))
    return FPTA_SUCCESS;

  return FPTA_NODATA;
}

int fpta_cursor_state(const fpta_cursor *cursor) {
  int rc = fpta_cursor_validate(cursor, fpta_read);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (likely(cursor->is_filled()))
    return FPTA_SUCCESS;

  return cursor->unladed_state();
}

int fpta_cursor_count(fpta_cursor *cursor, size_t *pcount, size_t limit) {
  if (unlikely(!pcount))
    return FPTA_EINVAL;
  *pcount = (size_t)FPTA_DEADBEEF;

  size_t count = 0;
  int rc = fpta_cursor_move(cursor, fpta_first);
  while (rc == FPTA_SUCCESS && count < limit) {
    ++count;
    rc = fpta_cursor_move(cursor, fpta_next);
  }

  if (rc == FPTA_NODATA) {
    *pcount = count;
    rc = FPTA_SUCCESS;
  }

  cursor->set_poor();
  return rc;
}

int fpta_cursor_dups(fpta_cursor *cursor, size_t *pdups) {
  if (unlikely(pdups == nullptr))
    return FPTA_EINVAL;
  *pdups = (size_t)FPTA_DEADBEEF;

  int rc = fpta_cursor_validate(cursor, fpta_read);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (unlikely(!cursor->is_filled())) {
    if (cursor->is_poor())
      return FPTA_ECURSOR;
    *pdups = 0;
    return FPTA_NODATA;
  }

  *pdups = 0;
  rc = mdbx_cursor_count(cursor->mdbx_cursor, pdups);
  return (rc == MDBX_NOTFOUND) ? (int)FPTA_NODATA : rc;
}

//----------------------------------------------------------------------------

int fpta_cursor_get(fpta_cursor *cursor, fptu_ro *row) {
  if (unlikely(row == nullptr))
    return FPTA_EINVAL;

  row->total_bytes = 0;
  row->units = nullptr;

  int rc = fpta_cursor_validate(cursor, fpta_read);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (unlikely(!cursor->is_filled()))
    return cursor->unladed_state();

  if (fpta_index_is_primary(cursor->index_shove()))
    return mdbx_cursor_get(cursor->mdbx_cursor, &cursor->current, &row->sys,
                           MDBX_GET_CURRENT);

  MDBX_val pk_key;
  rc = mdbx_cursor_get(cursor->mdbx_cursor, &cursor->current, &pk_key,
                       MDBX_GET_CURRENT);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  rc = mdbx_get(cursor->txn->mdbx_txn, cursor->tbl_handle, &pk_key, &row->sys);
  return (rc != MDBX_NOTFOUND) ? rc : (int)FPTA_INDEX_CORRUPTED;
}

int fpta_cursor_key(fpta_cursor *cursor, fpta_value *key) {
  if (unlikely(key == nullptr))
    return FPTA_EINVAL;
  int rc = fpta_cursor_validate(cursor, fpta_read);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (unlikely(!cursor->is_filled()))
    return cursor->unladed_state();

  rc = fpta_index_key2value(cursor->index_shove(), cursor->current, *key);
  return rc;
}

int fpta_cursor_delete(fpta_cursor *cursor) {
  int rc = fpta_cursor_validate(cursor, fpta_write);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (unlikely(!cursor->is_filled()))
    return cursor->unladed_state();

  if (!cursor->table_schema()->has_secondary()) {
    rc = mdbx_cursor_del(cursor->mdbx_cursor, 0);
    if (unlikely(rc != FPTA_SUCCESS)) {
      cursor->set_poor();
      return rc;
    }
  } else {
    MDBX_val pk_key;
    if (fpta_index_is_primary(cursor->index_shove())) {
      pk_key = cursor->current;
      if (pk_key.iov_len > 0 &&
          /* LY: FIXME тут можно убрать вызов mdbx_is_dirty() и просто
           * всегда копировать ключ, так как это скорее всего дешевле. */
          mdbx_is_dirty(cursor->txn->mdbx_txn, pk_key.iov_base) !=
              MDBX_RESULT_FALSE) {
        void *buffer = alloca(pk_key.iov_len);
        pk_key.iov_base = memcpy(buffer, pk_key.iov_base, pk_key.iov_len);
      }
    } else {
      rc = mdbx_cursor_get(cursor->mdbx_cursor, &cursor->current, &pk_key,
                           MDBX_GET_CURRENT);
      if (unlikely(rc != MDBX_SUCCESS)) {
        cursor->set_poor();
        return (rc != MDBX_NOTFOUND) ? rc : (int)FPTA_INDEX_CORRUPTED;
      }
    }

    fptu_ro old;
#if defined(NDEBUG) && __cplusplus >= 201103L
    const constexpr size_t likely_enough = 64u * 42u;
#else
    const size_t likely_enough = (time(nullptr) & 1) ? 11u : 64u * 42u;
#endif /* NDEBUG */
    void *buffer = alloca(likely_enough);
    old.sys.iov_base = buffer;
    old.sys.iov_len = likely_enough;

    rc = mdbx_replace(cursor->txn->mdbx_txn, cursor->tbl_handle, &pk_key,
                      nullptr, &old.sys, MDBX_CURRENT);
    if (unlikely(rc == MDBX_RESULT_TRUE)) {
      assert(old.sys.iov_base == nullptr && old.sys.iov_len > likely_enough);
      old.sys.iov_base = alloca(old.sys.iov_len);
      rc = mdbx_replace(cursor->txn->mdbx_txn, cursor->tbl_handle, &pk_key,
                        nullptr, &old.sys, MDBX_CURRENT);
    }
    if (unlikely(rc != MDBX_SUCCESS)) {
      cursor->set_poor();
      return rc;
    }

    rc = fpta_secondary_remove(cursor->txn, cursor->table_schema(), pk_key, old,
                               cursor->column_number);
    if (unlikely(rc != MDBX_SUCCESS)) {
      cursor->set_poor();
      return fpta_internal_abort(cursor->txn, rc);
    }

    if (!fpta_index_is_primary(cursor->index_shove())) {
      rc = mdbx_cursor_del(cursor->mdbx_cursor, 0);
      if (unlikely(rc != MDBX_SUCCESS)) {
        cursor->set_poor();
        return fpta_internal_abort(cursor->txn, rc);
      }
    }
  }

  if (fpta_cursor_is_descending(cursor->options)) {
    /* Для курсора с обратным порядком строк требуется перейти к предыдущей
     * строке, в том числе подходящей под условие фильтрации. */
    fpta_cursor_seek(cursor, MDBX_PREV, MDBX_PREV, nullptr, nullptr);
  } else if (mdbx_cursor_eof(cursor->mdbx_cursor) == MDBX_RESULT_TRUE) {
    cursor->set_eof(fpta_cursor::after_last);
  } else {
    /* Для курсора с прямым порядком строк требуется перейти
     * к следующей строке подходящей под условие фильтрации, но
     * не выполнять переход если текущая строка уже подходит под фильтр. */
    fpta_cursor_seek(cursor, MDBX_GET_CURRENT, MDBX_NEXT, nullptr, nullptr);
  }

  return FPTA_SUCCESS;
}

//----------------------------------------------------------------------------

int fpta_cursor_validate_update(fpta_cursor *cursor, fptu_ro new_row_value) {
  int rc = fpta_cursor_validate(cursor, fpta_write);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (unlikely(!cursor->is_filled()))
    return cursor->unladed_state();

  fpta_key column_key;
  rc = fpta_index_row2key(cursor->table_schema(), cursor->column_number,
                          new_row_value, column_key, false);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (!fpta_is_same(cursor->current, column_key.mdbx))
    return FPTA_KEY_MISMATCH;

  if (!cursor->table_schema()->has_secondary())
    return FPTA_SUCCESS;

  fptu_ro present_row;
  if (fpta_index_is_primary(cursor->index_shove())) {
    rc = mdbx_cursor_get(cursor->mdbx_cursor, &cursor->current,
                         &present_row.sys, MDBX_GET_CURRENT);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;

    return fpta_secondary_check(cursor->txn, cursor->table_schema(),
                                present_row, new_row_value, 0);
  }

  MDBX_val present_pk_key;
  rc = mdbx_cursor_get(cursor->mdbx_cursor, &cursor->current, &present_pk_key,
                       MDBX_GET_CURRENT);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  fpta_key new_pk_key;
  rc = fpta_index_row2key(cursor->table_schema(), 0, new_row_value, new_pk_key,
                          false);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  rc = mdbx_get(cursor->txn->mdbx_txn, cursor->tbl_handle, &present_pk_key,
                &present_row.sys);
  if (unlikely(rc != MDBX_SUCCESS))
    return (rc != MDBX_NOTFOUND) ? rc : (int)FPTA_INDEX_CORRUPTED;

  return fpta_secondary_check(cursor->txn, cursor->table_schema(), present_row,
                              new_row_value, cursor->column_number);
}

int fpta_cursor_update(fpta_cursor *cursor, fptu_ro new_row_value) {
  int rc = fpta_cursor_validate(cursor, fpta_write);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (unlikely(!cursor->is_filled()))
    return cursor->unladed_state();

  const fpta_table_schema *table_def = cursor->table_schema();
  rc = fpta_check_notindexed_cols(table_def, new_row_value);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  fpta_key column_key;
  rc = fpta_index_row2key(table_def, cursor->column_number, new_row_value,
                          column_key, false);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (!fpta_is_same(cursor->current, column_key.mdbx))
    return FPTA_KEY_MISMATCH;

  if (!table_def->has_secondary()) {
    rc = mdbx_cursor_put(cursor->mdbx_cursor, &column_key.mdbx,
                         &new_row_value.sys, MDBX_CURRENT | MDBX_NODUPDATA);
    if (likely(rc == MDBX_SUCCESS) &&
        /* актуализируем текущий ключ, если он был в грязной странице, то при
         * изменении мог быть перемещен с перезаписью старого значения */
        mdbx_is_dirty(cursor->txn->mdbx_txn, cursor->current.iov_base)) {
      rc = mdbx_cursor_get(cursor->mdbx_cursor, &cursor->current, nullptr,
                           MDBX_GET_CURRENT);
    }
    if (unlikely(rc != MDBX_SUCCESS))
      cursor->set_poor();
    return rc;
  }

  MDBX_val old_pk_key;
  if (fpta_index_is_primary(cursor->index_shove())) {
    old_pk_key = cursor->current;
  } else {
    rc = mdbx_cursor_get(cursor->mdbx_cursor, &cursor->current, &old_pk_key,
                         MDBX_GET_CURRENT);
    if (unlikely(rc != MDBX_SUCCESS)) {
      cursor->set_poor();
      return (rc != MDBX_NOTFOUND) ? rc : (int)FPTA_INDEX_CORRUPTED;
    }
  }

  /* Здесь не очевидный момент при обновлении с изменением PK:
   *  - для обновления secondary индексов требуется как старое,
   *    так и новое значения строки, а также оба значения PK.
   *  - подготовленный old_pk_key содержит указатель на значение,
   *    которое физически размещается в value в служебной таблице
   *    secondary индекса, по которому открыт курсор.
   *  - если сначала, вызовом fpta_secondary_upsert(), обновить
   *    вспомогательные таблицы для secondary индексов, то указатель
   *    внутри old_pk_key может стать невалидным, т.е. так мы потеряем
   *    предыдущее значение PK.
   *  - если же сначала просто обновить строку в основной таблице,
   *    то будет утрачено её предыдущее значение, которое требуется
   *    для обновления secondary индексов.
   *
   * Поэтому, чтобы не потерять старое значение PK и одновременно избежать
   * лишних копирований, здесь используется mdbx_get_ex(). В свою очередь
   * mdbx_get_ex() использует MDBX_SET_KEY для получения как данных, так и
   * данных ключа. */

  fptu_ro old;
  rc = mdbx_get_ex(cursor->txn->mdbx_txn, cursor->tbl_handle, &old_pk_key,
                   &old.sys, nullptr);
  if (unlikely(rc != MDBX_SUCCESS)) {
    cursor->set_poor();
    return (rc != MDBX_NOTFOUND) ? rc : (int)FPTA_INDEX_CORRUPTED;
  }

  fpta_key new_pk_key;
  rc = fpta_index_row2key(cursor->table_schema(), 0, new_row_value, new_pk_key,
                          false);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

#if 0 /* LY: в данный момент нет необходимости */
  if (old_pk_key.iov_len > 0 &&
      mdbx_is_dirty(cursor->txn->mdbx_txn, old_pk_key.iov_base) !=
          MDBX_RESULT_FALSE) {
    void *buffer = alloca(old_pk_key.iov_len);
    old_pk_key.iov_base =
        memcpy(buffer, old_pk_key.iov_base, old_pk_key.iov_len);
  }
#endif

  rc = fpta_secondary_upsert(cursor->txn, cursor->table_schema(), old_pk_key,
                             old, new_pk_key.mdbx, new_row_value,
                             cursor->column_number);
  if (unlikely(rc != MDBX_SUCCESS)) {
    cursor->set_poor();
    return fpta_internal_abort(cursor->txn, rc);
  }

  const bool pk_changed = !fpta_is_same(old_pk_key, new_pk_key.mdbx);
  if (pk_changed) {
    rc = mdbx_del(cursor->txn->mdbx_txn, cursor->tbl_handle, &old_pk_key,
                  nullptr);
    if (unlikely(rc != MDBX_SUCCESS)) {
      cursor->set_poor();
      return fpta_internal_abort(cursor->txn, rc);
    }

    rc = mdbx_put(cursor->txn->mdbx_txn, cursor->tbl_handle, &new_pk_key.mdbx,
                  &new_row_value.sys, MDBX_NODUPDATA | MDBX_NOOVERWRITE);
    if (unlikely(rc != MDBX_SUCCESS)) {
      cursor->set_poor();
      return fpta_internal_abort(cursor->txn, rc);
    }

    rc = mdbx_cursor_put(cursor->mdbx_cursor, &column_key.mdbx,
                         &new_pk_key.mdbx, MDBX_CURRENT | MDBX_NODUPDATA);

  } else {
    rc = mdbx_put(cursor->txn->mdbx_txn, cursor->tbl_handle, &new_pk_key.mdbx,
                  &new_row_value.sys, MDBX_CURRENT | MDBX_NODUPDATA);
  }

  if (likely(rc == MDBX_SUCCESS) &&
      /* актуализируем текущий ключ, если он был в грязной странице, то при
       * изменении мог быть перемещен с перезаписью старого значения */
      mdbx_is_dirty(cursor->txn->mdbx_txn, cursor->current.iov_base)) {
    rc = mdbx_cursor_get(cursor->mdbx_cursor, &cursor->current, nullptr,
                         MDBX_GET_CURRENT);
  }
  if (unlikely(rc != MDBX_SUCCESS)) {
    cursor->set_poor();
    return fpta_internal_abort(cursor->txn, rc);
  }

  return FPTA_SUCCESS;
}

//----------------------------------------------------------------------------

int fpta_apply_visitor(
    fpta_txn *txn, fpta_name *column_id, fpta_value range_from,
    fpta_value range_to, fpta_filter *filter, fpta_cursor_options op,
    size_t skip, size_t limit, fpta_value *page_top, fpta_value *page_bottom,
    size_t *count, int (*visitor)(const fptu_ro *row, void *context, void *arg),
    void *visitor_context, void *visitor_arg) {

  if (unlikely(limit < 1 || !visitor))
    return FPTA_EINVAL;

  fpta_cursor *cursor = nullptr /* TODO: заменить на объект на стеке */;
  int rc =
      fpta_cursor_open(txn, column_id, range_from, range_to, filter,
                       (fpta_cursor_options)(op & ~fpta_dont_fetch), &cursor);

  for (; skip > 0 && likely(rc == FPTA_SUCCESS); --skip)
    rc = fpta_cursor_move(cursor, fpta_next);

  if (page_top) {
    if (rc == FPTA_SUCCESS) {
      int err = fpta_index_key2value(cursor->index_shove(), cursor->current,
                                     *page_top);
      assert(err == FPTA_SUCCESS);
      if (unlikely(err != FPTA_SUCCESS))
        rc = err;
    } else {
      *page_top = (rc == FPTA_NODATA) ? fpta_value_begin() : fpta_value_null();
    }
  }

  size_t n;
  for (n = 0; likely(rc == FPTA_SUCCESS) && n < limit; n++) {
    fptu_ro row;
    rc = fpta_cursor_get(cursor, &row);
    if (unlikely(rc != FPTA_SUCCESS))
      break;
    rc = visitor(&row, visitor_context, visitor_arg);
    if (unlikely(rc != FPTA_SUCCESS))
      break;
    rc = fpta_cursor_move(cursor, fpta_next);
  }

  if (count)
    *count = n;

  if (page_bottom) {
    if (cursor && cursor->is_filled()) {
      int err = fpta_index_key2value(cursor->index_shove(), cursor->current,
                                     *page_bottom);
      assert(err == FPTA_SUCCESS);
      if (unlikely(err != FPTA_SUCCESS))
        rc = err;
    } else {
      *page_bottom = (rc == FPTA_NODATA) ? fpta_value_end() : fpta_value_null();
    }
  }

  if (cursor) {
    int err = fpta_cursor_close(cursor);
    assert(err == FPTA_SUCCESS);
    if (unlikely(err != FPTA_SUCCESS))
      rc = err;
  }
  return rc;
}

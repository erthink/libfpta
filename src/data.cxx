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

//----------------------------------------------------------------------------

fpta_value fpta_field2value(const fptu_field *field) {
  fpta_value result = {fpta_null, 0, {0}};

  if (unlikely(!field))
    return result;

  const fptu_payload *payload = fptu_field_payload(field);
  switch (fptu_get_type(field->ct)) {
  default:
  case fptu_nested:
    result.binary_length = (unsigned)units2bytes(payload->other.varlen.brutto);
    result.binary_data = (void *)payload->other.data;
    result.type = fpta_binary;
    break;

  case fptu_opaque:
    result.binary_length = payload->other.varlen.opaque_bytes;
    result.binary_data = (void *)payload->other.data;
    result.type = fpta_binary;
    break;

  case fptu_null:
    break;

  case fptu_uint16:
    result.type = fpta_unsigned_int;
    result.uint = field->get_payload_uint16();
    break;

  case fptu_int32:
    result.type = fpta_signed_int;
    result.sint = payload->i32;
    break;

  case fptu_uint32:
    result.type = fpta_unsigned_int;
    result.uint = payload->u32;
    break;

  case fptu_fp32:
    result.type = fpta_float_point;
    result.fp = payload->fp32;
    break;

  case fptu_int64:
    result.type = fpta_signed_int;
    result.sint = payload->i64;
    break;

  case fptu_uint64:
    result.type = fpta_unsigned_int;
    result.uint = payload->u64;
    break;

  case fptu_fp64:
    result.type = fpta_float_point;
    result.fp = payload->fp64;
    break;

  case fptu_datetime:
    result.type = fpta_datetime;
    result.datetime.fixedpoint = payload->u64;
    break;

  case fptu_96:
    result.type = fpta_binary;
    result.binary_length = 96 / 8;
    result.binary_data = (void *)payload->fixbin;
    break;

  case fptu_128:
    result.type = fpta_binary;
    result.binary_length = 128 / 8;
    result.binary_data = (void *)payload->fixbin;
    break;

  case fptu_160:
    result.type = fpta_binary;
    result.binary_length = 160 / 8;
    result.binary_data = (void *)payload->fixbin;
    break;

  case fptu_256:
    result.type = fpta_binary;
    result.binary_length = 256 / 8;
    result.binary_data = (void *)payload->fixbin;
    break;

  case fptu_cstr:
    result.type = fpta_string;
    result.str = payload->cstr;
    result.binary_length = (unsigned)strlen(result.str);
    break;
  }
  return result;
}

int fpta_get_column(fptu_ro row, const fpta_name *column_id,
                    fpta_value *value) {
  if (unlikely(column_id == nullptr || value == nullptr))
    return FPTA_EINVAL;

  const fptu_field *field = fptu_lookup_ro(row, (unsigned)column_id->column.num,
                                           fpta_name_coltype(column_id));
  *value = fpta_field2value(field);
  return field ? FPTA_SUCCESS : FPTA_NODATA;
}

int fpta_upsert_column(fptu_rw *pt, const fpta_name *column_id,
                       fpta_value value) {
  if (unlikely(!pt || !fpta_id_validate(column_id, fpta_column)))
    return FPTA_EINVAL;

  fptu_type coltype = fpta_shove2type(column_id->shove);
  assert(column_id->column.num <= fptu_max_cols);
  unsigned col = (unsigned)column_id->column.num;

  switch (coltype) {
  default:
    /* TODO: проверить корректность размера для fptu_farray */
    if (unlikely(value.type != fpta_binary))
      return FPTA_ETYPE;
    return FPTA_ENOIMP;

  case fptu_nested: {
    fptu_ro tuple;
    if (unlikely(value.type != fpta_binary))
      return FPTA_ETYPE;
    tuple.sys.iov_len = value.binary_length;
    tuple.sys.iov_base = value.binary_data;
    return fptu_upsert_nested(pt, col, tuple);
  }

  case fptu_opaque:
    if (unlikely(value.type != fpta_binary))
      return FPTA_ETYPE;
    return fptu_upsert_opaque(pt, col, value.binary_data, value.binary_length);

  case fptu_null:
    return FPTA_EINVAL;

  case fptu_uint16:
    switch (value.type) {
    default:
      return FPTA_ETYPE;
    case fpta_signed_int:
      if (unlikely(value.sint < 0))
        return FPTA_EVALUE;
    case fpta_unsigned_int:
      if (unlikely(value.uint > UINT16_MAX))
        return FPTA_EVALUE;
    }
    return fptu_upsert_uint16(pt, col, (uint16_t)value.uint);

  case fptu_int32:
    switch (value.type) {
    default:
      return FPTA_ETYPE;
    case fpta_unsigned_int:
      if (unlikely(value.uint > INT32_MAX))
        return FPTA_EVALUE;
    case fpta_signed_int:
      if (unlikely(value.sint != (int32_t)value.sint))
        return FPTA_EVALUE;
    }
    return fptu_upsert_int32(pt, col, (int32_t)value.sint);

  case fptu_uint32:
    switch (value.type) {
    default:
      return FPTA_ETYPE;
    case fpta_signed_int:
      if (unlikely(value.sint < 0))
        return FPTA_EVALUE;
    case fpta_unsigned_int:
      if (unlikely(value.uint > UINT32_MAX))
        return FPTA_EVALUE;
    }
    return fptu_upsert_uint32(pt, col, (uint32_t)value.uint);

  case fptu_fp32:
    if (unlikely(value.type != fpta_float_point))
      return FPTA_ETYPE;
    if (unlikely(std::isnan(value.fp)))
      return FPTA_EVALUE;
    if (unlikely(fabs(value.fp) > FLT_MAX) && !std::isinf(value.fp))
      return FPTA_EVALUE;
    return fptu_upsert_fp32(pt, col, (float)value.fp);

  case fptu_int64:
    switch (value.type) {
    default:
      return FPTA_ETYPE;
    case fpta_unsigned_int:
      if (unlikely(value.uint > INT64_MAX))
        return FPTA_EVALUE;
    case fpta_signed_int:
      break;
    }
    return fptu_upsert_int64(pt, col, value.sint);

  case fptu_uint64:
    switch (value.type) {
    default:
      return FPTA_ETYPE;
    case fpta_signed_int:
      if (unlikely(value.sint < 0))
        return FPTA_EVALUE;
    case fpta_unsigned_int:
      break;
    }
    return fptu_upsert_uint64(pt, col, value.uint);

  case fptu_fp64:
    if (unlikely(value.type != fpta_float_point))
      return FPTA_ETYPE;
    if (unlikely(std::isnan(value.fp)))
      return FPTA_EVALUE;
    return fptu_upsert_fp64(pt, col, value.fp);

  case fptu_datetime:
    if (value.type != fpta_datetime)
      return FPTA_ETYPE;
    return fptu_upsert_datetime(pt, col, value.datetime);

  case fptu_96:
    if (unlikely(value.type != fpta_binary))
      return FPTA_ETYPE;
    if (unlikely(value.binary_length != 96 / 8))
      return FPTA_DATALEN_MISMATCH;
    if (unlikely(!value.binary_data))
      return FPTA_EINVAL;
    return fptu_upsert_96(pt, col, value.binary_data);

  case fptu_128:
    if (unlikely(value.type != fpta_binary))
      return FPTA_ETYPE;
    if (unlikely(value.binary_length != 128 / 8))
      return FPTA_DATALEN_MISMATCH;
    if (unlikely(!value.binary_data))
      return FPTA_EINVAL;
    return fptu_upsert_128(pt, col, value.binary_data);

  case fptu_160:
    if (unlikely(value.type != fpta_binary))
      return FPTA_ETYPE;
    if (unlikely(value.binary_length != 160 / 8))
      return FPTA_DATALEN_MISMATCH;
    if (unlikely(!value.binary_data))
      return FPTA_EINVAL;
    return fptu_upsert_160(pt, col, value.binary_data);

  case fptu_256:
    if (unlikely(value.type != fpta_binary))
      return FPTA_ETYPE;
    if (unlikely(value.binary_length != 256 / 8))
      return FPTA_DATALEN_MISMATCH;
    if (unlikely(!value.binary_data))
      return FPTA_EINVAL;
    return fptu_upsert_256(pt, col, value.binary_data);

  case fptu_cstr:
    if (unlikely(value.type != fpta_string))
      return FPTA_ETYPE;
    return fptu_upsert_string(pt, col, value.str, value.binary_length);
  }
}

//----------------------------------------------------------------------------

int fpta_validate_put(fpta_txn *txn, fpta_name *table_id, fptu_ro row_value,
                      fpta_put_options op) {
  if (unlikely(op < fpta_insert || op > fpta_upsert))
    return FPTA_EINVAL;

  int rc = fpta_name_refresh_couple(txn, table_id, nullptr);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  fpta_key pk_key;
  rc = fpta_index_row2key(table_id->table.pk, 0, row_value, pk_key, false);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (unlikely(table_id->mdbx_dbi < 1)) {
    rc = fpta_open_table(txn, table_id);
    if (unlikely(rc != FPTA_SUCCESS))
      return rc;
  }

  fptu_ro present_row;
  int rows_with_same_key;
  rc = mdbx_get_ex(txn->mdbx_txn, table_id->mdbx_dbi, &pk_key.mdbx,
                   &present_row.sys, &rows_with_same_key);
  if (rc != MDB_SUCCESS) {
    if (unlikely(rc != MDB_NOTFOUND))
      return rc;
    present_row.sys.iov_base = nullptr;
    present_row.sys.iov_len = 0;
  }

  switch (op) {
  default:
    assert(false && "unreachable");
    __unreachable();
    return FPTA_EINVAL;
  case fpta_insert:
    if (fpta_index_is_unique(table_id->table.pk)) {
      if (present_row.sys.iov_base)
        /* запись с таким PK уже есть, вставка НЕ возможна */
        return MDB_KEYEXIST;
    }
    break;

  case fpta_update:
    if (!present_row.sys.iov_base)
      /* нет записи с таким PK, обновлять нечего */
      return MDB_NOTFOUND;
  /* no break here */
  case fpta_upsert:
    if (rows_with_same_key > 1)
      /* обновление НЕ возможно, если первичный ключ НЕ уникален */
      return MDB_KEYEXIST;
  }

  if (present_row.sys.iov_base) {
    if (present_row.total_bytes == row_value.total_bytes &&
        !memcmp(present_row.units, row_value.units, present_row.total_bytes))
      /* если полный дубликат записи */
      return MDB_KEYEXIST;
  }

  if (!fpta_table_has_secondary(table_id))
    return FPTA_SUCCESS;

  return fpta_check_constraints(txn, table_id, present_row, row_value, 0);
}

int fpta_put(fpta_txn *txn, fpta_name *table_id, fptu_ro row,
             fpta_put_options op) {
  int rc = fpta_name_refresh_couple(txn, table_id, nullptr);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  unsigned flags = MDB_NODUPDATA;
  switch (op) {
  default:
    return FPTA_EINVAL;
  case fpta_insert:
    if (fpta_index_is_unique(table_id->table.pk))
      flags |= MDB_NOOVERWRITE;
    break;
  case fpta_update:
    flags |= MDB_CURRENT;
    break;
  case fpta_upsert:
    if (!fpta_index_is_unique(table_id->table.pk))
      flags |= MDB_NOOVERWRITE;
    break;
  }

  fpta_key pk_key;
  rc = fpta_index_row2key(table_id->table.pk, 0, row, pk_key, false);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (unlikely(table_id->mdbx_dbi < 1)) {
    rc = fpta_open_table(txn, table_id);
    if (unlikely(rc != FPTA_SUCCESS))
      return rc;
  }

  if (!fpta_table_has_secondary(table_id))
    return mdbx_put(txn->mdbx_txn, table_id->mdbx_dbi, &pk_key.mdbx, &row.sys,
                    flags);

  fptu_ro old;
#if defined(NDEBUG) && !defined(_MSC_VER)
  constexpr size_t likely_enough = 64u * 42u;
#else
  const size_t likely_enough = (time(nullptr) & 1) ? 11u : 64u * 42u;
#endif /* NDEBUG */
  void *buffer = alloca(likely_enough);
  old.sys.iov_base = buffer;
  old.sys.iov_len = likely_enough;

  rc = mdbx_replace(txn->mdbx_txn, table_id->mdbx_dbi, &pk_key.mdbx, &row.sys,
                    &old.sys, flags);
  if (unlikely(rc == MDBX_RESULT_TRUE)) {
    assert(old.sys.iov_base == nullptr && old.sys.iov_len > likely_enough);
    old.sys.iov_base = alloca(old.sys.iov_len);
    rc = mdbx_replace(txn->mdbx_txn, table_id->mdbx_dbi, &pk_key.mdbx, &row.sys,
                      &old.sys, flags);
  }
  if (unlikely(rc != MDB_SUCCESS))
    return rc;

  rc = fpta_secondary_upsert(txn, table_id, pk_key.mdbx, old, pk_key.mdbx, row,
                             0);
  if (unlikely(rc != MDB_SUCCESS))
    return fpta_internal_abort(txn, rc);

  return FPTA_SUCCESS;
}

//----------------------------------------------------------------------------

int fpta_delete(fpta_txn *txn, fpta_name *table_id, fptu_ro row) {
  int rc = fpta_name_refresh_couple(txn, table_id, nullptr);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (row.sys.iov_len && fpta_table_has_secondary(table_id) &&
      mdbx_is_dirty(txn->mdbx_txn, row.sys.iov_base)) {
    /* LY: Делаем копию строки, так как удаление в основной таблице
     * уничтожит текущее значение при перезаписи "грязной" страницы.
     * Соответственно, будут утрачены значения необходимые для чистки
     * вторичных индексов.
     *
     * FIXME: На самом деле можно не делать копию, а просто почистить
     * вторичные индексы перед удалением из основной таблице. Однако,
     * при этом сложно правильно обрабатывать ошибки. Оптимальным же будет
     * такой вариант:
     *  - открываем mdbx-курсор и устанавливаем его на удаляемую строку;
     *  - при этом обрабатываем ситуацию отсутствия удаляемой строки;
     *  - затем чистим вторичные индексы, при этом любая ошибка должна
     *    обрабатываться также как сейчас;
     *  - в конце удаляем строку из главной таблицы.
     * Но для этого варианта нужен API быстрого (inplace) открытия курсора,
     * без выделения памяти. Иначе накладные расходы будут больше экономии. */
    void *buffer = alloca(row.sys.iov_len);
    row.sys.iov_base = memcpy(buffer, row.sys.iov_base, row.sys.iov_len);
  }

  fpta_key key;
  rc = fpta_index_row2key(table_id->table.pk, 0, row, key, false);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (unlikely(table_id->mdbx_dbi < 1)) {
    rc = fpta_open_table(txn, table_id);
    if (unlikely(rc != FPTA_SUCCESS))
      return rc;
  }

  rc = mdbx_del(txn->mdbx_txn, table_id->mdbx_dbi, &key.mdbx, &row.sys);
  if (unlikely(rc != MDB_SUCCESS))
    return rc;

  if (fpta_table_has_secondary(table_id)) {
    rc = fpta_secondary_remove(txn, table_id, key.mdbx, row, 0);
    if (unlikely(rc != MDB_SUCCESS))
      return fpta_internal_abort(txn, rc);
  }

  return FPTA_SUCCESS;
}

int fpta_get(fpta_txn *txn, fpta_name *column_id,
             const fpta_value *column_value, fptu_ro *row) {
  if (unlikely(row == nullptr))
    return FPTA_EINVAL;

  row->units = nullptr;
  row->total_bytes = 0;

  if (unlikely(column_value == nullptr))
    return FPTA_EINVAL;
  if (unlikely(!fpta_id_validate(column_id, fpta_column)))
    return FPTA_EINVAL;

  fpta_name *table_id = column_id->column.table;
  int rc = fpta_name_refresh_couple(txn, table_id, column_id);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  fpta_index_type index = fpta_shove2index(column_id->shove);
  if (unlikely(index == fpta_index_none || !fpta_index_is_unique(index)))
    return FPTA_NO_INDEX;

  fpta_key column_key;
  rc = fpta_index_value2key(column_id->shove, *column_value, column_key, false);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (unlikely(column_id->mdbx_dbi < 1)) {
    rc = fpta_open_column(txn, column_id);
    if (unlikely(rc != FPTA_SUCCESS))
      return rc;
  }

  if (fpta_index_is_primary(index))
    return mdbx_get(txn->mdbx_txn, column_id->mdbx_dbi, &column_key.mdbx,
                    &row->sys);

  MDB_val pk_key;
  rc = mdbx_get(txn->mdbx_txn, column_id->mdbx_dbi, &column_key.mdbx, &pk_key);
  if (unlikely(rc != MDB_SUCCESS))
    return rc;

  rc = mdbx_get(txn->mdbx_txn, table_id->mdbx_dbi, &pk_key, &row->sys);
  if (unlikely(rc == MDB_NOTFOUND))
    return FPTA_INDEX_CORRUPTED;

  return rc;
}

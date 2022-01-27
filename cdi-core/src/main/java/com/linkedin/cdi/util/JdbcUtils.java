// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.util;

import com.google.common.collect.ImmutableMap;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;


/**
 * utility functions ported from JdbcExtractor
 */

public interface JdbcUtils {
  Map<Integer, JsonElementTypes> SQL_2_AVRO_TYPE_MAPPING = new ImmutableMap.Builder<Integer, JsonElementTypes>()
      .put(Types.BIT, JsonElementTypes.BOOLEAN)
      .put(Types.BOOLEAN, JsonElementTypes.BOOLEAN)

      .put(Types.DATE, JsonElementTypes.TIMESTAMP)
      .put(Types.TIMESTAMP, JsonElementTypes.TIMESTAMP)
      .put(Types.TIMESTAMP_WITH_TIMEZONE, JsonElementTypes.TIMESTAMP)

      .put(Types.TIME, JsonElementTypes.TIME)
      .put(Types.TIME_WITH_TIMEZONE, JsonElementTypes.TIME)

      .put(Types.TINYINT, JsonElementTypes.INT)
      .put(Types.SMALLINT, JsonElementTypes.INT)
      .put(Types.INTEGER, JsonElementTypes.INT)
      .put(Types.BIGINT, JsonElementTypes.LONG)

      .put(Types.DECIMAL, JsonElementTypes.DOUBLE)
      .put(Types.DOUBLE, JsonElementTypes.DOUBLE)
      .put(Types.FLOAT, JsonElementTypes.DOUBLE)
      .put(Types.REAL, JsonElementTypes.DOUBLE)
      .put(Types.NUMERIC, JsonElementTypes.DOUBLE)

      .put(Types.STRUCT, JsonElementTypes.RECORD)
      .put(Types.ARRAY, JsonElementTypes.ARRAY)

      .build();

  static String parseColumnAsString(final ResultSet resultset, final ResultSetMetaData resultsetMetadata, int i)
      throws SQLException {

    if (isBlob(resultsetMetadata.getColumnType(i))) {
      return readBlobAsString(resultset.getBlob(i));
    }
    if (isClob(resultsetMetadata.getColumnType(i))) {
      return readClobAsString(resultset.getClob(i));
    }
    if ((resultsetMetadata.getColumnType(i) == Types.BIT
        || resultsetMetadata.getColumnType(i) == Types.BOOLEAN)
        && convertBitToBoolean()) {
      String columnValue = Boolean.toString(resultset.getBoolean(i));
      // https://docs.oracle.com/javase/7/docs/api/java/sql/ResultSet.html#wasNull()
      return resultset.wasNull() ? null : columnValue;
    }
    return resultset.getString(i);
  }

  static boolean isBlob(int columnType) {
    return columnType == Types.LONGVARBINARY || columnType == Types.BINARY;
  }

  static boolean isClob(int columnType) {
    return columnType == Types.CLOB;
  }

  /**
   * For Blob data, need to get the bytes and use base64 encoding to encode the byte[]
   * When reading from the String, need to use base64 decoder
   *     String tmp = ... ( get the String value )
   *     byte[] foo = Base64.decodeBase64(tmp);
   */
  static String readBlobAsString(Blob logBlob) throws SQLException {
    if (logBlob == null) {
      return StringUtils.EMPTY;
    }

    byte[] ba = logBlob.getBytes(1L, (int) (logBlob.length()));

    if (ba == null) {
      return StringUtils.EMPTY;
    }
    return Base64.encodeBase64String(ba);
  }

  /**
   * For Clob data, we need to use the substring function to extract the string
   */
  static String readClobAsString(Clob logClob) throws SQLException {
    if (logClob == null) {
      return StringUtils.EMPTY;
    }
    long length = logClob.length();
    return logClob.getSubString(1, (int) length);
  }

  /**
   * HACK: there is a bug in the MysqlExtractor where tinyint columns are always treated as ints.
   * There are MySQL jdbc driver setting (tinyInt1isBit=true and transformedBitIsBoolean=false) that
   * can cause tinyint(1) columns to be treated as BIT/BOOLEAN columns. The default behavior is to
   * treat tinyint(1) as BIT.
   *
   * Currently, MysqlExtractor.getDataTypeMap() uses the information_schema to check types.
   * That does not do the above conversion. {@link #parseColumnAsString(ResultSet, ResultSetMetaData, int)}
   * which does the above type mapping.
   *
   * On the other hand, SqlServerExtractor treats BIT columns as Booleans. So we can be in a bind
   * where sometimes BIT has to be converted to an int (for backwards compatibility in MySQL) and
   * sometimes to a Boolean (for SqlServer).
   *
   * This function adds configurable behavior depending on the Extractor type.
   **/
  static boolean convertBitToBoolean() {
    return true;
  }

  /**
   * get a not nullable JsonElementType from a java.sql.Types
   * @param columnSqlType java.sql.Types
   * @return converted none nullable JsonElementType
   */
  static JsonElementTypes parseColumnType(final int columnSqlType) {
    return parseColumnType(columnSqlType, false);
  }

  /**
   * get a JsonElementType from a java.sql.Types and a nullability flag
   * @param columnSqlType java.sql.Types
   * @param nullable nullability flag
   * @return converted JsonElementType
   */
  static JsonElementTypes parseColumnType(final int columnSqlType, final boolean nullable) {
    if (nullable) {
      return SQL_2_AVRO_TYPE_MAPPING.getOrDefault(columnSqlType, JsonElementTypes.STRING).reverseNullability();
    } else {
      return SQL_2_AVRO_TYPE_MAPPING.getOrDefault(columnSqlType, JsonElementTypes.STRING);
    }
  }
}

// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.util;

import com.sun.rowset.JdbcRowSetImpl;
import java.sql.SQLException;
import java.sql.Types;
import javax.sql.rowset.RowSetMetaDataImpl;
import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


/**
 * Test following functions of JdbcUtils
 * 1. Column Type conversion from java.sql.Types to JsonElementTypes
 * 2.
 */

@PrepareForTest({Base64.class})
@PowerMockIgnore("jdk.internal.reflect.*")
public class JdbcUtilsTest extends PowerMockTestCase {

  @Mock
  private SerialBlob blob;
  @Mock
  private SerialClob clob;
  @Mock
  private JdbcRowSetImpl rowSet;
  @Mock
  private RowSetMetaDataImpl rowSetMetaData;

  /**
   * Test none nullable column type conversion per following rules
   *  Types.BOOLEAN, JsonElementTypes.BOOLEAN
   *  Types.DATE, JsonElementTypes.TIMESTAMP
   *  Types.TIMESTAMP, JsonElementTypes.TIMESTAMP
   *  Types.TIMESTAMP_WITH_TIMEZONE, JsonElementTypes.TIMESTAMP
   *  Types.TIME, JsonElementTypes.TIME
   *  Types.TIME_WITH_TIMEZONE, JsonElementTypes.TIME
   *  Types.TINYINT, JsonElementTypes.INT
   *  Types.SMALLINT, JsonElementTypes.INT
   *  Types.INTEGER, JsonElementTypes.INT
   *  Types.BIGINT, JsonElementTypes.LONG
   *  Types.DECIMAL, JsonElementTypes.DOUBLE
   *  Types.DOUBLE, JsonElementTypes.DOUBLE
   *  Types.FLOAT, JsonElementTypes.DOUBLE
   *  Types.REAL, JsonElementTypes.DOUBLE
   *  Types.NUMERIC, JsonElementTypes.DOUBLE
   *  Types.STRUCT, JsonElementTypes.RECORD
   *  Types.ARRAY, JsonElementTypes.ARRAY
   *  Everything else, JsonElementTypes.STRING
   */
  @Test
  public void testParseColumnTypeNotNull() {
    Assert.assertEquals(JdbcUtils.parseColumnType(Types.BIT), JsonElementTypes.BOOLEAN);
    Assert.assertEquals(JdbcUtils.parseColumnType(Types.BOOLEAN), JsonElementTypes.BOOLEAN);
    Assert.assertEquals(JdbcUtils.parseColumnType(Types.DATE), JsonElementTypes.TIMESTAMP);
    Assert.assertEquals(JdbcUtils.parseColumnType(Types.TIMESTAMP), JsonElementTypes.TIMESTAMP);
    Assert.assertEquals(JdbcUtils.parseColumnType(Types.TIMESTAMP_WITH_TIMEZONE), JsonElementTypes.TIMESTAMP);
    Assert.assertEquals(JdbcUtils.parseColumnType(Types.TIME_WITH_TIMEZONE), JsonElementTypes.TIME);
    Assert.assertEquals(JdbcUtils.parseColumnType(Types.TINYINT), JsonElementTypes.INT);
    Assert.assertEquals(JdbcUtils.parseColumnType(Types.SMALLINT), JsonElementTypes.INT);
    Assert.assertEquals(JdbcUtils.parseColumnType(Types.INTEGER), JsonElementTypes.INT);
    Assert.assertEquals(JdbcUtils.parseColumnType(Types.BIGINT), JsonElementTypes.LONG);
    Assert.assertEquals(JdbcUtils.parseColumnType(Types.DECIMAL), JsonElementTypes.DOUBLE);
    Assert.assertEquals(JdbcUtils.parseColumnType(Types.DOUBLE), JsonElementTypes.DOUBLE);
    Assert.assertEquals(JdbcUtils.parseColumnType(Types.FLOAT), JsonElementTypes.DOUBLE);
    Assert.assertEquals(JdbcUtils.parseColumnType(Types.NUMERIC), JsonElementTypes.DOUBLE);
    Assert.assertEquals(JdbcUtils.parseColumnType(Types.STRUCT), JsonElementTypes.RECORD);
    Assert.assertEquals(JdbcUtils.parseColumnType(Types.ARRAY), JsonElementTypes.ARRAY);
    Assert.assertEquals(JdbcUtils.parseColumnType(Types.CHAR), JsonElementTypes.STRING);
    Assert.assertEquals(JdbcUtils.parseColumnType(Types.NCHAR), JsonElementTypes.STRING);
    Assert.assertEquals(JdbcUtils.parseColumnType(Types.VARCHAR), JsonElementTypes.STRING);
    Assert.assertEquals(JdbcUtils.parseColumnType(Types.NVARCHAR), JsonElementTypes.STRING);
    Assert.assertEquals(JdbcUtils.parseColumnType(Types.LONGVARCHAR), JsonElementTypes.STRING);
    Assert.assertEquals(JdbcUtils.parseColumnType(Types.LONGNVARCHAR), JsonElementTypes.STRING);
  }

  /**
   * Test nullable column type conversion per following rules
   *  Types.BOOLEAN, JsonElementTypes.NULLABLEBOOLEAN
   *  Types.DATE, JsonElementTypes.NULLABLETIMESTAMP
   *  Types.TIMESTAMP, JsonElementTypes.NULLABLETIMESTAMP
   *  Types.TIMESTAMP_WITH_TIMEZONE, JsonElementTypes.NULLABLETIMESTAMP
   *  Types.TIME, JsonElementTypes.NULLABLETIME
   *  Types.TIME_WITH_TIMEZONE, JsonElementTypes.NULLABLETIME
   *  Types.TINYINT, JsonElementTypes.NULLABLEINT
   *  Types.SMALLINT, JsonElementTypes.NULLABLEINT
   *  Types.INTEGER, JsonElementTypes.NULLABLEINT
   *  Types.BIGINT, JsonElementTypes.NULLABLELONG
   *  Types.DECIMAL, JsonElementTypes.NULLABLEDOUBLE
   *  Types.DOUBLE, JsonElementTypes.NULLABLEDOUBLE
   *  Types.FLOAT, JsonElementTypes.NULLABLEDOUBLE
   *  Types.REAL, JsonElementTypes.NULLABLEDOUBLE
   *  Types.NUMERIC, JsonElementTypes.NULLABLEDOUBLE
   *  Types.STRUCT, JsonElementTypes.NULLABLERECORD
   *  Types.ARRAY, JsonElementTypes.NULLABLEARRAY
   *  Everything else, JsonElementTypes.NULLABLESTRING
   */
  @Test
  public void testParseColumnTypeNullable() {
    Assert.assertEquals(JdbcUtils.parseColumnType(Types.BIT, true), JsonElementTypes.NULLABLEBOOLEAN);
    Assert.assertEquals(JdbcUtils.parseColumnType(Types.BOOLEAN, true), JsonElementTypes.NULLABLEBOOLEAN);
    Assert.assertEquals(JdbcUtils.parseColumnType(Types.DATE, true), JsonElementTypes.NULLABLETIMESTAMP);
    Assert.assertEquals(JdbcUtils.parseColumnType(Types.TIMESTAMP, true), JsonElementTypes.NULLABLETIMESTAMP);
    Assert.assertEquals(JdbcUtils.parseColumnType(Types.TIMESTAMP_WITH_TIMEZONE, true), JsonElementTypes.NULLABLETIMESTAMP);
    Assert.assertEquals(JdbcUtils.parseColumnType(Types.TIME_WITH_TIMEZONE, true), JsonElementTypes.NULLABLETIME);
    Assert.assertEquals(JdbcUtils.parseColumnType(Types.TINYINT, true), JsonElementTypes.NULLABLEINT);
    Assert.assertEquals(JdbcUtils.parseColumnType(Types.SMALLINT, true), JsonElementTypes.NULLABLEINT);
    Assert.assertEquals(JdbcUtils.parseColumnType(Types.INTEGER, true), JsonElementTypes.NULLABLEINT);
    Assert.assertEquals(JdbcUtils.parseColumnType(Types.BIGINT, true), JsonElementTypes.NULLABLELONG);
    Assert.assertEquals(JdbcUtils.parseColumnType(Types.DECIMAL, true), JsonElementTypes.NULLABLEDOUBLE);
    Assert.assertEquals(JdbcUtils.parseColumnType(Types.DOUBLE, true), JsonElementTypes.NULLABLEDOUBLE);
    Assert.assertEquals(JdbcUtils.parseColumnType(Types.FLOAT, true), JsonElementTypes.NULLABLEDOUBLE);
    Assert.assertEquals(JdbcUtils.parseColumnType(Types.NUMERIC, true), JsonElementTypes.NULLABLEDOUBLE);
    Assert.assertEquals(JdbcUtils.parseColumnType(Types.STRUCT, true), JsonElementTypes.NULLABLERECORD);
    Assert.assertEquals(JdbcUtils.parseColumnType(Types.ARRAY, true), JsonElementTypes.NULLABLEARRAY);
    Assert.assertEquals(JdbcUtils.parseColumnType(Types.CHAR, true), JsonElementTypes.NULLABLESTRING);
    Assert.assertEquals(JdbcUtils.parseColumnType(Types.NCHAR, true), JsonElementTypes.NULLABLESTRING);
    Assert.assertEquals(JdbcUtils.parseColumnType(Types.VARCHAR, true), JsonElementTypes.NULLABLESTRING);
    Assert.assertEquals(JdbcUtils.parseColumnType(Types.NVARCHAR, true), JsonElementTypes.NULLABLESTRING);
    Assert.assertEquals(JdbcUtils.parseColumnType(Types.LONGVARCHAR, true), JsonElementTypes.NULLABLESTRING);
    Assert.assertEquals(JdbcUtils.parseColumnType(Types.LONGNVARCHAR, true), JsonElementTypes.NULLABLESTRING);
  }

  @Test
  public void testParseColumnAsString() throws SQLException {

    int row = 6;
    when(rowSetMetaData.getColumnType(row)).thenReturn(Types.BINARY);
    when(rowSet.getBlob(row)).thenReturn(null);
    Assert.assertEquals(JdbcUtils.parseColumnAsString(rowSet, rowSetMetaData, row), StringUtils.EMPTY);

    when(rowSetMetaData.getColumnType(row)).thenReturn(Types.CLOB);
    Assert.assertEquals(JdbcUtils.parseColumnAsString(rowSet, rowSetMetaData, row), StringUtils.EMPTY);

    when(rowSetMetaData.getColumnType(row)).thenReturn(Types.BIT);
    when(rowSet.getBoolean(row)).thenReturn(false);
    when(rowSet.wasNull()).thenReturn(false);
    Assert.assertEquals(JdbcUtils.parseColumnAsString(rowSet, rowSetMetaData, row), Boolean.toString(false));

    when(rowSetMetaData.getColumnType(row)).thenReturn(Types.BIT);
    when(rowSet.getBoolean(row)).thenReturn(false);
    when(rowSet.wasNull()).thenReturn(true);
    Assert.assertEquals(JdbcUtils.parseColumnAsString(rowSet, rowSetMetaData, row), null);

    when(rowSetMetaData.getColumnType(row)).thenReturn(Types.BIT);
    when(rowSet.getBoolean(row)).thenReturn(true);
    when(rowSet.wasNull()).thenReturn(false);
    Assert.assertEquals(JdbcUtils.parseColumnAsString(rowSet, rowSetMetaData, row), Boolean.toString(true));

    when(rowSetMetaData.getColumnType(row)).thenReturn(Types.BOOLEAN);
    when(rowSet.getBoolean(row)).thenReturn(true);
    Assert.assertEquals(JdbcUtils.parseColumnAsString(rowSet, rowSetMetaData, row), Boolean.toString(true));

    when(rowSetMetaData.getColumnType(row)).thenReturn(Types.DATE);
    when(rowSet.getString(row)).thenReturn(null);
    Assert.assertEquals(JdbcUtils.parseColumnAsString(rowSet, rowSetMetaData, row), null);
  }

  @Test
  public void testIsBlob() {
    Assert.assertEquals(JdbcUtils.isBlob(Types.LONGVARBINARY), true);
    Assert.assertEquals(JdbcUtils.isBlob(Types.BINARY), true);
    Assert.assertEquals(JdbcUtils.isBlob(Types.ARRAY), false);
  }

  @Test
  public void testIsClob() {
    Assert.assertEquals(JdbcUtils.isClob(Types.CLOB), true);
    Assert.assertEquals(JdbcUtils.isClob(Types.BLOB), false);
  }

  @Test
  public void testReadBlobAsString() throws SQLException {
    Assert.assertEquals(JdbcUtils.readBlobAsString(null), StringUtils.EMPTY);

    when(blob.length()).thenReturn(1000L);
    when(blob.getBytes(1L, (int) 1000L)).thenReturn(null);
    Assert.assertEquals(JdbcUtils.readBlobAsString(blob), StringUtils.EMPTY);

    byte[] ba = "testbytes".getBytes();
    when(blob.getBytes(1L, (int) 1000L)).thenReturn(ba);
    PowerMockito.mockStatic(Base64.class);
    String expectedBase64String = "TestBase64String";
    when(Base64.encodeBase64String(ba)).thenReturn(expectedBase64String);
    Assert.assertEquals(JdbcUtils.readBlobAsString(blob), expectedBase64String);
  }

  @Test
  public void testReadClobAsString() throws SQLException {
    Assert.assertEquals(JdbcUtils.readClobAsString(null), StringUtils.EMPTY);

    when(clob.length()).thenReturn(1000L);
    String testingClobString = "testingClobString";
    when(clob.getSubString(1, (int) 1000L)).thenReturn(testingClobString);

    Assert.assertEquals(JdbcUtils.readClobAsString(clob), testingClobString);
  }

  @Test
  public void testConvertBitToBoolean() {
    Assert.assertEquals(JdbcUtils.convertBitToBoolean(), true);
  }
}

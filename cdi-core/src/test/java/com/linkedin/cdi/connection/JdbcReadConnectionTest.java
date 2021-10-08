// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.connection;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.linkedin.cdi.exception.RetriableAuthenticationException;
import com.linkedin.cdi.extractor.MultistageExtractor;
import com.linkedin.cdi.keys.ExtractorKeys;
import com.linkedin.cdi.keys.JdbcKeys;
import com.linkedin.cdi.util.Database;
import com.linkedin.cdi.util.JsonParameter;
import com.linkedin.cdi.util.ParameterTypes;
import com.linkedin.cdi.util.VariableUtils;
import com.linkedin.cdi.util.WorkUnitStatus;
import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.rowset.JdbcRowSet;
import javax.sql.rowset.RowSetMetaDataImpl;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gobblin.configuration.WorkUnitState;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.powermock.reflect.Whitebox;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.cdi.configuration.MultistageProperties.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.*;


@PrepareForTest({Database.class, JsonParameter.class, StringEscapeUtils.class, VariableUtils.class})
public class JdbcReadConnectionTest extends PowerMockTestCase {
  /**
   * Test getFirst:
   * Scenario 1: Fail to get jdbcConnection
   * Scenario 2: Fail to execute statement
   * @throws UnsupportedEncodingException
   */
  @Test
  public void testGetFirst() throws UnsupportedEncodingException, RetriableAuthenticationException {
    PowerMockito.mockStatic(JsonParameter.class);
    PowerMockito.mockStatic(VariableUtils.class);
    PowerMockito.mockStatic(Database.class);
    when(JsonParameter.getParametersAsJson(any(), any(), any())).thenReturn(new JsonObject());
    MultistageExtractor extractor = mock(MultistageExtractor.class);
    ExtractorKeys extractorKeys = mock(ExtractorKeys.class);
    when(extractor.getExtractorKeys()).thenReturn(extractorKeys);
    when(extractorKeys.getActivationParameters()).thenReturn(new JsonObject());
    when(VariableUtils.replace(any(), any(), any())).thenReturn(new JsonObject());

    WorkUnitState state = mock(WorkUnitState.class);
    when(extractor.getState()).thenReturn(state);
    JdbcKeys jdbcSourceKeys = Mockito.mock(JdbcKeys.class);
    when(jdbcSourceKeys.getSourceParameters()).thenReturn(new JsonArray());

    JdbcConnection conn = new JdbcConnection(state, jdbcSourceKeys, extractorKeys);
    Assert.assertNull(conn.executeFirst(WorkUnitStatus.builder().build()));
  }

  /**
   * Test getNext
   */
  @Test
  public void testGetNext() throws UnsupportedEncodingException, SQLException, RetriableAuthenticationException {
    PowerMockito.mockStatic(Database.class);
    PowerMockito.mockStatic(VariableUtils.class);
    PowerMockito.mockStatic(JsonParameter.class);

    JdbcKeys jdbcSourceKeys = mock(JdbcKeys.class);
    when(jdbcSourceKeys.getCallInterval()).thenReturn(1L);
    ExtractorKeys extractorKeys = new ExtractorKeys();
    WorkUnitState state = Mockito.mock(WorkUnitState.class);
    WorkUnitStatus workUnitStatus = Mockito.mock(WorkUnitStatus.class);
    WorkUnitStatus.WorkUnitStatusBuilder builder = Mockito.mock(WorkUnitStatus.WorkUnitStatusBuilder.class);

    JdbcConnection conn = new JdbcConnection(state, jdbcSourceKeys, extractorKeys);

    String jdbcStatement = "select * from linkedin.someTable limit 1000";
    when(jdbcSourceKeys.getJdbcStatement()).thenReturn(jdbcStatement);
    extractorKeys.setSignature("testSignature");
    extractorKeys.setActivationParameters(new JsonObject());
    when(builder.build()).thenReturn(workUnitStatus);
    when(workUnitStatus.toBuilder()).thenReturn(builder);

    String uri = "jdbc:mysql://odbcva01.clientx.com:3630/linkedin?useSSL=true";
    String username = "username";
    String password = "password";
    when(jdbcSourceKeys.getSourceUri()).thenReturn(uri);
    when(state.getProp(SOURCE_CONN_USERNAME.getConfig(), StringUtils.EMPTY)).thenReturn(username);
    when(state.getProp(SOURCE_CONN_PASSWORD.getConfig(), StringUtils.EMPTY)).thenReturn(password);

    Pair<String, JsonObject> res = new MutablePair<>(jdbcStatement, new JsonObject());
    when(VariableUtils.replaceWithTracking(any(), any(), any())).thenReturn(res);
    when(VariableUtils.replace(any(), any())).thenReturn(new JsonObject());
    when(VariableUtils.replace(any(), any(), any())).thenReturn(new JsonObject());

    when(jdbcSourceKeys.isPaginationEnabled()).thenReturn(true);
    when(jdbcSourceKeys.getSourceParameters()).thenReturn(new JsonArray());
    java.sql.Connection jdbcConnection = PowerMockito.mock(java.sql.Connection.class);
    Statement statement = PowerMockito.mock(Statement.class);
    PowerMockito.when(jdbcConnection.createStatement()).thenReturn(statement);
    ResultSet resultSet = PowerMockito.mock(ResultSet.class);
    when(statement.getResultSet()).thenReturn(resultSet);
    when(statement.execute(any())).thenReturn(true);
    doNothing().when(statement).setFetchSize(anyInt());

    String unSupportedExtractor = "com.linkedin.cdi.extractor.SomeExtractor";
    when(state.getProp(MSTAGE_EXTRACTOR_CLASS.getConfig(), StringUtils.EMPTY)).thenReturn(unSupportedExtractor);
    when(JsonParameter.getParametersAsJson(any(), any(), any())).thenReturn(new JsonObject());
    Assert.assertNull(conn.executeNext(workUnitStatus));

    when(jdbcSourceKeys.getPaginationInitValues()).thenReturn(ImmutableMap.of(ParameterTypes.PAGESIZE, 100L));
    conn.setJdbcConnection(jdbcConnection);
    Assert.assertNull(conn.executeNext(workUnitStatus));

    String supportedExtractor = "com.linkedin.cdi.extractor.CsvExtractor";
    when(state.getProp(MSTAGE_EXTRACTOR_CLASS.getConfig(), StringUtils.EMPTY)).thenReturn(supportedExtractor);
    when(jdbcSourceKeys.hasSourceSchema()).thenReturn(true);
    Assert.assertEquals(conn.executeNext(workUnitStatus), workUnitStatus);

    when(statement.execute(any())).thenReturn(false);
    Assert.assertEquals(conn.executeNext(workUnitStatus), workUnitStatus);
  }

  /**
   * Test closeAll
   * Scenario: throw an exception
   */
  @Test
  public void testCloseAll() throws SQLException {
    ExtractorKeys extractorKeys = mock(ExtractorKeys.class);
    String testSignature = "test_signature";
    when(extractorKeys.getSignature()).thenReturn(testSignature);
    JdbcConnection conn = new JdbcConnection(null, new JdbcKeys(), extractorKeys);
    conn.closeAll("");

    java.sql.Connection jdbcConnection = mock(java.sql.Connection.class);
    conn.setJdbcConnection(jdbcConnection);
    doThrow(new RuntimeException()).when(jdbcConnection).close();
    conn.closeAll("");
  }

  @Test
  public void testToCsv() throws Exception {
    PowerMockito.mockStatic(StringEscapeUtils.class);
    when(StringEscapeUtils.escapeCsv(anyString())).thenReturn("test_data");
    RowSetMetaDataImpl rowSetMetaData = mock(RowSetMetaDataImpl.class);
    JdbcRowSet jdbcRowSet = PowerMockito.mock(JdbcRowSet.class);
    PowerMockito.when(jdbcRowSet.next()).thenReturn(true).thenReturn(false);
    when(rowSetMetaData.getColumnCount()).thenReturn(2);
    JdbcConnection conn = new JdbcConnection(null, new JdbcKeys(), null);
    Assert.assertEquals(Whitebox.invokeMethod(conn, "toCsv", jdbcRowSet, rowSetMetaData).toString(),
        "test_data,test_data" + System.lineSeparator());
  }

  @Test
  public void testToJson() throws Exception {
    PowerMockito.mockStatic(StringEscapeUtils.class);
    when(StringEscapeUtils.escapeCsv(anyString())).thenReturn("test_data");
    RowSetMetaDataImpl rowSetMetaData = mock(RowSetMetaDataImpl.class);
    JdbcRowSet jdbcRowSet = PowerMockito.mock(JdbcRowSet.class);
    PowerMockito.when(jdbcRowSet.next())
        .thenReturn(true).thenReturn(false)
        .thenReturn(true).thenReturn(false)
        .thenReturn(true).thenReturn(false);

    when(rowSetMetaData.getColumnCount()).thenReturn(2);
    when(rowSetMetaData.getColumnName(1)).thenReturn("column0");
    when(rowSetMetaData.getColumnName(2)).thenReturn("column1");
    JdbcConnection conn = new JdbcConnection(null, new JdbcKeys(), null);

    Assert.assertEquals(Whitebox.invokeMethod(conn, "toJson", jdbcRowSet, rowSetMetaData).toString(),
        "[{\"column0\":null,\"column1\":null}]");

    conn.getJdbcSourceKeys().setSchemaRefactorFunction("toupper");
    Assert.assertEquals(Whitebox.invokeMethod(conn, "toJson", jdbcRowSet, rowSetMetaData).toString(),
        "[{\"COLUMN0\":null,\"COLUMN1\":null}]");

    conn.getJdbcSourceKeys().setSchemaRefactorFunction("tolower");
    Assert.assertEquals(Whitebox.invokeMethod(conn, "toJson", jdbcRowSet, rowSetMetaData).toString(),
        "[{\"column0\":null,\"column1\":null}]");
  }

  /**
   * Test retrieveSchema
   */
  @Test
  public void testRetrieveSchema() throws Exception {
    RowSetMetaDataImpl rowSetMetaData = mock(RowSetMetaDataImpl.class);
    when(rowSetMetaData.getColumnCount()).thenReturn(1);
    when(rowSetMetaData.isNullable(1)).thenReturn(2);
    when(rowSetMetaData.getColumnName(1)).thenReturn("columnValue");
    when(rowSetMetaData.getColumnType(1)).thenReturn(1);
    JdbcConnection conn = new JdbcConnection(null, new JdbcKeys(), null);
    Assert.assertEquals(Whitebox.invokeMethod(conn, "retrieveSchema", rowSetMetaData).toString(),
        "[{\"columnName\":\"columnValue\",\"isNullable\":false,\"dataType\":{\"type\":\"string\"}}]");
  }
}

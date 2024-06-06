// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.connection;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.linkedin.cdi.exception.RetriableAuthenticationException;
import com.linkedin.cdi.factory.ConnectionClientFactory;
import com.linkedin.cdi.keys.ExtractorKeys;
import com.linkedin.cdi.keys.JdbcKeys;
import com.linkedin.cdi.keys.JobKeys;
import com.linkedin.cdi.util.JdbcUtils;
import com.linkedin.cdi.util.ParameterTypes;
import com.linkedin.cdi.util.SchemaBuilder;
import com.linkedin.cdi.util.WorkUnitStatus;
import com.opencsv.CSVWriter;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.gobblin.configuration.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.cdi.configuration.PropertyCollection.*;


/**
 * JdbcConnection creates transmission channel with JDBC data provider or JDBC data receiver,
 * and it executes commands per Extractor calls.
 *
 * @author Chris Li
 */
public class JdbcConnection extends MultistageConnection {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcConnection.class);
  private JdbcKeys jdbcSourceKeys;

  public JdbcKeys getJdbcSourceKeys() {
    return jdbcSourceKeys;
  }

  public void setJdbcSourceKeys(JdbcKeys jdbcSourceKeys) {
    this.jdbcSourceKeys = jdbcSourceKeys;
  }

  public Connection getJdbcConnection() {
    return jdbcConnection;
  }

  public void setJdbcConnection(Connection jdbcConnection) {
    this.jdbcConnection = jdbcConnection;
  }

  private Connection jdbcConnection;

  public JdbcConnection(State state, JobKeys jobKeys, ExtractorKeys extractorKeys) {
    super(state, jobKeys, extractorKeys);
    assert jobKeys instanceof JdbcKeys;
    jdbcSourceKeys = (JdbcKeys) jobKeys;
  }

  @Override
  public WorkUnitStatus execute(WorkUnitStatus status) {
    try {
      return executeStatement(
        getWorkUnitSpecificString(jdbcSourceKeys.getJdbcStatement(), getExtractorKeys().getDynamicParameters()),
        status);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }

  @Override
  public boolean closeAll(String message) {
    try {
      if (jdbcConnection != null) {
        jdbcConnection.close();
        jdbcConnection = null;
      }
    } catch (Exception e) {
      LOG.error("Error closing the input stream", e);
      return false;
    }
    return true;
  }

  @Override
  public WorkUnitStatus executeFirst(WorkUnitStatus workUnitStatus) throws RetriableAuthenticationException {
    WorkUnitStatus status = super.executeFirst(workUnitStatus);
    jdbcConnection = getJdbcConnection(getState());
    return jdbcConnection != null ? execute(status) : null;
  }

  @Override
  public WorkUnitStatus executeNext(WorkUnitStatus workUnitStatus) throws RetriableAuthenticationException {
    WorkUnitStatus status = super.executeNext(workUnitStatus);
    jdbcConnection = jdbcConnection == null ? getJdbcConnection(getState()) : jdbcConnection;
    return jdbcConnection != null ? execute(status) : null;
  }

  /**
   * Create jdbcConnection for work unit in thread-safe mode
   */
  private synchronized Connection getJdbcConnection(State state) {
    try {
      Class<?> factoryClass = Class.forName(MSTAGE_CONNECTION_CLIENT_FACTORY.get(state));
      ConnectionClientFactory factory = (ConnectionClientFactory) factoryClass.newInstance();

      return factory.getJdbcConnection(
          jdbcSourceKeys.getSourceUri(),
          SOURCE_CONN_USERNAME.get(state),
          SOURCE_CONN_PASSWORD.get(state),
          state);
    } catch (Exception e) {
      LOG.error("Error creating Jdbc connection: {}", e.getMessage());
    }
    return null;
  }

  /**
   * Execute the user provided statement and put the result as an InputStream in WorkUnitStatus
   *
   * The content of the InputStream can be either a JsonArray or a CSV buffer, depends on the extractor
   * configuration. The reason it is devised so is that we would use JsonExtractor to handle nested data,
   * and use CsvExtractor to handle larger data volume.
   *
   * Use case developers should decide on which option to use based on the payload by setting
   * ms.extractor.class accordingly
   *
   * Pagination is one way to control the batch size, here we fetch the page in once.
   * This control only makes sense when Limit clause is not present in the SQL statement.
   *
   * When Limit Offset is used in the SQL statement, page size = result set size.
   *
   * For better performance optimization, please use:
   *      1. time watermark partitioning if an date time index is available on the table
   *      2. unit watermarks if any attributes can be used effectively to breakdown data ingestion to smaller chunks
   * see ms.watermarks and go/dil-doc for details
   *
   * @param query the query to be executed
   * @param wuStatus the input work unit status
   * @return the updated work unit status object
   * @throws SQLException extractor shall handle this exception and fail the work unit
   */
  @SuppressFBWarnings
  private WorkUnitStatus executeStatement(
      String query,
      WorkUnitStatus wuStatus) throws SQLException {

    LOG.info("Executing SQL statement: {}", query);
    Statement stmt = jdbcConnection.createStatement();

    if (jdbcSourceKeys.isPaginationEnabled()) {
      try {
        stmt.setFetchSize(jdbcSourceKeys.getPaginationInitValues().get(ParameterTypes.PAGESIZE).intValue());
      } catch (SQLException e) {
        LOG.error("Not able to fetch size: ", e);
      }
    }

    if (stmt.execute(query)) {
      ResultSet resultSet = stmt.getResultSet();
      if (MSTAGE_EXTRACTOR_CLASS.get(getState()).matches(".*JsonExtractor.*")) {
        wuStatus.setBuffer(new ByteArrayInputStream(toJson(resultSet,
            resultSet.getMetaData()).toString().getBytes(StandardCharsets.UTF_8)));
      } else if (MSTAGE_EXTRACTOR_CLASS.get(getState()).matches(".*CsvExtractor.*")) {
        wuStatus.setBuffer(toCsvInputStream(resultSet, resultSet.getMetaData()));
      } else {
        stmt.close();
        throw new UnsupportedOperationException();
      }
      // if source schema is not present, try retrieving the source schema and store in the work unit message
      // this also prevents from processing source schema repeatedly in the pagination scenario
      if (!jdbcSourceKeys.hasSourceSchema()) {
        wuStatus.getMessages().put("schema", retrieveSchema(resultSet.getMetaData()).toString());
      }
    }
    stmt.close();
    return wuStatus;
  }

  /**
   * Converts a ResultSet to a JsonArray
   *
   * for nested dataset, this is more preferred
   *
   * @param resultSet the input result set
   * @param resultSetMetadata the result set metadata
   * @return the converted JsonArray
   * @throws SQLException SQL Exception from processing ResultSet
   */
  private JsonArray toJson(final ResultSet resultSet, final ResultSetMetaData resultSetMetadata) throws SQLException {
    JsonArray jsonArray = new JsonArray();
    while (resultSet.next()) {
      JsonObject jsonObject = new JsonObject();
      for (int i = 0; i < resultSetMetadata.getColumnCount(); i++) {
        jsonObject.addProperty(getColumnName(resultSetMetadata, i + 1), JdbcUtils.parseColumnAsString(resultSet, resultSetMetadata, i + 1));
      }
      jsonArray.add(jsonObject);
    }
    return jsonArray;
  }

  /**
   * Retrieve schema info from metadata
   * @param resultSetMetadata result set metadata
   * @return schema in JsonArray format
   * @throws SQLException SQL Exception from processing ResultSet
   */
  private JsonArray retrieveSchema(final ResultSetMetaData resultSetMetadata) throws SQLException {
    List<SchemaBuilder> columns = new ArrayList<>();
    for (int i = 0; i < resultSetMetadata.getColumnCount(); i++) {
      boolean nullable = resultSetMetadata.isNullable(i + 1) == ResultSetMetaData.columnNullable;
      columns.add(new SchemaBuilder(getColumnName(resultSetMetadata, i + 1),
          SchemaBuilder.PRIMITIVE, nullable, new ArrayList<>()).setPrimitiveType(
              JdbcUtils.parseColumnType(resultSetMetadata.getColumnType(i + 1), nullable).getAltName()));
    }
    return new SchemaBuilder(SchemaBuilder.RECORD, false, columns)
        .buildAltSchema(new HashMap<>(),
            getJobKeys().isEnableCleansing(),
            getJobKeys().getSchemaCleansingPattern(),
            getJobKeys().getSchemaCleansingReplacement(),
            getJobKeys().getSchemaCleansingNullable())
        .getAsJsonArray();
  }

  /**
   * convert column names if required
   * @param resultSetMetadata the result set schema
   * @param index1 the 1 based index of the column
   * @return the column name after conversion
   * @throws SQLException
   */
  private String getColumnName(final ResultSetMetaData resultSetMetadata, int index1) throws SQLException {
    if (jdbcSourceKeys.getSchemaRefactorFunction().equalsIgnoreCase("toupper")) {
      return resultSetMetadata.getColumnName(index1).toUpperCase();
    } else if (jdbcSourceKeys.getSchemaRefactorFunction().equalsIgnoreCase("tolower")) {
      return resultSetMetadata.getColumnName(index1).toLowerCase();
    }
    return resultSetMetadata.getColumnName(index1);
  }

  /**
   * Converts a ResultSet to CSV file and return an input stream from the file
   *
   * for large dataset, this is more preferred
   *
   * @param resultSet the input result set
   * @param resultSetMetadata the result set metadata
   * @return an InputStream
   * @throws SQLException SQL Exception from processing ResultSet
   */
  private InputStream toCsvInputStream(final ResultSet resultSet, final ResultSetMetaData resultSetMetadata)
      throws SQLException {
    try {
      Path path = Files.createTempFile(null, null);
      File file = path.toFile();
      file.deleteOnExit();
      file.setReadable(true, true);

      OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(path.toFile()), StandardCharsets.UTF_8);
      CSVWriter csvWriter = new CSVWriter(writer,
          MSTAGE_CSV.getFieldSeparator(getState()).charAt(0),
          MSTAGE_CSV.getQuoteCharacter(getState()).charAt(0),
          MSTAGE_CSV.getEscapeCharacter(getState()).charAt(0));

      long lines = 0;
      while (resultSet.next()) {
        csvWriter.writeNext(getRowAsStringArray(resultSet, resultSetMetadata));
        lines ++;
      }

      csvWriter.flush();
      csvWriter.close();

      LOG.info(String.format("Wrote %d lines to temp file %s", lines, path.toAbsolutePath()));
      return Files.newInputStream(path);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Converts one row from the ResultSet to CSV
   *
   * @param resultSet the input result set
   * @param resultSetMetadata the result set metadata
   * @return a string array including all fields
   * @throws SQLException SQL Exception from processing ResultSet
   */
  private String[] getRowAsStringArray(final ResultSet resultSet, final ResultSetMetaData resultSetMetadata) throws SQLException {
    String[] row = new String[resultSetMetadata.getColumnCount()];
    for (int i = 0; i < resultSetMetadata.getColumnCount(); i++) {
      row[i] = JdbcUtils.parseColumnAsString(resultSet, resultSetMetadata, i + 1);
    }
    return row;
  }
}

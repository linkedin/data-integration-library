// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.source;

import java.sql.Connection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import com.linkedin.cdi.configuration.MultistageProperties;
import com.linkedin.cdi.connection.JdbcConnection;
import com.linkedin.cdi.extractor.MultistageExtractor;
import com.linkedin.cdi.keys.JdbcKeys;
import com.linkedin.cdi.util.CsvUtils;
import org.apache.gobblin.source.extractor.Extractor;


/***
 * JdbcSource handles JDBC protocol
 *
 */

@Slf4j
public class JdbcSource extends MultistageSource<Schema, GenericRecord> {
  @Setter
  private ConcurrentMap<MultistageExtractor, Connection> memberConnections = new ConcurrentHashMap<>();
  private JdbcKeys jdbcSourceKeys = null;

  public JdbcSource() {
    jdbcSourceKeys = new JdbcKeys();
    jobKeys = jdbcSourceKeys;
  }

  protected void initialize(State state) {
    super.initialize(state);
    jdbcSourceKeys.logUsage(state);
    jdbcSourceKeys.setJdbcStatement(MultistageProperties.MSTAGE_JDBC_STATEMENT.getValidNonblankWithDefault(state));
    jdbcSourceKeys.setSeparator(CsvUtils.unescape(MultistageProperties.MSTAGE_CSV_SEPARATOR
        .getValidNonblankWithDefault(state)));
    jdbcSourceKeys.setQuoteCharacter(CsvUtils.unescape(MultistageProperties.MSTAGE_CSV_QUOTE_CHARACTER
        .getValidNonblankWithDefault(state)));
    jdbcSourceKeys.setEscapeCharacter(CsvUtils.unescape(MultistageProperties.MSTAGE_CSV_ESCAPE_CHARACTER
        .getValidNonblankWithDefault(state)));
    jdbcSourceKeys.setSchemaRefactorFunction(MultistageProperties.MSTAGE_JDBC_SCHEMA_REFACTOR
        .getValidNonblankWithDefault(state));
    jdbcSourceKeys.logDebugAll();
  }

  /**
   * Create extractor based on the input WorkUnitState, the extractor.class
   * configuration, and a new JdbcConnection
   *
   * @param state WorkUnitState passed in from Gobblin framework
   * @return the MultistageExtractor object
   */
  @Override
  public Extractor<Schema, GenericRecord> getExtractor(WorkUnitState state) {
    initialize(state);
    MultistageExtractor<Schema, GenericRecord> extractor =
        (MultistageExtractor<Schema, GenericRecord>) super.getExtractor(state);
    extractor.setConnection(new JdbcConnection(state, this.jdbcSourceKeys, extractor.getExtractorKeys()));
    return extractor;
  }
}

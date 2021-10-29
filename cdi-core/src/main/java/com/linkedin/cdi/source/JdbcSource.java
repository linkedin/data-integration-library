// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.source;

import com.linkedin.cdi.connection.JdbcConnection;
import com.linkedin.cdi.extractor.MultistageExtractor;
import com.linkedin.cdi.keys.JdbcKeys;
import java.sql.Connection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.Extractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.cdi.configuration.PropertyCollection.*;


/***
 * JdbcSource handles JDBC protocol
 *
 */
public class JdbcSource extends MultistageSource<Schema, GenericRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcSource.class);

  public ConcurrentMap<MultistageExtractor, Connection> getMemberConnections() {
    return memberConnections;
  }

  public void setMemberConnections(ConcurrentMap<MultistageExtractor, Connection> memberConnections) {
    this.memberConnections = memberConnections;
  }

  public JdbcKeys getJdbcSourceKeys() {
    return jdbcSourceKeys;
  }

  public void setJdbcSourceKeys(JdbcKeys jdbcSourceKeys) {
    this.jdbcSourceKeys = jdbcSourceKeys;
  }

  private ConcurrentMap<MultistageExtractor, Connection> memberConnections = new ConcurrentHashMap<>();
  private JdbcKeys jdbcSourceKeys = null;

  public JdbcSource() {
    jdbcSourceKeys = new JdbcKeys();
    jobKeys = jdbcSourceKeys;
  }

  protected void initialize(State state) {
    super.initialize(state);
    jdbcSourceKeys.setJdbcStatement(MSTAGE_JDBC_STATEMENT.get(state));
    jdbcSourceKeys.setSchemaRefactorFunction(MSTAGE_JDBC_SCHEMA_REFACTOR
        .get(state));
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

// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.keys;

import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import java.util.List;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.State;
import com.linkedin.cdi.configuration.MultistageProperties;


/**
 * This structure holds static Source parameters that are commonly used in JDBC Sources.
 *
 * @author chrli
 */
@Slf4j
@Getter (AccessLevel.PUBLIC)
@Setter(AccessLevel.PUBLIC)
public class JdbcKeys extends JobKeys {
  final private static List<MultistageProperties> ESSENTIAL_PARAMETERS = Lists.newArrayList(
      MultistageProperties.MSTAGE_JDBC_STATEMENT,
      MultistageProperties.SOURCE_CONN_USERNAME,
      MultistageProperties.SOURCE_CONN_PASSWORD);

  private String jdbcStatement = null;
  private JsonObject initialParameterValues = new JsonObject();
  private String separator = MultistageProperties.MSTAGE_CSV_SEPARATOR.getDefaultValue();
  private String quoteCharacter = MultistageProperties.MSTAGE_CSV_QUOTE_CHARACTER.getDefaultValue();
  private String escapeCharacter = MultistageProperties.MSTAGE_CSV_ESCAPE_CHARACTER.getDefaultValue();
  private String schemaRefactorFunction = MultistageProperties.MSTAGE_JDBC_SCHEMA_REFACTOR.getDefaultValue();

  @Override
  public void logDebugAll() {
    super.logDebugAll();
    log.debug("These are values in JdbcSource");
    log.debug("JDBC statement: {}", jdbcStatement);
    log.debug("Initial values of dynamic parameters: {}", initialParameterValues);
  }

  @Override
  public void logUsage(State state) {
    super.logUsage(state);
    for (MultistageProperties p: ESSENTIAL_PARAMETERS) {
      log.info("Property {} ({}) has value {} ", p.toString(), p.getClassName(), p.getValidNonblankWithDefault(state));
    }
  }
}

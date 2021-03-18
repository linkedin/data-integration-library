// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.dil.connection;

import com.google.gson.JsonObject;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.State;
import com.linkedin.dil.exception.RetriableAuthenticationException;
import com.linkedin.dil.keys.ExtractorKeys;
import com.linkedin.dil.keys.JobKeys;
import com.linkedin.dil.util.VariableUtils;
import com.linkedin.dil.util.WorkUnitStatus;

/**
 * MultistageConnection is a basic implementation of Connection interface.
 *
 * @author Chris Li
 */
@Slf4j
public class MultistageConnection implements Connection {
  @Getter @Setter private State state = null;
  @Getter @Setter private JobKeys jobKeys = null;
  @Getter @Setter private ExtractorKeys extractorKeys = null;

  public MultistageConnection(State state, JobKeys jobKeys, ExtractorKeys extractorKeys) {
    this.setJobKeys(jobKeys);
    this.setState(state);
    this.setExtractorKeys(extractorKeys);
  }

  /**
   * Default execute methods
   * @param status prior work unit status
   * @return new work unit status
   */
  @Override
  public WorkUnitStatus execute(final WorkUnitStatus status) throws RetriableAuthenticationException {
    return status.toBuilder().build();
  }

  /**
   * Close the connection and pool of connections if applicable, default
   * implementation does nothing.
   * @param message the message to send to the other end of connection upon closing
   * @return true (default)
   */
  @Override
  public boolean closeAll(final String message) {
    return true;
  }

  /**
   * Close the current cursor or stream if applicable, default
   * implementation do nothing.
   * @return true (default)
   */
  @Override
  public boolean closeStream() {
    return true;
  }

  public JsonObject getWorkUnitParameters() {
    return null;
  }

  /**
   * Default implementation of a multistage read connection
   * @param workUnitStatus prior work unit status
   * @return new work unit status
   */
  @SneakyThrows
  public WorkUnitStatus executeFirst(final WorkUnitStatus workUnitStatus) throws RetriableAuthenticationException {
    return WorkUnitStatus.builder().build();
  }

  public WorkUnitStatus executeNext(final WorkUnitStatus workUnitStatus) throws RetriableAuthenticationException {
    try {
      Thread.sleep(jobKeys.getCallInterval());
    } catch (Exception e) {
      log.warn(e.getMessage());
    }
    log.info("Starting a new request to the source, work unit = {}", extractorKeys.getSignature());
    log.debug("Prior parameters: {}", extractorKeys.getDynamicParameters().toString());
    log.debug("Prior work unit status: {}", workUnitStatus.toString());
    return workUnitStatus;
  }

  /**
   * This method applies the work unit parameters to string template, and
   * then return a work unit specific string
   *
   * @param template the template string
   * @param parameters the parameters with all variables substituted
   * @return work unit specific string
   */
  protected String getWorkUnitSpecificString(String template, JsonObject parameters) {
    String finalString = template;
    try {
      // substitute with parameters defined in ms.parameters and activation parameters
      finalString = VariableUtils.replaceWithTracking(
          finalString,
          parameters,
          false).getKey();
    } catch (Exception e) {
      log.error("Error getting work unit specific string " + e);
    }
    log.info("Final work unit specific string: {}", finalString);
    return finalString;
  }
}

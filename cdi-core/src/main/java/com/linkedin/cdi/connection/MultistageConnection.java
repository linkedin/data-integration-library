// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.connection;

import com.beust.jcommander.internal.Lists;
import com.google.gson.JsonObject;
import com.linkedin.cdi.exception.RetriableAuthenticationException;
import com.linkedin.cdi.extractor.CsvExtractor;
import com.linkedin.cdi.extractor.JsonExtractor;
import com.linkedin.cdi.keys.ExtractorKeys;
import com.linkedin.cdi.keys.JobKeys;
import com.linkedin.cdi.util.InputStreamUtils;
import com.linkedin.cdi.util.VariableUtils;
import com.linkedin.cdi.util.WorkUnitStatus;
import java.io.InputStream;
import java.util.List;
import org.apache.gobblin.configuration.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.cdi.configuration.PropertyCollection.*;
import static com.linkedin.cdi.configuration.StaticConstants.*;


/**
 * MultistageConnection is a basic implementation of Connection interface.
 *
 * @author Chris Li
 */
public class MultistageConnection implements Connection {
  private static final Logger LOG = LoggerFactory.getLogger(MultistageConnection.class);
  private State state = null;
  private JobKeys jobKeys = null;
  private ExtractorKeys extractorKeys = null;

  public State getState() {
    return state;
  }

  public void setState(State state) {
    this.state = state;
  }

  public JobKeys getJobKeys() {
    return jobKeys;
  }

  public void setJobKeys(JobKeys jobKeys) {
    this.jobKeys = jobKeys;
  }

  public ExtractorKeys getExtractorKeys() {
    return extractorKeys;
  }

  public void setExtractorKeys(ExtractorKeys extractorKeys) {
    this.extractorKeys = extractorKeys;
  }

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
  public WorkUnitStatus executeFirst(final WorkUnitStatus workUnitStatus) throws RetriableAuthenticationException {
    return WorkUnitStatus.builder().build();
  }

  public WorkUnitStatus executeNext(final WorkUnitStatus workUnitStatus) throws RetriableAuthenticationException {
    try {
      Thread.sleep(jobKeys.getCallInterval());
    } catch (Exception e) {
      LOG.warn(e.getMessage());
    }
    LOG.info("Starting a new request to the source, work unit = {}", extractorKeys.getSignature());
    LOG.debug("Prior parameters: {}", extractorKeys.getDynamicParameters().toString());
    LOG.debug("Prior work unit status: {}", workUnitStatus.toString());
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
      LOG.error("Error getting work unit specific string " + e);
    }
    LOG.info("Final work unit specific string: {}", finalString);
    return finalString;
  }

  /**
   * Wraps a list of entries into an InputStream
   *
   * The InputStream will be plain text if CsvExtractor is used, and
   * it will be a Json structure if JsonExtractor is used.
   *
   * @param entries list of files or keys
   * @return the wrapped InputStream
   */
  protected InputStream wrap(final List<String> entries) {
    List<String> list = Lists.newArrayList(entries);
    if (list.size() == 0) {
      list.add("#####dummy#file#name#or#prefix#####");
    }
    if (MSTAGE_EXTRACTOR_CLASS.get(getState()).equals(CsvExtractor.class.getName())) {
      return InputStreamUtils.convertListToInputStream(list);
    } else if (MSTAGE_EXTRACTOR_CLASS.get(getState()).equals(JsonExtractor.class.getName())) {
      return InputStreamUtils.convertListToInputStream(KEY_WORD_VALUES, list);
    } else {
      return null;
    }
  }
}

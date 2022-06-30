// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.keys;

import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.linkedin.cdi.configuration.MultistageProperties;
import com.linkedin.cdi.preprocessor.StreamProcessor;
import java.util.ArrayList;
import java.util.List;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.cdi.configuration.PropertyCollection.*;


/**
 * each of these keys provide information how to populate corresponding values
 *
 * each format Extractor is responsible for populating these key with proper values
 * so that their those values can be pull by the Source
 *
 * @author chrli
 */
public class ExtractorKeys {
  private static final Logger LOG = LoggerFactory.getLogger(ExtractorKeys.class);
  final static private List<MultistageProperties<?>> WORK_UNIT_PARAMETERS = Lists.newArrayList(
      MSTAGE_ACTIVATION_PROPERTY,
      MSTAGE_PAYLOAD_PROPERTY,
      MSTAGE_WATERMARK_GROUPS,
      MSTAGE_WORK_UNIT_SCHEDULING_STARTTIME);

  private JsonObject activationParameters = new JsonObject();
  private long startTime = DateTime.now().getMillis();
  private long delayStartTime;
  private String signature;
  private JsonArray inferredSchema = null;
  private String sessionKeyValue;
  private List<StreamProcessor<?>> preprocessors = new ArrayList<>();
  private JsonObject dynamicParameters = new JsonObject();
  private Boolean explictEof;
  private JsonArray payloads = new JsonArray();
  private String payloadsBinaryPath;
  private long processedCount = 0;

  public void incrProcessedCount() {
    processedCount++;
  }

  public void logDebugAll(WorkUnit workUnit) {
    LOG.debug("These are values in MultistageExtractor regarding to Work Unit: {}",
        workUnit == null ? "testing" : workUnit.getProp(DATASET_URN.toString()));
    LOG.debug("Activation parameters: {}", activationParameters);
    LOG.debug("Payload size: {}", payloads.size());
    LOG.debug("Starting time: {}", startTime);
    LOG.debug("Signature of the work unit: {}", signature);
    if (inferredSchema != null) {
      LOG.info("Inferred schema: {}", inferredSchema.toString());
      LOG.info("Avro-flavor schema: {}", inferredSchema.toString());
    }
    LOG.debug("Session Status: {}", sessionKeyValue);
    LOG.debug("Total rows processed: {}", processedCount);
  }

  /**
   * Log work unit specific property values
   * @param state work unit states
   */
  public void logUsage(State state) {
    for (MultistageProperties<?> p: WORK_UNIT_PARAMETERS) {
      LOG.info(p.info(state));
    }
  }

  public JsonObject getActivationParameters() {
    return activationParameters;
  }

  public void setActivationParameters(JsonObject activationParameters) {
    this.activationParameters = activationParameters;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getDelayStartTime() {
    return delayStartTime;
  }

  public void setDelayStartTime(long delayStartTime) {
    this.delayStartTime = delayStartTime;
  }

  public String getSignature() {
    return signature;
  }

  public void setSignature(String signature) {
    this.signature = signature;
  }

  public JsonArray getInferredSchema() {
    return inferredSchema;
  }

  public void setInferredSchema(JsonArray inferredSchema) {
    this.inferredSchema = inferredSchema;
  }

  public String getSessionKeyValue() {
    return sessionKeyValue;
  }

  public void setSessionKeyValue(String sessionKeyValue) {
    this.sessionKeyValue = sessionKeyValue;
  }

  public List<StreamProcessor<?>> getPreprocessors() {
    return preprocessors;
  }

  public void setPreprocessors(List<StreamProcessor<?>> preprocessors) {
    this.preprocessors = preprocessors;
  }

  public JsonObject getDynamicParameters() {
    return dynamicParameters;
  }

  public void setDynamicParameters(JsonObject dynamicParameters) {
    this.dynamicParameters = dynamicParameters;
  }

  public Boolean getExplictEof() {
    return explictEof;
  }

  public void setExplictEof(Boolean explictEof) {
    this.explictEof = explictEof;
  }

  public JsonArray getPayloads() {
    return payloads;
  }

  public void setPayloads(JsonArray payloads) {
    this.payloads = payloads;
  }

  public long getProcessedCount() {
    return processedCount;
  }

  public void setProcessedCount(long processedCount) {
    this.processedCount = processedCount;
  }

  public String getPayloadsBinaryPath() {
    return payloadsBinaryPath;
  }

  public void setPayloadsBinaryPath(String payloadsBinaryPath) {
    this.payloadsBinaryPath = payloadsBinaryPath;
  }
}

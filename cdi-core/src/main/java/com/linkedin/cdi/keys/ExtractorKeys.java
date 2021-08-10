// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.keys;

import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.List;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.State;
import com.linkedin.cdi.configuration.MultistageProperties;
import com.linkedin.cdi.preprocessor.StreamProcessor;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.joda.time.DateTime;


/**
 * each of these keys provide information how to populate corresponding values
 *
 * each format Extractor is responsible for populating these key with proper values
 * so that their those values can be pull by the Source
 *
 * @author chrli
 */
@Slf4j
@Getter (AccessLevel.PUBLIC)
@Setter
public class ExtractorKeys {
  final static private List<MultistageProperties> ESSENTIAL_PARAMETERS = Lists.newArrayList(
      MultistageProperties.EXTRACT_TABLE_NAME_KEY,
      MultistageProperties.MSTAGE_ACTIVATION_PROPERTY,
      MultistageProperties.MSTAGE_PARAMETERS
  );

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
  private long processedCount = 0;

  public void incrProcessedCount() {
    processedCount++;
  }

  public void logDebugAll(WorkUnit workUnit) {
    log.debug("These are values in MultistageExtractor regarding to Work Unit: {}",
        workUnit == null ? "testing" : workUnit.getProp(MultistageProperties.DATASET_URN_KEY.toString()));
    log.debug("Activation parameters: {}", activationParameters);
    log.debug("Payload size: {}", payloads.size());
    log.debug("Starting time: {}", startTime);
    log.debug("Signature of the work unit: {}", signature);
    if (inferredSchema != null) {
      log.info("Inferred schema: {}", inferredSchema.toString());
      log.info("Avro-flavor schema: {}", inferredSchema.toString());
    }
    log.debug("Session Status: {}", sessionKeyValue);
    log.debug("Total rows processed: {}", processedCount);
  }

  public void logUsage(State state) {
    for (MultistageProperties p: ESSENTIAL_PARAMETERS) {
      log.info("Property {} ({}) has value {} ", p.toString(), p.getClassName(), p.getValidNonblankWithDefault(state));
    }
  }
}

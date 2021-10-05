package com.linkedin.cdi.extractor;

// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.linkedin.cdi.configuration.StaticConstants;
import com.linkedin.cdi.exception.RetriableAuthenticationException;
import com.linkedin.cdi.keys.ExtractorKeys;
import com.linkedin.cdi.keys.JobKeys;
import com.linkedin.cdi.keys.JsonExtractorKeys;
import com.linkedin.cdi.preprocessor.InputStreamProcessor;
import com.linkedin.cdi.preprocessor.StreamProcessor;
import com.linkedin.cdi.util.JsonUtils;
import com.linkedin.cdi.util.WorkUnitStatus;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.WorkUnitState;
import org.testng.Assert;


/**
 * TextDumpJsonExtractor takes an InputStream, applies proper preprocessors, and returns a JSON output containing
 * complete dump of output in output field.
 * This will mainly be used in cases where output for any source is not in any specific format like Json/CSV/Avro
 * Output Schema: [{"columnName":"output","isNullable":true,"dataType":{"type":"string"}}]
 */
@Slf4j
public class TextDumpJsonExtractor extends JsonExtractor {

  /* Keeping a limit of 10 MB so that it does not result in memory issues */
  private final static int TEXT_EXTRACTOR_BYTE_LIMIT = 10 * 1024 * 1024;
  private final static int BUFFER_SIZE = 8192;
  private final static String TEXT_EXTRACTOR_SCHEMA =
      "[{\"columnName\":\"output\",\"isNullable\":true,\"dataType\":{\"type\":\"string\"}}]";

  public TextDumpJsonExtractor(WorkUnitState state, JobKeys jobKeys) {
    super(state, jobKeys);
  }

  /**
   * Returns a fixed schema from TEXT_EXTRACTOR_SCHEMA variable ain a JsonArray
   *
   * @return schema that is structured as a JsonArray but formatted as a String
   */
  @Override
  public JsonArray getSchema() {
    JsonParser parser = new JsonParser();
    JsonElement jsonelement = parser.parse(TEXT_EXTRACTOR_SCHEMA);
    JsonArray schemaArray = jsonelement.getAsJsonArray();
    Assert.assertNotNull(schemaArray);
    if (jobKeys.getDerivedFields().size() > 0 &&
        JsonUtils.get(StaticConstants.KEY_WORD_COLUMN_NAME, jobKeys.getDerivedFields().keySet().iterator().next(),
            StaticConstants.KEY_WORD_COLUMN_NAME, schemaArray) == JsonNull.INSTANCE) {
      schemaArray.addAll(addDerivedFieldsToAltSchema());
    }
    log.debug("Schema Array is " + schemaArray);
    return schemaArray;
  }

  @Nullable
  @Override
  public JsonObject readRecord(JsonObject reuse) {
    log.warn("Here 2");
    JsonExtractorKeys jsonExtractorKeys = super.getJsonExtractorKeys();
    if (super.getJsonExtractorKeys().getTotalCount() >= 1) {
      log.warn("Here 3");
      return null;
    }
    if (processInputStream(jsonExtractorKeys.getTotalCount())) {
      log.warn("Here 4");
      jsonExtractorKeys.setTotalCount(1);
      log.warn("Reading record for text extractor ");
      StringBuffer output = new StringBuffer();
      if (workUnitStatus.getBuffer() == null) {
        log.warn("Received a NULL InputStream, ending the work unit ");
        return null;
      } else {
        try {
          // apply preprocessors
          InputStream input = workUnitStatus.getBuffer();
          for (StreamProcessor<?> transformer : extractorKeys.getPreprocessors()) {
            if (transformer instanceof InputStreamProcessor) {
              input = ((InputStreamProcessor) transformer).process(input);
            }
          }
          writeToStringBuffer(input, output);
          input.close();
          JsonObject jsonObject = new JsonObject();
          jsonObject.addProperty("output", output.toString());
          JsonObject outputJson = addDerivedFields(jsonObject);
          return outputJson;
        } catch (Exception e) {
          log.error("Error while extracting from source or writing to target", e);
          this.state.setWorkingState(WorkUnitState.WorkingState.FAILED);
          return null;
        }
      }
    } else {
      return this.readRecord(reuse);
    }
  }

  /**
   * write an input stream at the dump location.
   */
  private void writeToStringBuffer(InputStream is, StringBuffer output) {
    Preconditions.checkNotNull(is, "InputStream");
    try {
      char[] buffer = new char[BUFFER_SIZE];
      long totalBytes = 0;
      int len = 0;
      Reader in = new InputStreamReader(is, StandardCharsets.UTF_8);
      while ((len = in.read(buffer)) != -1) {
        output.append(String.valueOf(buffer));
        totalBytes += len;
        if (totalBytes > TEXT_EXTRACTOR_BYTE_LIMIT) {
          log.warn("Download limit of {} bytes reached for text extractor ", TEXT_EXTRACTOR_BYTE_LIMIT);
          break;
        }
      }
      is.close();
      log.info("TextExtractor: written {} bytes ", totalBytes);
    } catch (IOException e) {
      throw new RuntimeException("Unable to extract text in TextExtractor", e);
    }
  }

  /**
   * Core data extract function that calls the Source to obtain an InputStream and then
   * decode the InputStream to records.
   *
   * @param starting the starting position of this extract, which mostly means the actual records
   *                 that have been extracted previously
   * @return false if no more data to be pulled or an significant error that requires early job termination
   */
  protected boolean processInputStream(long starting) {
    holdExecutionUnitPresetStartTime();

    if (isWorkUnitCompleted(starting)) {
      return false;
    }

    currentParameters = isFirst(starting) ? getInitialWorkUnitParameters() : getCurrentWorkUnitParameters();
    extractorKeys.setDynamicParameters(currentParameters);

    WorkUnitStatus updatedStatus = null;
    long retryies = Math.max(jobKeys.getRetryCount(), 1);
    while (retryies > 0) {
      try {
        updatedStatus = connection == null ? null : isFirst(starting) ? connection.executeFirst(this.workUnitStatus)
            : connection.executeNext(this.workUnitStatus);
        retryies = 0;
      } catch (RetriableAuthenticationException e) {
        // TODO update sourceKeys
        retryies--;
      }
    }

    if (updatedStatus == null) {
      this.failWorkUnit("Received a NULL WorkUnitStatus, fail the work unit");
      return false;
    }
    // update work unit status
    workUnitStatus.setBuffer(updatedStatus.getBuffer());
    workUnitStatus.setMessages(updatedStatus.getMessages());
    workUnitStatus.setSessionKey(getSessionKey(updatedStatus));

    // update extractor key
    extractorKeys.setSessionKeyValue(workUnitStatus.getSessionKey());

    // read source schema from the message if available
    if (jobKeys != null && !jobKeys.hasSourceSchema() && !jobKeys.hasOutputSchema() && workUnitStatus.getMessages()
        .containsKey("schema")) {
      jobKeys.setSourceSchema(workUnitStatus.getSchema());
    }
    return true;
  }
}
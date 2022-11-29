// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.extractor;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.linkedin.cdi.configuration.StaticConstants;
import com.linkedin.cdi.connection.MultistageConnection;
import com.linkedin.cdi.exception.RetriableAuthenticationException;
import com.linkedin.cdi.filter.JsonSchemaBasedFilter;
import com.linkedin.cdi.filter.MultistageSchemaBasedFilter;
import com.linkedin.cdi.keys.ExtractorKeys;
import com.linkedin.cdi.keys.JobKeys;
import com.linkedin.cdi.preprocessor.InputStreamProcessor;
import com.linkedin.cdi.preprocessor.StreamProcessor;
import com.linkedin.cdi.util.DateTimeUtils;
import com.linkedin.cdi.util.HdfsReader;
import com.linkedin.cdi.util.InputStreamUtils;
import com.linkedin.cdi.util.JsonIntermediateSchema;
import com.linkedin.cdi.util.JsonParameter;
import com.linkedin.cdi.util.JsonUtils;
import com.linkedin.cdi.util.ParameterTypes;
import com.linkedin.cdi.util.SchemaBuilder;
import com.linkedin.cdi.util.SecretManager;
import com.linkedin.cdi.util.VariableUtils;
import com.linkedin.cdi.util.WorkUnitStatus;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.validator.routines.LongValidator;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.extractor.extract.LongWatermark;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

import static com.linkedin.cdi.configuration.PropertyCollection.*;
import static com.linkedin.cdi.configuration.StaticConstants.*;


/**
 * MulistageExtractor is the base class of other format specific Extractors.
 *
 * The base class only defines function to deal with work units and activation parameters
 *
 * @author chrli
 * @param <S> The schema class
 * @param <D> The data class
 */
public class MultistageExtractor<S, D> implements Extractor<S, D> {
  private static final Logger LOG = LoggerFactory.getLogger(MultistageExtractor.class);
  protected final static String CURRENT_DATE = "currentdate";
  protected final static String PXD = "P\\d+D";
  protected final static String CONTENT_TYPE_KEY = "Content-Type";
  protected final static List<String> SUPPORTED_DERIVED_FIELD_TYPES =
      Arrays.asList(KEY_WORD_EPOC, KEY_WORD_STRING, KEY_WORD_REGEXP, KEY_WORD_BOOLEAN, KEY_WORD_INT, KEY_WORD_NUMBER);
  protected static final String COMMA_STR = ",";
  protected WorkUnitStatus workUnitStatus = WorkUnitStatus.builder().build();
  protected WorkUnitState state = null;
  protected MultistageSchemaBasedFilter<?> rowFilter = null;
  protected Boolean eof = false;
  // subclass might override this to decide whether to do record
  // level pagination
  protected Iterator<JsonElement> payloadIterator = null;
  ExtractorKeys extractorKeys = new ExtractorKeys();
  JsonObject currentParameters = null;
  MultistageConnection connection = null;
  JobKeys jobKeys;

  public WorkUnitStatus getWorkUnitStatus() {
    return workUnitStatus;
  }

  public void setWorkUnitStatus(WorkUnitStatus workUnitStatus) {
    this.workUnitStatus = workUnitStatus;
  }

  public WorkUnitState getState() {
    return state;
  }

  public void setState(WorkUnitState state) {
    this.state = state;
  }

  public MultistageSchemaBasedFilter<?> getRowFilter() {
    return rowFilter;
  }

  public void setRowFilter(MultistageSchemaBasedFilter<?> rowFilter) {
    this.rowFilter = rowFilter;
  }

  public Boolean getEof() {
    return eof;
  }

  public void setEof(Boolean eof) {
    this.eof = eof;
  }

  public ExtractorKeys getExtractorKeys() {
    return extractorKeys;
  }

  public void setExtractorKeys(ExtractorKeys extractorKeys) {
    this.extractorKeys = extractorKeys;
  }

  public JsonObject getCurrentParameters() {
    return currentParameters;
  }

  public void setCurrentParameters(JsonObject currentParameters) {
    this.currentParameters = currentParameters;
  }

  public MultistageConnection getConnection() {
    return connection;
  }

  public void setConnection(MultistageConnection connection) {
    this.connection = connection;
  }

  public JobKeys getJobKeys() {
    return jobKeys;
  }

  public void setJobKeys(JobKeys jobKeys) {
    this.jobKeys = jobKeys;
  }

  public MultistageExtractor(WorkUnitState state, JobKeys jobKeys) {
    this.state = state;
    this.jobKeys = jobKeys;
  }

  protected void initialize(ExtractorKeys keys) {
    extractorKeys = keys;
    extractorKeys.setActivationParameters(MSTAGE_ACTIVATION_PROPERTY.get(state));
    extractorKeys.setDelayStartTime(MSTAGE_WORK_UNIT_SCHEDULING_STARTTIME.get(state));
    extractorKeys.setExplictEof(MSTAGE_DATA_EXPLICIT_EOF.get(state));
    extractorKeys.setSignature(DATASET_URN.get(state));
    extractorKeys.setPreprocessors(getPreprocessors(state));
    readPayloads(state);
    extractorKeys.logDebugAll(state.getWorkunit());
  }

  @Override
  public S getSchema() {
    return null;
  }

  @Override
  public long getExpectedRecordCount() {
    return 0;
  }

  @Override
  public long getHighWatermark() {
    return 0;
  }

  @Nullable
  @Override
  public D readRecord(D reuse) {
    if (extractorKeys.getProcessedCount() % (100 * 1000) == 0) {
      LOG.debug(String.format(MSG_ROWS_PROCESSED,
          extractorKeys.getProcessedCount(),
          extractorKeys.getSignature()));
    }
    return null;
  }

  @Override
  public void close() {
    LOG.info("Closing the work unit: {}", this.extractorKeys.getSignature());

    Preconditions.checkNotNull(state.getWorkunit(), MSG_WORK_UNIT_ALWAYS);
    Preconditions.checkNotNull(state.getWorkunit().getLowWatermark(), MSG_LOW_WATER_MARK_ALWAYS);
    if (state.getWorkingState().equals(WorkUnitState.WorkingState.SUCCESSFUL)) {
      state.setActualHighWatermark(state.getWorkunit().getExpectedHighWatermark(LongWatermark.class));
    } else if (state.getActualHighWatermark() == null) {
      // Set the actual high watermark to low watermark explicitly,
      // replacing the implicit behavior in state.getActualHighWatermark(LongWatermark.class)
      // avoiding different returns from the two versions of getActualHighWatermark()
      state.setActualHighWatermark(state.getWorkunit().getLowWatermark(LongWatermark.class));
    }

    if (connection != null) {
      connection.closeAll(StringUtils.EMPTY);
    }

    // reset counters for retrying
    extractorKeys.setProcessedCount(0);
    workUnitStatus = WorkUnitStatus.builder().build();
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

    try {
      InputStream input = updatedStatus.getBuffer();
      for (StreamProcessor<?> transformer : extractorKeys.getPreprocessors()) {
        if (transformer instanceof InputStreamProcessor) {
          input = ((InputStreamProcessor) transformer).process(input);
        }
      }
      updatedStatus.setBuffer(input);
    } catch (IOException e) {
      LOG.error("Error applying preprocessors to the input stream: {}, cause: {}",
          e.getMessage(),
          e.getCause(),
          e);
      LOG.warn("Preprocessors are ignored because of IOException.");
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

  /**
   * Initialize row filter; by default, json schema based filter is used
   * @param schemaArray schema array
   */
  protected void setRowFilter(JsonArray schemaArray) {
    if (rowFilter == null) {
      if (MSTAGE_ENABLE_SCHEMA_BASED_FILTERING.get(state)) {
        rowFilter = new JsonSchemaBasedFilter(new JsonIntermediateSchema(schemaArray));
      }
    }
  }

  /**
   * returns the schema definition from properties, or if not definition not present, returns the inferred schema
   *
   * @return the schema definition in a JsonArray
   */
  JsonArray getOrInferSchema() {
    if (!this.jobKeys.hasOutputSchema()) {
      if (!processInputStream(0)) {
        return createMinimumSchema();
      }
    }

    JsonArray schemaArray = new JsonArray();
    if (this.jobKeys.hasOutputSchema()) {
      // take pre-defined fixed schema
      schemaArray = jobKeys.getOutputSchema();
      setRowFilter(schemaArray);
    } else {
      if (this.jobKeys.hasSourceSchema()) {
        schemaArray = this.jobKeys.getSourceSchema();
        schemaArray = JsonUtils.deepCopy(schemaArray).getAsJsonArray();
        LOG.info("Source provided schema: {}", schemaArray.toString());
      } else if (extractorKeys.getInferredSchema() != null) {
        schemaArray = JsonUtils.deepCopy(extractorKeys.getInferredSchema()).getAsJsonArray();
        LOG.info("Inferred schema: {}", schemaArray.toString());
      }
    }

    return schemaArray;
  }

  /**
   * Get the work unit watermarks from the work unit state
   *
   * the return value will have format like
   *
   * {"low": 123, "high": 456}
   *
   * @return the specified low and expected high wartermark in a JsonObject format
   */
  public JsonObject getWorkUnitWaterMarks() {
    Long lowWatermark = state.getWorkunit().getLowWatermark(LongWatermark.class).getValue();
    Long highWatermark = state.getWorkunit().getExpectedHighWatermark(LongWatermark.class).getValue();
    JsonObject watermark = new JsonObject();
    watermark.addProperty("low", lowWatermark);
    watermark.addProperty("high", highWatermark);
    return watermark;
  }

  /**
   *  a utility method to wait for its turn when multiple work units were started at the same time
   */
  protected void holdExecutionUnitPresetStartTime() {
    if (extractorKeys.getDelayStartTime() != 0) {
      while (DateTime.now().getMillis() < extractorKeys.getDelayStartTime()) {
        try {
          Thread.sleep(100L);
        } catch (Exception e) {
          LOG.warn(e.getMessage());
        }
      }
    }
  }

  /**
   * read preprocessor configuration and break it into an array of strings, and then
   * dynamically load each class and instantiate preprocessors.
   *
   * @param state the work unit state
   * @return a list of preprocessors
   */
  List<StreamProcessor<?>> getPreprocessors(State state) {
    ImmutableList.Builder<StreamProcessor<?>> builder = ImmutableList.builder();
    JsonObject preprocessorsParams =
        MSTAGE_EXTRACT_PREPROCESSORS_PARAMETERS.get(state);
    String preprocessors = MSTAGE_EXTRACT_PREPROCESSORS.get(state);
    JsonObject preprocessorParams;
    for (String preprocessor : preprocessors.split(COMMA_STR)) {
      String p = preprocessor.trim();
      if (!p.isEmpty()) {
        try {
          preprocessorParams = new JsonObject();
          if (preprocessorsParams.has(p)) {
            // Get the parameters for the given processor class
            preprocessorParams = preprocessorsParams.getAsJsonObject(p);

            // backward compatibility, by default create a decryption preprocessor
            if (p.contains("GpgProcessor")) {
              if (preprocessorParams.has("action") && preprocessorParams.get("action")
                  .getAsString()
                  .equalsIgnoreCase("encrypt")) {
                p = p.replaceAll("GpgProcessor", "GpgEncryptProcessor");
              } else {
                p = p.replaceAll("GpgProcessor", "GpgDecryptProcessor");
              }
            }

            // Decrypt if any credential is encrypted
            for (Map.Entry<String, JsonElement> entry : preprocessorParams.entrySet()) {
              String key = entry.getKey();
              String value = preprocessorParams.get(key).getAsString();
              String decryptedValue = SecretManager.getInstance(state).decrypt(value);
              preprocessorParams.addProperty(key, decryptedValue);
            }
          }
          Class<?> clazz = Class.forName(p);
          StreamProcessor<?> instance =
              (StreamProcessor<?>) clazz.getConstructor(JsonObject.class).newInstance(preprocessorParams);
          builder.add(instance);
        } catch (Exception e) {
          LOG.error("Error creating preprocessor: {}, Exception: {}", p, e.getMessage());
        }
      }
    }
    return builder.build();
  }

  /**
   * set work unit state to fail and log an error message as failure reason
   * @param error failure reason
   */
  protected void failWorkUnit(String error) {
    if (!StringUtils.isEmpty(error)) {
      LOG.error(error);
    }
    this.state.setWorkingState(WorkUnitState.WorkingState.FAILED);
  }

  /**
   * read the source and derive epoc from an existing field and convert it to a epoch value in the specified timezone
   *
   * Order of conversion:
   * 1. If there is a format specified, try the specified format first. In this case, the timezone is also required.
   *    Without timezone America/Los_Angeles is assumed. Actual data is also cut to length of the format string if
   *    it is longer than the format.
   * 2. If a format is unspecified (blank), or the specified format doesn't match the actual data, it will try the following
   *    additional steps:
   *    2.a. try conversion using the standard ISO date time format without timezone offset,
   *         assuming data itself has no timezone offset in it; The input timezone is used if provided,
   *         otherwise America/Los_Angeles is used.
   *    2.b. try conversion using the standard ISO date time format with timezone offset,
   *         assuming data itself has timezone offset in it; The input timezone is ignored even if it is provided
   * @param format specified format of datetime string
   * @param strValue pre-fetched value from the data source
   * @param timezone source data timezone
   * @return the epoc string: empty if failed to format strValue in the specified way
   */
  protected String deriveEpoc(String format, String strValue, String timezone) {
    String message = StringUtils.EMPTY;
    if (StringUtils.isNotBlank(format)) {
      try {
        return String.valueOf(DateTimeUtils.parse(strValue, format, timezone).getMillis());
      } catch (Exception e) {
        // no logging here because if the format is wrong, the error will be printed for each row
        message = e.getMessage();
      }
    }

    try {
      return String.valueOf(DateTimeUtils.parse(strValue, timezone).getMillis());
    } catch (Exception e) {
      failWorkUnit(e.getMessage() +  message);
      return StringUtils.EMPTY;
    }
  }

  /***
   * Append the derived field definition to the output schema
   *
   * @return output schema with the added derived field
   */
  protected JsonArray addDerivedFieldsToAltSchema() {
    JsonArray columns = new JsonArray();
    for (Map.Entry<String, Map<String, String>> entry : jobKeys.getDerivedFields().entrySet()) {
      JsonObject column = new JsonObject();
      column.addProperty("columnName", entry.getKey());
      JsonObject dataType = new JsonObject();
      switch (entry.getValue().get(KEY_WORD_TYPE)) {
        case "epoc":
          dataType.addProperty(KEY_WORD_TYPE, "long");
          break;
        case KEY_WORD_STRING:
        case KEY_WORD_INT:
        case KEY_WORD_NUMBER:
        case KEY_WORD_BOOLEAN:
          dataType.addProperty(KEY_WORD_TYPE, entry.getValue().get(KEY_WORD_TYPE));
          break;
        case "regexp":
          dataType.addProperty(KEY_WORD_TYPE, KEY_WORD_STRING);
          break;
        default:
          // by default take the source types
          JsonElement source = JsonUtils.get(entry.getValue().get(KEY_WORD_SOURCE), jobKeys.getOutputSchema());
          dataType.addProperty(KEY_WORD_TYPE,
              source.isJsonNull() ? KEY_WORD_STRING : source.getAsJsonObject().get(KEY_WORD_TYPE).getAsString());
          break;
      }
      column.add("dataType", dataType);
      columns.add(column);
    }
    return columns;
  }

  protected boolean isInputValueFromSource(String source) {
    return !(StringUtils.isEmpty(source) || source.equalsIgnoreCase(CURRENT_DATE) || source.matches(PXD)
        || VariableUtils.PATTERN.matcher(source).matches());
  }

  protected String generateDerivedFieldValue(String name, Map<String, String> derivedFieldDef,
      final String inputValue, boolean isStrValueFromSource) {
    String strValue = inputValue;
    long longValue = Long.MIN_VALUE;
    String source = derivedFieldDef.getOrDefault(KEY_WORD_SOURCE, StringUtils.EMPTY);
    String type = derivedFieldDef.get("type");
    String format = derivedFieldDef.getOrDefault(KEY_WORD_FORMAT, StringUtils.EMPTY);

    // use default timezone when it is unspecified or specified but has blank values
    String timezone = derivedFieldDef.getOrDefault(KEY_WORD_TIMEZONE, TZ_LOS_ANGELES);
    DateTimeZone timeZone = DateTimeZone.forID(StringUtils.isBlank(timezone) ? TZ_LOS_ANGELES : timezone);

    // get the base value from date times or variables
    if (source.equalsIgnoreCase(CURRENT_DATE)) {
      longValue = DateTime.now().withZone(StringUtils.isBlank(derivedFieldDef.get(KEY_WORD_TIMEZONE))
          ? DateTimeZone.UTC : DateTimeZone.forID(timezone)).getMillis();
    } else if (source.matches(PXD)) {
      Period period = Period.parse(source);
      longValue = DateTime.now().withZone(timeZone).minus(period).dayOfMonth().roundFloorCopy().getMillis();
    } else if (VariableUtils.PATTERN.matcher(source).matches()) {
      strValue = replaceVariable(source);
    } else if (!StringUtils.isEmpty(source) && !isStrValueFromSource) {
      failWorkUnit("Unsupported source for derived fields: " + source);
    }

    // further processing required for specific types
    switch (type) {
      case "epoc":
        if (longValue != Long.MIN_VALUE) {
          strValue = String.valueOf(longValue);
        } else if (StringUtils.isNotBlank(format)) {
          strValue = format.equalsIgnoreCase(KEY_WORD_ISO)
              ? deriveEpoc(StringUtils.EMPTY, strValue, timezone)
              : deriveEpoc(format, strValue, timezone);
        } else {
          // Otherwise, the strValue should be a LONG string derived from a dynamic variable source
          if (!LongValidator.getInstance().isValid(strValue)) {
            LOG.warn("Deriving Epoch value from non-numeric value without specifying format, default value to 0");
            return "0";
          }
        }
        break;
      case "regexp":
        Pattern pattern = Pattern.compile(!format.equals(StringUtils.EMPTY) ? format : "(.*)");
        Matcher matcher = pattern.matcher(strValue);
        if (matcher.find()) {
          strValue = matcher.group(1);
        } else {
          LOG.error("Regular expression finds no match!");
          strValue = "no match";
        }
        break;
      case "boolean":
        if (StringUtils.isEmpty(strValue)) {
          LOG.error("Input value of a boolean derived field should not be empty!");
        }
        break;
      default:
        break;
    }

    if (StringUtils.isEmpty(strValue)) {
      failWorkUnit(String.format("Could not extract the value for the derived field %s from %s",
          name, StringUtils.join(derivedFieldDef)));
    }
    return strValue;
  }

  /**
   * Extract the text from input stream for scenarios where an error page is returned as successful response
   * @param input the InputStream, which most likely is from an HttpResponse
   * @return the String extracted from InputStream, if the InputStream cannot be converted to a String
   * then an exception should be logged in debug mode, and an empty string returned.
   */
  protected String extractText(InputStream input) {
    LOG.debug("Parsing response InputStream as Text");
    String data = "";
    if (input != null) {
      try {
        data = InputStreamUtils.extractText(input,
            MSTAGE_SOURCE_DATA_CHARACTER_SET.get(state));
      } catch (Exception e) {
        LOG.debug(e.toString());
      }
    }
    return data;
  }

  /**
   *  If Content-Type is provided, but not as expected, the response can have
   *  useful error information
   *
   * @param wuStatus work unit status
   * @param expectedContentType expected content type
   * @return false if content type is present but not as expected otherwise true
   */
  @Deprecated
  protected boolean checkContentType(WorkUnitStatus wuStatus, String expectedContentType) {
    if (wuStatus.getMessages() != null && wuStatus.getMessages().containsKey("contentType")) {
      String contentType = wuStatus.getMessages().get("contentType");
      if (!contentType.equalsIgnoreCase(expectedContentType)) {
        LOG.info("Content is {}, expecting {}", contentType, expectedContentType);
        LOG.debug(extractText(wuStatus.getBuffer()));
        return false;
      }
    }
    return true;
  }

  protected boolean checkContentType(WorkUnitStatus wuStatus, HashSet<String> expectedContentType) {
    if (wuStatus.getMessages() != null && wuStatus.getMessages().containsKey("contentType")) {
      String contentType = wuStatus.getMessages().get("contentType");
      if (!expectedContentType.contains(contentType.toLowerCase())) {
        LOG.info("Content is {}, expecting {}", contentType, expectedContentType.toString());
        LOG.debug(extractText(wuStatus.getBuffer()));
        return false;
      }
    }
    return true;
  }

  /**
   * Retrieve session keys from the payload or header
   * @param wuStatus
   * @return the session key in the headers
   */
  protected String getSessionKey(WorkUnitStatus wuStatus) {
    if (wuStatus.getMessages() != null && wuStatus.getMessages().containsKey("headers")
        && jobKeys.getSessionKeyField() != null && jobKeys.getSessionKeyField().has("name")) {
      JsonObject headers = GSON.fromJson(wuStatus.getMessages().get("headers"), JsonObject.class);
      if (headers.has(this.jobKeys.getSessionKeyField().get("name").getAsString())) {
        return headers.get(this.jobKeys.getSessionKeyField().get("name").getAsString()).getAsString();
      }
    }
    return StringUtils.EMPTY;
  }

  /**
   * Check if the work unit is completed.
   *
   * When there is no payload data from the secondary input, it returns
   * false by default and lets the sub-classes to decide whether complete
   * the work unit becuase sub-classes can parse the incoming data.
   *
   * When a payload is configured, the records in a payload will be
   * processed one by one through the pagination mechanism. If a payload
   * dataset has many records, and the records should not be send out
   * one by one, but rather in batches, the preprocess should group the
   * records into batches.
   *
   * @param starting the starting position of the request
   * @return default true payload iterator has no more entries
   */
  protected boolean isWorkUnitCompleted(long starting) {
    if (extractorKeys.getPayloads().size() == 0) {
      // let sub class decide
      return false;
    }

    // sub-class can override this by reassigning a different iterator
    // to payloadIterator. This statement reflects the default record
    // by record pagination.
    return !payloadIterator.hasNext();
  }

  /**
   * If the position is 0, then it must be the first request
   * @param starting the starting position of the request
   * @return true if the starting position is 0, otherwise false
   */
  protected boolean isFirst(long starting) {
    return starting == 0;
  }

  /**
   * check if the stop condition has been met or if it should timeout,
   * however, when no condition is present, we assume no wait
   *
   * @return true if stop condition is met or it should timeout
   */
  protected boolean waitingBySessionKeyWithTimeout() {
    if (!jobKeys.isSessionStateEnabled() || isSessionStateMatch()) {
      return true;
    }

    // Fail if the session failCondition is met
    if (isSessionStateFailed()) {
      String message = String.format("Session fail condition is met: %s", jobKeys.getSessionStateFailCondition());
      LOG.warn(message);
      throw new RuntimeException(message);
    }

    // if stop condition is present but the condition has not been met, we
    // will check if the session should time out
    if (DateTime.now().getMillis() > extractorKeys.getStartTime() + jobKeys.getSessionTimeout()) {
      LOG.warn("Session time out after {} seconds", jobKeys.getSessionTimeout() / 1000);
      throw new RuntimeException("Session timed out before ending condition is met");
    }

    // return false to indicate wait should continue
    return false;
  }

  /**
   * Check if session state is enabled and session stop condition is met
   *
   * @return true if session state is enabled and session stop condition is met
   * otherwise return false
   */
  protected boolean isSessionStateMatch() {
    return jobKeys.isSessionStateEnabled() && extractorKeys.getSessionKeyValue()
        .matches(jobKeys.getSessionStateCondition());
  }

  /**
   * Check if session state is enabled and session fail condition is met
   *
   * @return true if session state is enabled and session fail condition is met
   * otherwise return false
   */
  protected boolean isSessionStateFailed() {
    return jobKeys.isSessionStateEnabled() && extractorKeys.getSessionKeyValue()
        .matches(jobKeys.getSessionStateFailCondition());
  }

  /**
   * This helper function determines whether to send a new pagination request. A new page
   * should be requested if:
   * 1. if session state control is enabled, then check if session stop condition is met or if timeout
   * 2. otherwise, check if pagination is enabled
   *
   * Sub-classes should further refine the new page condition.
   *
   * @return true if a new page should be requested
   */
  protected boolean hasNextPage() {
    try {
      if (jobKeys.isSessionStateEnabled()) {
        return !waitingBySessionKeyWithTimeout();
      } else {
        return jobKeys.isPaginationEnabled();
      }
    } catch (Exception e) {
      failWorkUnit(String.format("Timeout waiting for next page: %s", e.getMessage()));
      return false;
    }
  }

  /**
   * Utility function in the extractor to replace a variable
   * @param variableString variable string
   * @return actual value of a variable; empty string if variable not found
   */
  protected String replaceVariable(String variableString) {
    String finalString = "";
    try {
      finalString = VariableUtils.replaceWithTracking(variableString, currentParameters, false).getKey();
    } catch (IOException e) {
      failWorkUnit("Invalid parameter " + variableString);
    }
    return finalString;
  }

  /**
   * When there is no data return from the source, schema inferring will fail; however, Gobblin
   * will always call schema converter before record converter. When it does so in the event of
   * empty data, schema converter will fail.
   *
   * This function creates a dummy schema with primary keys and delta key to cheat converter
   *
   * @return the dummy schema with primary keys and delta keys
   */
  protected JsonArray createMinimumSchema() {
    List<SchemaBuilder> elements = new ArrayList<>();

    if (state.contains(ConfigurationKeys.EXTRACT_PRIMARY_KEY_FIELDS_KEY)) {
      String[] primaryKeys =
          state.getProp(ConfigurationKeys.EXTRACT_PRIMARY_KEY_FIELDS_KEY, StringUtils.EMPTY).split(COMMA_STR);
      for (String key: primaryKeys) {
        if (!key.isEmpty()) {
          elements.add(new SchemaBuilder(key, SchemaBuilder.PRIMITIVE, true, new ArrayList<>()).setPrimitiveType(
              KEY_WORD_STRING));
        }
      }
    }
    if (state.contains(ConfigurationKeys.EXTRACT_DELTA_FIELDS_KEY)) {
      String[] deltaKeys =
          state.getProp(ConfigurationKeys.EXTRACT_DELTA_FIELDS_KEY, StringUtils.EMPTY).split(COMMA_STR);
      for (String key: deltaKeys) {
        if (!key.isEmpty()) {
          elements.add(new SchemaBuilder(key, SchemaBuilder.PRIMITIVE, true, new ArrayList<>()).setPrimitiveType(
              KEY_WORD_TIMESTAMP));
        }
      }
    }
    return new SchemaBuilder(SchemaBuilder.RECORD, true, elements).buildAltSchema().getAsJsonArray();
  }

  public boolean closeConnection() {
    if (connection != null) {
      connection.closeAll(StringUtils.EMPTY);
    }
    return true;
  }

  /**
   * ms.parameters have variables. For the initial execution of each work unit, we substitute those
   * variables with initial work unit variable values.
   *
   * @return the substituted parameters
   */
  protected JsonObject getInitialWorkUnitParameters() {
    JsonObject definedParameters =
        JsonParameter.getParametersAsJson(MSTAGE_PARAMETERS.get(this.state).toString(), getInitialWorkUnitVariableValues(),
            this.state);
    JsonObject initialParameters = replaceVariablesInParameters(appendActivationParameter(definedParameters));

    // payload Iterator might not have been initialized yet
    if (payloadIterator != null && payloadIterator.hasNext()) {
      initialParameters.add("payload", payloadIterator.next());
    }

    return initialParameters;
  }

  /**
   * Initial variable values are not specific to protocols, moving this method here
   * so that it can be shared among protocols.
   *
   * Initial work unit variable values include
   * - watermarks defined for each work unit
   * - initial pagination defined at the source level
   * - initial session key values.
   *
   * @return work unit specific initial parameters for the first request to source
   */
  private JsonObject getInitialWorkUnitVariableValues() {
    JsonObject variableValues = new JsonObject();

    variableValues.add(ParameterTypes.WATERMARK.toString(), getWorkUnitWaterMarks());
    for (Map.Entry<ParameterTypes, Long> entry : jobKeys.getPaginationInitValues().entrySet()) {
      variableValues.addProperty(entry.getKey().toString(), entry.getValue());
    }

    Optional<String> initialSessionValue = jobKeys.getSessionInitialValue();
    initialSessionValue.ifPresent(sessionValue -> variableValues.addProperty(ParameterTypes.SESSION.toString(), sessionValue));

    return variableValues;
  }

  /**
   * Replace variables in the parameters itself, so that ms.parameters can accept variables.
   * @param parameters the JsonObject with parameters
   * @return the replaced parameter object
   */
  JsonObject replaceVariablesInParameters(final JsonObject parameters) {
    JsonObject parametersCopy = JsonUtils.deepCopy(parameters).getAsJsonObject();
    JsonObject finalParameter = JsonUtils.deepCopy(parameters).getAsJsonObject();
    try {
      Pair<String, JsonObject> replaced =
          VariableUtils.replaceWithTracking(parameters.toString(), parametersCopy, false);
      finalParameter = GSON.fromJson(replaced.getKey(), JsonObject.class);

      // for each parameter in the original parameter list, if the name of the parameter
      // name starts with "tmp" and the parameter was used once in this substitution operation,
      // then it shall be removed from the final list
      for (Map.Entry<String, JsonElement> entry : parameters.entrySet()) {
        if (entry.getKey().matches("tmp.*") && !replaced.getRight().has(entry.getKey())) {
          finalParameter.remove(entry.getKey());
        }
      }
    } catch (Exception e) {
      LOG.error("Encoding error is not expected, but : {}", e.getMessage());
    }
    LOG.debug("Final parameters: {}", finalParameter.toString());
    return finalParameter;
  }

  /**
   * Add activation parameters to work unit parameters
   * @param parameters the defined parameters
   * @return the set of parameters including activation parameters
   */
  private JsonObject appendActivationParameter(JsonObject parameters) {
    JsonObject activationParameters = extractorKeys.getActivationParameters();
    if (activationParameters.entrySet().size() > 0) {
      for (Map.Entry<String, JsonElement> entry : activationParameters.entrySet()) {
        String key = entry.getKey();
        parameters.add(key, activationParameters.get(key));
      }
    }
    return JsonUtils.deepCopy(parameters).getAsJsonObject();
  }

  protected JsonObject getCurrentWorkUnitParameters() {
    JsonObject definedParameters = JsonParameter.getParametersAsJson(MSTAGE_PARAMETERS.get(state).toString(),
        getUpdatedWorkUnitVariableValues(getInitialWorkUnitVariableValues()), state);
    JsonObject currentParameters = replaceVariablesInParameters(appendActivationParameter(definedParameters));

    if (payloadIterator != null && payloadIterator.hasNext()) {
      currentParameters.add("payload", payloadIterator.next());
    }
    return currentParameters;
  }

  /**
   * Update variable values based on work unit status
   *
   * Following variable values are updated:
   * 1. session key value if the work unit status has session key
   * 2. page start value if page start control is used
   * 3. page size value if page size control is used
   * 4. page number value if page number control is used
   *
   * Typically use case can use any of following pagination methods, some may use multiple:
   * 1. use page start (offset) and page size to control pagination
   * 2. use page number and page size to control pagination
   * 3. use page number to control pagination, while page size can be fixed
   * 4. use session key to control pagination, and the session key decides what to fetch next
   * 5. not use any variables, the session just keep going until following conditions are met:
   *    a. return an empty page
   *    b. return a specific status, such as "complete", in response
   *
   * @param initialVariableValues initial variable values
   * @return the updated variable values
   */
  private JsonObject getUpdatedWorkUnitVariableValues(JsonObject initialVariableValues) {
    JsonObject updatedVariableValues = JsonUtils.deepCopy(initialVariableValues).getAsJsonObject();
    WorkUnitStatus wuStatus = this.getWorkUnitStatus();

    // if session key is used, the extractor has to provide it int its work unit status
    // in order for this to work
    if (updatedVariableValues.has(ParameterTypes.SESSION.toString())) {
      updatedVariableValues.remove(ParameterTypes.SESSION.toString());
    }
    updatedVariableValues.addProperty(ParameterTypes.SESSION.toString(), wuStatus.getSessionKey());

    // if page start is used, the extractor has to provide it int its work unit status
    // in order for this to work
    if (updatedVariableValues.has(ParameterTypes.PAGESTART.toString())) {
      updatedVariableValues.remove(ParameterTypes.PAGESTART.toString());
    }
    updatedVariableValues.addProperty(ParameterTypes.PAGESTART.toString(), wuStatus.getPageStart());

    // page size doesn't change much often, if extractor doesn't provide
    // a page size, then assume it is the same as initial value
    if (updatedVariableValues.has(ParameterTypes.PAGESIZE.toString()) && wuStatus.getPageSize() > 0) {
      updatedVariableValues.remove(ParameterTypes.PAGESIZE.toString());
    }
    if (wuStatus.getPageSize() > 0) {
      updatedVariableValues.addProperty(ParameterTypes.PAGESIZE.toString(), wuStatus.getPageSize());
    }

    // if page number is used, the extractor has to provide it in its work unit status
    // in order for this to work
    if (updatedVariableValues.has(ParameterTypes.PAGENO.toString())) {
      updatedVariableValues.remove(ParameterTypes.PAGENO.toString());
    }
    updatedVariableValues.addProperty(ParameterTypes.PAGENO.toString(), wuStatus.getPageNumber());

    return updatedVariableValues;
  }

  /**
   * Read payload records from secondary input location. Subclasses might
   * override this to process payload differently.
   *
   * @param state WorkUnitState
   */
  protected void readPayloads(State state) {
    JsonArray payloads = MSTAGE_PAYLOAD_PROPERTY.get(state, getInitialWorkUnitParameters());
    JsonArray records = new JsonArray();
    for (JsonElement entry : payloads) {
      try {
        JsonObject entryObject = entry.getAsJsonObject();
        if (shouldReadAsBinary(entryObject)) {
          extractorKeys.setPayloadsBinaryPath(entryObject.get("path").getAsString());
          LOG.info("using binary payload path: " + entryObject.get("path").getAsString());
        }
        else {
          records.addAll(new HdfsReader(state).readSecondary(entry.getAsJsonObject()));
          extractorKeys.setPayloads(records);
          payloadIterator = records.iterator();
        }
      } catch (Exception e) {
        // in exception, put payload definition as payload, keep iterator as null
        LOG.error(String.format(ERROR_READING_SECONDARY_INPUT, KEY_WORD_PAYLOAD, DATASET_URN.get(state)));
        extractorKeys.setPayloads(payloads);
      }
    }
  }

  /**
   * read the input as a binary stream if the key "format" is set to "binary"
   */
  private boolean shouldReadAsBinary(JsonObject entryObject) {
    return JsonUtils.getAndCompare("format", "binary", entryObject);
  }

  /**
   * Validate the minimum work unit records threshold is met, otherwise raise
   * exception, which will faile the task
   */
  protected Object endProcessingAndValidateCount() {
    if (extractorKeys.getProcessedCount() < jobKeys.getMinWorkUnitRecords()) {
      throw new RuntimeException(String.format(EXCEPTION_RECORD_MINIMUM,
          jobKeys.getMinWorkUnitRecords(),
          jobKeys.getMinWorkUnitRecords()));
    }
    return null;
  }

  /**
   * Add derived fields to defined schema if they are not in already.
   *
   * In a LKG (last known good) source schema definition, the derived fields could
   * have been included in the schedule definition already, hence no action.
   *
   * @return schema that is structured as a JsonArray with derived fields if they are not added already
   */
  protected JsonArray getSchemaArray() {
    LOG.debug("Retrieving schema definition");
    JsonArray schemaArray = getOrInferSchema();
    Assert.assertNotNull(schemaArray);
    if (jobKeys.getDerivedFields().size() > 0 && JsonUtils.get(StaticConstants.KEY_WORD_COLUMN_NAME,
        jobKeys.getDerivedFields().keySet().iterator().next(), StaticConstants.KEY_WORD_COLUMN_NAME, schemaArray) == JsonNull.INSTANCE) {
      schemaArray.addAll(addDerivedFieldsToAltSchema());
    }
    return schemaArray;
  }
}

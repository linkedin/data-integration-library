// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.keys;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.linkedin.cdi.configuration.MultistageProperties;
import com.linkedin.cdi.factory.ConnectionClientFactory;
import com.linkedin.cdi.factory.reader.SchemaReader;
import com.linkedin.cdi.util.DateTimeUtils;
import com.linkedin.cdi.util.JsonUtils;
import com.linkedin.cdi.util.ParameterTypes;
import com.linkedin.cdi.util.WorkUnitPartitionTypes;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.configuration.State;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.cdi.configuration.PropertyCollection.*;
import static com.linkedin.cdi.configuration.StaticConstants.*;


/**
 * This class holds static Job parameters and it is initialized in the Source as part of
 * planning process, yet it can contain destination parameters as well in a egress scenario.
 *
 * Each of these keys provide information how to populate corresponding values in protocol
 * sub-classes. Each protocol is responsible for proper usage of these keys.
 *
 * The JobKeys class has 3 categories of functions:
 * 1. parsing: parse the complex job properties
 * 2. validating: validate job properties
 * 3. logging: log configurations
 *
 * @author chrli
 */
public class JobKeys {
  private static final Logger LOG = LoggerFactory.getLogger(JobKeys.class);
  final static public Gson GSON = new Gson();
  private Map<String, Map<String, String>> derivedFields = new HashMap<>();
  private Map<String, String> defaultFieldTypes = new HashMap<>();

  // sourceSchema is the schema provided or retrieved from source
  private JsonArray sourceSchema = new JsonArray();

  // outputSchema is the schema to be supplied to converters
  private JsonArray outputSchema = new JsonArray();

  // targetSchema is the schema to be supplied to writers
  private JsonArray targetSchema = new JsonArray();

  private JsonObject sessionKeyField = new JsonObject();
  private String totalCountField = StringUtils.EMPTY;
  private Map<ParameterTypes, String> paginationFields = new HashMap<>();
  private Map<ParameterTypes, Long> paginationInitValues = new HashMap<>();
  private long sessionTimeout;
  private long callInterval;
  private JsonArray encryptionField = new JsonArray();
  private boolean enableCleansing;
  String dataField = StringUtils.EMPTY;
  private JsonArray watermarkDefinition = new JsonArray();
  private long retryDelayInSec;
  private long retryCount;
  private Boolean isPartialPartition;
  private WorkUnitPartitionTypes workUnitPartitionType;
  private Boolean isSecondaryAuthenticationEnabled = false;
  private String sourceUri = StringUtils.EMPTY;
  private SchemaReader schemaReader;
  private String schemaCleansingPattern = "(\\s|\\$|@)";
  private String schemaCleansingReplacement = "_";
  private Boolean schemaCleansingNullable = false;
  private long minWorkUnits = 0;
  private long minWorkUnitRecords = 0;
  private JsonObject auxKeys = new JsonObject();

  public void initialize(State state) {
    parsePaginationFields(state);
    parsePaginationInitialValues(state);
    setSessionKeyField(MSTAGE_SESSION_KEY_FIELD.get(state));
    setAuxKeys(MSTAGE_AUX_KEYS.get(state));
    setTotalCountField(MSTAGE_TOTAL_COUNT_FIELD.get(state));
    setSourceUri(MSTAGE_SOURCE_URI.get(state));
    setDefaultFieldTypes(parseDefaultFieldTypes(state));
    setDerivedFields(MSTAGE_DERIVED_FIELDS.getAsMap(state));
    setOutputSchema(parseOutputSchema(state));
    setTargetSchema(MSTAGE_TARGET_SCHEMA.get(state));
    setEncryptionField(MSTAGE_ENCRYPTION_FIELDS.get(state));
    setDataField(MSTAGE_DATA_FIELD.get(state));
    setCallInterval(MSTAGE_CALL_INTERVAL_MILLIS.get(state));
    setSessionTimeout(MSTAGE_WAIT_TIMEOUT_SECONDS.getMillis(state));
    setMinWorkUnitRecords(MSTAGE_WORK_UNIT_MIN_RECORDS.get(state));
    setMinWorkUnits(MSTAGE_WORK_UNIT_MIN_UNITS.get(state));

    setEnableCleansing(MSTAGE_ENABLE_CLEANSING.get(state));
    JsonObject schemaCleansing = MSTAGE_SCHEMA_CLEANSING.get(state);
    if (schemaCleansing.has("enabled")) {
      setEnableCleansing(Boolean.parseBoolean(schemaCleansing.get("enabled").getAsString()));
      if (enableCleansing && schemaCleansing.has("pattern")) {
        setSchemaCleansingPattern(schemaCleansing.get("pattern").getAsString());
      }
      if (enableCleansing && schemaCleansing.has("replacement")) {
        setSchemaCleansingPattern(schemaCleansing.get("replacement").getAsString());
      }
      if (enableCleansing && schemaCleansing.has("nullable")) {
        setSchemaCleansingNullable(Boolean.parseBoolean(schemaCleansing.get("nullable").getAsString()));
      }
    }

    setIsPartialPartition(MSTAGE_WORK_UNIT_PARTIAL_PARTITION.get(state));
    setWorkUnitPartitionType(parsePartitionType(state));
    setWatermarkDefinition(MSTAGE_WATERMARK.get(state));
    Map<String, Long> retry = MSTAGE_SECONDARY_INPUT.getAuthenticationRetry(state);
    setRetryDelayInSec(retry.get(KEY_WORD_RETRY_DELAY_IN_SEC));
    setRetryCount(retry.get(KEY_WORD_RETRY_COUNT));
    setIsSecondaryAuthenticationEnabled(MSTAGE_SECONDARY_INPUT.isAuthenticationEnabled(state));

    setSourceSchema(readSourceSchemaFromUrn(state, MSTAGE_SOURCE_SCHEMA_URN.get(state)));
    setTargetSchema(readTargetSchemaFromUrn(state, MSTAGE_TARGET_SCHEMA_URN.get(state)));

    // closing out schema reader if it was created because of reading
    // output schema or target schema.
    if (schemaReader != null) {
      schemaReader.close();
      schemaReader = null;
    }
  }


  public boolean isPaginationEnabled() {
    // if a pagination key or an initial value is defined, then we have pagination enabled.
    // this flag will impact how session be handled, and each protocol can implement it
    // accordingly
    return paginationFields.size() > 0 || paginationInitValues.size() > 0;
  }

  public boolean isSessionStateEnabled() {
    return sessionKeyField != null
        && sessionKeyField.entrySet().size() > 0
        && sessionKeyField.has("condition")
        && sessionKeyField.get("condition").getAsJsonObject().has("regexp");
  }

  public String getSessionStateCondition() {
    if (isSessionStateEnabled()) {
      return sessionKeyField.get("condition").getAsJsonObject().get("regexp").getAsString();
    }
    return StringUtils.EMPTY;
  }

  public boolean shouldCleanseNoRangeWorkUnit() {
    if (auxKeys != null && auxKeys.has(CLEANSE_NO_RANGE_WORK_UNIT)) {
      return auxKeys.get(CLEANSE_NO_RANGE_WORK_UNIT).getAsBoolean();
    }
    return false;
  }

  /**
   * failCondition is optional in the definition
   * @return failCondition if it is defined
   */
  public String getSessionStateFailCondition() {
    String retValue = StringUtils.EMPTY;
    if (isSessionStateEnabled()) {
      try {
        retValue = sessionKeyField.get("failCondition").getAsJsonObject().get("regexp").getAsString();
      } catch (Exception e) {
        LOG.debug("failCondition is not defined: {}", sessionKeyField);
      }
    }
    return retValue;
  }

  /**
   * Return the optional initial session value if provided, otherwise return empty optional.
   */
  public Optional<String> getSessionInitialValue() {
    if (Objects.nonNull(sessionKeyField)) {
      if (sessionKeyField.has("initValue")) {
        return Optional.of(sessionKeyField.get("initValue").toString());
      }
    }
    return Optional.empty();
  }

  public boolean hasSourceSchema() {
    return sourceSchema.size() > 0;
  }

  public boolean hasOutputSchema() {
    return outputSchema.size() > 0;
  }

  public boolean hasTargetSchema() {
    return targetSchema.size() > 0;
  }

  /**
   * override the setter and update output schema when source schema is available
   * @param sourceSchema source provided schema
   */
  public JobKeys setSourceSchema(JsonArray sourceSchema) {
    this.sourceSchema = sourceSchema;
    if (!this.hasOutputSchema()) {
      setOutputSchema(JsonUtils.deepCopy(sourceSchema).getAsJsonArray());
    }
    LOG.debug("Source Schema: {}", sourceSchema.toString());
    LOG.debug("Output Schema: {}", outputSchema.toString());
    return this;
  }

  /**
   * Validate the configuration
   * @param state configuration state
   * @return true if validation was successful, otherwise false
   */
  public boolean validate(State state) {

    // Validate all job parameters
    boolean allValid = true;
    for (MultistageProperties<?> p: allProperties) {
      if (!p.isValid(state))  {
        LOG.error(p.errorMessage(state));
        allValid = false;
      }
    }

    for (String deprecatedKey: deprecatedProperties.keySet()) {
      if (state.contains(deprecatedKey) &&
          StringUtils.isNotBlank(state.getProp(deprecatedKey, StringUtils.EMPTY)))  {
        LOG.error(String.format(EXCEPTION_DEPRECATED_CONFIGURATION, deprecatedKey,
            deprecatedProperties.get(deprecatedKey).getConfig(),
            deprecatedProperties.get(deprecatedKey).getDocUrl()));
        allValid = false;
      }
    }

    if(!allValid) {
      return false;
    }

    /**
     * If pagination is enabled,  we need one of following ways to stop pagination
     *  1. through a total count field, i.e. ms.total.count.field = data.
     *    This doesn't validate the correctness of the field. The correctness of this
     *    field will be validated at extraction time in extractor classes
     *  2. through a session cursor with a stop condition,
     *    i.e. ms.session.key.field = {"name": "status", "condition": {"regexp": "success"}}.
     *    This doesn't validate whether the stop condition can truly be met.
     *    If a condition cannot be met because of incorrect specification, eventually
     *    it will timeout and fail the task.
     *  3. through a condition that will eventually lead to a empty response from the source
     *    This condition cannot be done through a static check, therefore, here only a warning is
     *    provided.
     */
    if (isPaginationEnabled()) {
      if (totalCountField == null && !isSessionStateEnabled()) {
        LOG.warn("Pagination is enabled, but there is no total count field or session \n"
            + "control to stop it. Pagination will stop only when a blank page is returned from source. \n"
            + "Please check the configuration of essential parameters if such condition can happen.");
      }
    }

    /**
     * Check if output schema is correct.
     * When a string is present but cannot be parsed, log an error.
     * It is OK if output schema is intentionally left blank.
     */
    if (!hasOutputSchema()) {
      if (!state.getProp(MSTAGE_OUTPUT_SCHEMA.getConfig(), StringUtils.EMPTY).isEmpty()) {
        LOG.error("Output schema is specified but it is an invalid or empty JsonArray");
        return false;
      }
    }

    /**
     * Check if partitioning property is correct
     */
    if (getWorkUnitPartitionType() == null) {
      String partTypeString = state.getProp(MSTAGE_WORK_UNIT_PARTITION.getConfig());
      if (!StringUtils.isBlank(partTypeString)) {
        LOG.error("ms.work.unit.partition has a unaccepted value: {}", partTypeString);
        return false;
      }
    } else if (getWorkUnitPartitionType() == WorkUnitPartitionTypes.COMPOSITE) {
      /**
       * for a broad range like this, it must generate at least 1 partition, otherwise
       * the partitioning ranges must have incorrect date strings
       */
      if (WorkUnitPartitionTypes.COMPOSITE.getRanges(
          DateTime.parse("2001-01-01"),
          DateTime.now(), true).size() < 1) {
        LOG.error("ms.work.unit.partition has incorrect or non-ISO-formatted date time values");
        return false;
      }
    }
    // TODO other checks
    // TODO validate master key location
    // TODO validate secondary input structure
    // TODO validate watermark structure
    // TODO validate parameters structure
    // TODO validate authentication structure

    return true;
  }

  public void logDebugAll() {
    LOG.debug("These are values in MultistageSource");
    LOG.debug("Source Uri: {}", sourceUri);
    LOG.debug("Total count field: {}", totalCountField);
    LOG.debug("Pagination: fields {}, initial values {}", paginationFields.toString(), paginationInitValues.toString());
    LOG.debug("Session key field definition: {}", sessionKeyField.toString());
    LOG.debug("Call interval in milliseconds: {}", callInterval);
    LOG.debug("Session timeout: {}", sessionTimeout);
    LOG.debug("Derived fields definition: {}", derivedFields.toString());
    LOG.debug("Output schema definition: {}", outputSchema.toString());
    LOG.debug("Watermark definition: {}", watermarkDefinition.toString());
    LOG.debug("Encrypted fields: {}", encryptionField);
    LOG.debug("Retry Delay: {}", retryDelayInSec);
    LOG.debug("Retry Count: {}", retryCount);
  }

  public void logUsage(State state) {
    for (MultistageProperties<?> p: allProperties) {
      LOG.info(p.info(state));
    }
  }

  private void parsePaginationFields(State state) {
    List<ParameterTypes> paramTypes = Lists.newArrayList(
        ParameterTypes.PAGESTART,
        ParameterTypes.PAGESIZE,
        ParameterTypes.PAGENO
    );
    if (MSTAGE_PAGINATION.isValidNonblank(state)) {
      JsonObject p = MSTAGE_PAGINATION.get(state);
      if (p.has("fields")) {
        JsonArray fields = p.get("fields").getAsJsonArray();
        for (int i = 0; i < fields.size(); i++) {
          if (StringUtils.isNoneBlank(fields.get(i).getAsString())) {
            paginationFields.put(paramTypes.get(i), fields.get(i).getAsString());
          }
        }
      }
    }
  }

  private void parsePaginationInitialValues(State state) {
    List<ParameterTypes> paramTypes = Lists.newArrayList(
        ParameterTypes.PAGESTART,
        ParameterTypes.PAGESIZE,
        ParameterTypes.PAGENO
    );
    if (MSTAGE_PAGINATION.isValidNonblank(state)) {
      JsonObject p = MSTAGE_PAGINATION.get(state);
      if (p.has("initialvalues")) {
        JsonArray values = p.get("initialvalues").getAsJsonArray();
        for (int i = 0; i < values.size(); i++) {
          paginationInitValues.put(paramTypes.get(i), values.get(i).getAsLong());
        }
      }
    } else {
      setPaginationInitValues(new HashMap<>());
    }
  }

  /**
   * Default field types can be used in schema inferrence, this method
   * collect default field types if they are  specified in configuration.
   *
   * @return A map of fields and their default types
   */
  private Map<String, String> parseDefaultFieldTypes(State state) {
    if (MSTAGE_DATA_DEFAULT_TYPE.isValidNonblank(state)) {
      return GSON.fromJson(MSTAGE_DATA_DEFAULT_TYPE.get(state).toString(),
          new TypeToken<HashMap<String, String>>() {
          }.getType());
    }
    return new HashMap<>();
  }

  /**
   * Parse output schema defined in ms.output.schema parameter
   *
   * @param state the Gobblin configurations
   * @return the output schema
   */
  public JsonArray parseOutputSchema(State state) {
    return JsonUtils.deepCopy(MSTAGE_OUTPUT_SCHEMA.get(state)).getAsJsonArray();
  }


  /**
   * This helper function parse out the WorkUnitPartitionTypes from ms.work.unit.partition property
   * @param state the State with all configurations
   * @return the WorkUnitPartitionTypes
   */
  WorkUnitPartitionTypes parsePartitionType(State state) {
    WorkUnitPartitionTypes partitionType = WorkUnitPartitionTypes.fromString(
        MSTAGE_WORK_UNIT_PARTITION.get(state));

    if (partitionType != WorkUnitPartitionTypes.COMPOSITE) {
      return partitionType;
    }

    // add sub ranges for composite partition type
    WorkUnitPartitionTypes.COMPOSITE.resetSubRange();
    try {
      JsonObject jsonObject = GSON.fromJson(
          MSTAGE_WORK_UNIT_PARTITION.get(state).toString(),
          JsonObject.class);

      for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
        String partitionTypeString = entry.getKey();
        DateTime start = DateTimeUtils.parse(jsonObject.get(entry.getKey()).getAsJsonArray().get(0).getAsString());
        String endDateTimeString = jsonObject.get(entry.getKey()).getAsJsonArray().get(1).getAsString();
        DateTime end;
        if (endDateTimeString.matches("-")) {
          end = DateTime.now();
        } else {
          end = DateTimeUtils.parse(endDateTimeString);
        }
        partitionType.addSubRange(start, end, WorkUnitPartitionTypes.fromString(partitionTypeString));
      }
    } catch (Exception e) {
      LOG.error("Error parsing composite partition string: "
              + MSTAGE_WORK_UNIT_PARTITION.get(state).toString()
              + "\n partitions may not be generated properly.",
          e);
    }
    return partitionType;
  }

  /**
   * Call the reader factory and read schema of the URN
   * @param urn the dataset URN
   * @param state gobblin configuration
   * @return schema in a JsonArray
   */
  @VisibleForTesting
  public JsonArray readSchemaFromUrn(State state, String urn) {
    try {
      // Schema Reader could be plugged in before the initialization on JobKeys
      if (schemaReader == null) {
        Class<?> factoryClass = Class.forName(
            MSTAGE_CONNECTION_CLIENT_FACTORY.get(state));
        ConnectionClientFactory factory = (ConnectionClientFactory) factoryClass.newInstance();
        schemaReader = factory.getSchemaReader(state);
      }
      return schemaReader.read(state, urn).getAsJsonArray();
    } catch (Exception e) {
      LOG.error("Error reading schema based on urn: {}", urn);
      throw new RuntimeException(e);
    }
  }

  /**
   * Filter out derived fields that will be added later on
   * @param inSchema the schema array from reader
   * @return the filtered schema
   */
  public JsonArray removeDerivedFieldsFromSchema(JsonArray inSchema) {
    Set<String> derived = getDerivedFields().keySet();
    JsonArray output = new JsonArray();
    inSchema.forEach(column -> {
      if (!derived.contains(column.getAsJsonObject().get(KEY_WORD_COLUMN_NAME).getAsString())) {
        output.add(column);
      }
    });
    return output;
  }

  /**
   * Read source schema if output schema is not present
   *
   * @param state the Gobblin configurations
   * @param urn the source schema URN
   * @return the source schema from URN if output schema is not present,
   * otherwise return the output schema directly
   */
  public JsonArray readSourceSchemaFromUrn(State state, String urn) {
    if (!hasOutputSchema() && StringUtils.isNotBlank(urn)) {
      JsonArray schema = removeDerivedFieldsFromSchema(readSchemaFromUrn(state, urn));
      return JsonUtils.deepCopy(schema).getAsJsonArray();
    }
    return getOutputSchema();
  }

  /**
   * Target schema can come from 2 sources
   *
   * 1. actual schema defined in ms.target.schema parameter
   * 2. a URN or source defined in ms.target.schema.urn
   *
   * @param state the Gobblin configurations
   * @return the target schema
   */
  public JsonArray readTargetSchemaFromUrn(State state, String urn) {
    return !hasTargetSchema() && StringUtils.isNotBlank(urn)
        ? readSchemaFromUrn(state, urn)
        : getTargetSchema();
  }

  public Map<String, Map<String, String>> getDerivedFields() {
    return derivedFields;
  }

  public void setDerivedFields(Map<String, Map<String, String>> derivedFields) {
    this.derivedFields = derivedFields;
  }

  public Map<String, String> getDefaultFieldTypes() {
    return defaultFieldTypes;
  }

  public void setDefaultFieldTypes(Map<String, String> defaultFieldTypes) {
    this.defaultFieldTypes = defaultFieldTypes;
  }

  public JsonArray getSourceSchema() {
    return sourceSchema;
  }

  public JsonArray getOutputSchema() {
    return outputSchema;
  }

  public void setOutputSchema(JsonArray outputSchema) {
    this.outputSchema = outputSchema;
  }

  public JsonArray getTargetSchema() {
    return targetSchema;
  }

  public void setTargetSchema(JsonArray targetSchema) {
    this.targetSchema = targetSchema;
  }

  public JsonObject getSessionKeyField() {
    return sessionKeyField;
  }

  public void setSessionKeyField(JsonObject sessionKeyField) {
    this.sessionKeyField = sessionKeyField;
  }

  public JsonObject getAuxKeys() {
    return auxKeys;
  }

  public void setAuxKeys(JsonObject auxKeys) {
    this.auxKeys = auxKeys;
  }

  public String getTotalCountField() {
    return totalCountField;
  }

  public void setTotalCountField(String totalCountField) {
    this.totalCountField = totalCountField;
  }

  public Map<ParameterTypes, String> getPaginationFields() {
    return paginationFields;
  }

  public void setPaginationFields(Map<ParameterTypes, String> paginationFields) {
    this.paginationFields = paginationFields;
  }

  public Map<ParameterTypes, Long> getPaginationInitValues() {
    return paginationInitValues;
  }

  public void setPaginationInitValues(Map<ParameterTypes, Long> paginationInitValues) {
    this.paginationInitValues = paginationInitValues;
  }

  public long getSessionTimeout() {
    return sessionTimeout;
  }

  public void setSessionTimeout(long sessionTimeout) {
    this.sessionTimeout = sessionTimeout;
  }

  public long getCallInterval() {
    return callInterval;
  }

  public void setCallInterval(long callInterval) {
    this.callInterval = callInterval;
  }

  public JsonArray getEncryptionField() {
    return encryptionField;
  }

  public void setEncryptionField(JsonArray encryptionField) {
    this.encryptionField = encryptionField;
  }

  public boolean isEnableCleansing() {
    return enableCleansing;
  }

  public void setEnableCleansing(boolean enableCleansing) {
    this.enableCleansing = enableCleansing;
  }

  public String getDataField() {
    return dataField;
  }

  public void setDataField(String dataField) {
    this.dataField = dataField;
  }

  public JsonArray getWatermarkDefinition() {
    return watermarkDefinition;
  }

  public void setWatermarkDefinition(JsonArray watermarkDefinition) {
    this.watermarkDefinition = watermarkDefinition;
  }

  public long getRetryDelayInSec() {
    return retryDelayInSec;
  }

  public void setRetryDelayInSec(long retryDelayInSec) {
    this.retryDelayInSec = retryDelayInSec;
  }

  public long getRetryCount() {
    return retryCount;
  }

  public void setRetryCount(long retryCount) {
    this.retryCount = retryCount;
  }

  public Boolean getIsPartialPartition() {
    return isPartialPartition;
  }

  public void setIsPartialPartition(Boolean partialPartition) {
    isPartialPartition = partialPartition;
  }

  public WorkUnitPartitionTypes getWorkUnitPartitionType() {
    return workUnitPartitionType;
  }

  public void setWorkUnitPartitionType(WorkUnitPartitionTypes workUnitPartitionType) {
    this.workUnitPartitionType = workUnitPartitionType;
  }

  public Boolean getIsSecondaryAuthenticationEnabled() {
    return isSecondaryAuthenticationEnabled;
  }

  public void setIsSecondaryAuthenticationEnabled(Boolean secondaryAuthenticationEnabled) {
    isSecondaryAuthenticationEnabled = secondaryAuthenticationEnabled;
  }

  public String getSourceUri() {
    return sourceUri;
  }

  public void setSourceUri(String sourceUri) {
    this.sourceUri = sourceUri;
  }

  public SchemaReader getSchemaReader() {
    return schemaReader;
  }

  public void setSchemaReader(SchemaReader schemaReader) {
    this.schemaReader = schemaReader;
  }

  public String getSchemaCleansingPattern() {
    return schemaCleansingPattern;
  }

  public void setSchemaCleansingPattern(String schemaCleansingPattern) {
    this.schemaCleansingPattern = schemaCleansingPattern;
  }

  public String getSchemaCleansingReplacement() {
    return schemaCleansingReplacement;
  }

  public void setSchemaCleansingReplacement(String schemaCleansingReplacement) {
    this.schemaCleansingReplacement = schemaCleansingReplacement;
  }

  public Boolean getSchemaCleansingNullable() {
    return schemaCleansingNullable;
  }

  public void setSchemaCleansingNullable(Boolean schemaCleansingNullable) {
    this.schemaCleansingNullable = schemaCleansingNullable;
  }

  public long getMinWorkUnits() {
    return minWorkUnits;
  }

  public void setMinWorkUnits(long minWorkUnits) {
    this.minWorkUnits = minWorkUnits;
  }

  public long getMinWorkUnitRecords() {
    return minWorkUnitRecords;
  }

  public void setMinWorkUnitRecords(long minWorkUnitRecords) {
    this.minWorkUnitRecords = minWorkUnitRecords;
  }
}

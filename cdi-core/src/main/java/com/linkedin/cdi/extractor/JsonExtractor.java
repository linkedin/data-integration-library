// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.extractor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.linkedin.cdi.filter.JsonSchemaBasedFilter;
import com.linkedin.cdi.keys.ExtractorKeys;
import com.linkedin.cdi.keys.JobKeys;
import com.linkedin.cdi.keys.JsonExtractorKeys;
import com.linkedin.cdi.util.EncryptionUtils;
import com.linkedin.cdi.util.JsonUtils;
import com.linkedin.cdi.util.ParameterTypes;
import com.linkedin.cdi.util.SchemaBuilder;
import com.linkedin.cdi.util.SecretManager;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.configuration.WorkUnitState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.cdi.configuration.PropertyCollection.*;
import static com.linkedin.cdi.configuration.StaticConstants.*;


/**
 * JsonExtractor reads Json formatted responses from HTTP sources, like Rest API source.
 *
 * This extractor will output schema in JsonArray format, such as
 * [{"columnName": "id", "type": "string"},{"columnName": "duration", "type": "integer"}]
 *
 * The rows will be pass output to converters in the form of JsonObjects, which represent
 * rows.
 *
 * This extractor can used to feed into a JsonIntermediateToAvroConverter and sink data into Avro.
 *
 * @author chrli
 */
public class JsonExtractor extends MultistageExtractor<JsonArray, JsonObject> {
  private static final Logger LOG = LoggerFactory.getLogger(JsonExtractor.class);
  final private static JsonObject EOF = new Gson().fromJson("{\"EOF\": \"EOF\"}", JsonObject.class);

  private final static String JSON_MEMBER_SEPARATOR = ".";
  private final static Long SCHEMA_INFER_MAX_SAMPLE_SIZE = 100L;
  private JsonExtractorKeys jsonExtractorKeys = new JsonExtractorKeys();

  public JsonExtractorKeys getJsonExtractorKeys() {
    return jsonExtractorKeys;
  }

  public JsonExtractor(WorkUnitState state, JobKeys jobKeys) {
    super(state, jobKeys);
    super.initialize(jsonExtractorKeys);
    initialize(jsonExtractorKeys);
  }

  @Override
  protected void initialize(ExtractorKeys keys) {
    jsonExtractorKeys.logUsage(state);
    jsonExtractorKeys.logDebugAll(state.getWorkunit());
  }

  /**
   * Utility function to do a double assignment
   * @param jsonExtractorKeys the extractor key
   */
  @VisibleForTesting
  protected void setJsonExtractorKeys(JsonExtractorKeys jsonExtractorKeys) {
    this.extractorKeys = jsonExtractorKeys;
    this.jsonExtractorKeys = jsonExtractorKeys;
  }

  /**
   * getSchema will be called by Gobblin to retrieve the schema of the output of this extract.
   * The returned schema will be used in subsequent converters. The alternative schema here suites
   * JsonIntermediateToAvroConverter. Future development can support other converter by making
   * the schema conversion configurable.
   *
   * typically a json schema would be like following with nesting structures
   * <p>{</p>
   * <p>  "type": "array",</p>
   * <p>  "items": {</p>
   * <p>    "id": {</p>
   * <p>      "type": "string"</p>
   * <p>    },</p>
   * <p>    "emailAddress": {</p>
   * <p>      "type": "string"</p>
   * <p>    },</p>
   * <p>    "emailAliases": {</p>
   * <p>      "type": "array",</p>
   * <p>      "items": {</p>
   * <p>        "type": ["string", "null"]</p>
   * <p>      }</p>
   * <p>    },
   * <p>    "personalMeetingUrls": {</p>
   * <p>      "type": "string",</p>
   * <p>      "items": {</p>
   * <p>        "type": "null"</p>
   * <p>      }</p>
   * <p>    },</p>
   * <p>    "settings": {</p>
   * <p>      "type": "object",</p>
   * <p>      "properties": {</p>
   * <p>        "webConferencesRecorded": {</p>
   * <p>          "type": "boolean"</p>
   * <p>        },</p>
   * <p>        "preventWebConferenceRecording": {</p>
   * <p>          "type": "boolean"</p>
   * <p>        },</p>
   * <p>        "preventEmailImport": {</p>
   * <p>          "type": "boolean"</p>
   * <p>        }</p>
   * <p>      }</p>
   * <p>    }</p>
   * <p>  }</p>
   * <p>}</p>
   *
   * <p>However an alternative or intermediate way of writing the schema</p>
   * <p>{"emailAddress": {"type": "string"}}</p>
   * <p>is:</p>
   * <p>{"columnName": "emailAddress", "dataType": {"type": "string'}}</p>
   *
   * @return the schema of the extracted record set in a JasonArray String
   */
  @Override
  public JsonArray getSchema() {
    return getSchemaArray();
  }

  @Nullable
  @Override
  public JsonObject readRecord(JsonObject reuse) {
    super.readRecord(reuse);

    if (jsonExtractorKeys.getJsonElementIterator() == null && !processInputStream(0)) {
      return (JsonObject) endProcessingAndValidateCount();
    }

    if (jsonExtractorKeys.getJsonElementIterator().hasNext()) {
      jsonExtractorKeys.setProcessedCount(1 + jsonExtractorKeys.getProcessedCount());
      JsonObject row = jsonExtractorKeys.getJsonElementIterator().next().getAsJsonObject();
      if (jobKeys.getEncryptionField() != null && jobKeys.getEncryptionField().size() > 0) {
        row = encryptJsonFields("", row);
      }
      if (jobKeys.isEnableCleansing()) {
        row = limitedCleanse(row).getAsJsonObject();
      }
      JsonSchemaBasedFilter jsonSchemaBasedFilter = (JsonSchemaBasedFilter) rowFilter;
      return addDerivedFields(jsonSchemaBasedFilter != null ? jsonSchemaBasedFilter.filter(row) : row);
    } else {
      connection.closeStream();
      if (hasNextPage() && processInputStream(jsonExtractorKeys.getProcessedCount())) {
        return readRecord(reuse);
      }
    }
    if (!this.eof && extractorKeys.getExplictEof()) {
      eof = true;
      return EOF;
    }
    return (JsonObject) endProcessingAndValidateCount();
  }

  /**
   * This is the main method in this extractor, it extracts data from source and perform essential checks.
   *
   * @param starting [0, +INF), points to the last count of record processed, 0 means it's the first of a series of requests
   * @return true if Successful
   */
  @Override
  protected boolean processInputStream(long starting) {
    if (!super.processInputStream(starting)) {
      return false;
    }

    // if Content-Type is provided, but not application/json, the response can have
    // useful error information
    JsonObject expectedContentType = MSTAGE_HTTP_RESPONSE_TYPE.get(state);
    HashSet<String> expectedContentTypeSet = new LinkedHashSet<>(Collections.singletonList("application/json"));
    if (expectedContentType.has(CONTENT_TYPE_KEY)) {
      for (Map.Entry<String, JsonElement> entry: expectedContentType.entrySet()) {
        expectedContentTypeSet.add(entry.getValue().getAsString());
      }
    }
    if (!checkContentType(workUnitStatus, expectedContentTypeSet)) {
      return false;
    }

    JsonElement data;
    try {
      data = extractJson(workUnitStatus.getBuffer());
      // return false to stop the job under these situations
      if (data == null || data.isJsonNull() || data.isJsonPrimitive()) {
        return false;
      }
    } catch (Exception e) {
      LOG.error("Source Error: {}", e.getMessage());
      state.setWorkingState(WorkUnitState.WorkingState.FAILED);
      throw new RuntimeException(e);
    }

    LOG.debug("Checking parsed Json object");

    JsonArray coreData = new JsonArray();
    JsonElement payload;
    if (StringUtils.isNotBlank(jobKeys.getDataField())) {
      payload = JsonUtils.get(data.getAsJsonObject(), jobKeys.getDataField());
      if (payload.isJsonNull()) {
        LOG.info("Terminate the ingestion because no actual payload in the response");
        return false;
      }
    } else {
      payload = data;
    }

    if (payload.isJsonArray()) {
      coreData = payload.getAsJsonArray();
    } else {
      LOG.info("Payload is not a Json Array, therefore add the whole payload a one single entry");
      coreData.add(payload);
    }

    // get basic profile of the returned data
    jsonExtractorKeys.setTotalCount(getTotalCountValue(data));
    jsonExtractorKeys.setPushDowns(retrievePushDowns(data, jobKeys.getDerivedFields()));
    extractorKeys.setSessionKeyValue(retrieveSessionKeyValue(data));
    jsonExtractorKeys.setCurrentPageNumber(jsonExtractorKeys.getCurrentPageNumber() + 1);

    // get profile of the payload
    if (!jobKeys.hasOutputSchema() && starting == 0 && coreData.size() > 0) {
      JsonArray sample = new JsonArray();
      for (int i = 0; i < Long.min(coreData.size(), SCHEMA_INFER_MAX_SAMPLE_SIZE); i++) {
        sample.add(JsonUtils.deepCopy(coreData.get(i)));
      }
      extractorKeys.setInferredSchema(SchemaBuilder.fromJsonData(sample).buildAltSchema(
          jobKeys.getDefaultFieldTypes(),
          jobKeys.isEnableCleansing(),
          jobKeys.getSchemaCleansingPattern(),
          jobKeys.getSchemaCleansingReplacement(),
          jobKeys.getSchemaCleansingNullable()).getAsJsonArray());
    }

    // update work unit status for next Source call
    workUnitStatus.setSetCount(coreData.size());
    workUnitStatus.setTotalCount(jsonExtractorKeys.getTotalCount());
    workUnitStatus.setSessionKey(extractorKeys.getSessionKeyValue());
    updatePaginationStatus(data);

    jsonExtractorKeys.logDebugAll(state.getWorkunit());
    workUnitStatus.logDebugAll();
    extractorKeys.logDebugAll(state.getWorkunit());

    jsonExtractorKeys.setJsonElementIterator(coreData.getAsJsonArray().iterator());
    return coreData.getAsJsonArray().size() > 0;
  }

  /**
   * Process the derived field source to get intermediate value
   * @param row current row being processed
   * @param name derived field's name
   * @param derivedFieldDef map {type: type1, source: source1, format: format1}
   * @return String value of the derived field
   */
  private String processDerivedFieldSource(JsonObject row, String name, Map<String, String> derivedFieldDef) {
    String source = derivedFieldDef.getOrDefault("source", StringUtils.EMPTY);
    String inputValue = derivedFieldDef.getOrDefault("value", StringUtils.EMPTY);
    boolean isInputValueFromSource = false;

    // get the base value from the source row or push down if present
    if (jsonExtractorKeys.getPushDowns().entrySet().size() > 0 && jsonExtractorKeys.getPushDowns().has(name)) {
      inputValue = jsonExtractorKeys.getPushDowns().get(name).getAsString();
      isInputValueFromSource = true;
    } else if (isInputValueFromSource(source)) {
      JsonElement ele = JsonUtils.get(row, source);
      if (ele != null && !ele.isJsonNull()) {
        inputValue = ele.getAsString();
        isInputValueFromSource = true;
      }
    }

    return generateDerivedFieldValue(name, derivedFieldDef, inputValue, isInputValueFromSource);
  }

  /**
   * calculate and add derived fields
   * derivedFields map in this in structure {name1 : {type: type1, source: source1, format: format1}}
   * @param row original record
   * @return modified record
   */
  private JsonObject addDerivedFields(JsonObject row) {
    for (Map.Entry<String, Map<String, String>> derivedField : jobKeys.getDerivedFields().entrySet()) {
      String name = derivedField.getKey();
      Map<String, String> derivedFieldDef = derivedField.getValue();
      String strValue = processDerivedFieldSource(row, name, derivedFieldDef);
      String type = derivedField.getValue().get("type");
      switch (type) {
        case KEY_WORD_EPOC:
          if (strValue.length() > 0) {
            row.addProperty(name, Long.parseLong(strValue));
          }
          break;
        case KEY_WORD_STRING:
        case KEY_WORD_REGEXP:
          row.addProperty(name, strValue);
          break;
        case KEY_WORD_BOOLEAN:
          row.addProperty(name, Boolean.parseBoolean(strValue));
          break;
        case KEY_WORD_INT:
          row.addProperty(name, Integer.parseInt(strValue));
          break;
        case KEY_WORD_NUMBER:
          row.addProperty(name, Double.parseDouble(strValue));
          break;
        default:
          failWorkUnit("Unsupported type for derived fields: " + type);
          break;
      }
    }
    return row;
  }

  /**
   * Update pagination parameters
   * @param data response from the source, can be JsonArray or JsonObject
   */
  private Map<ParameterTypes, Long> getNextPaginationValues(JsonElement data) {
    Map<ParameterTypes, String> paginationKeys = jobKeys.getPaginationFields();
    Map<ParameterTypes, Long> paginationValues = new HashMap<>();

    if (data.isJsonObject()) {
      JsonElement pageStartElement = null;
      JsonElement pageSizeElement = null;
      JsonElement pageNumberElement = null;

      if (paginationKeys.containsKey(ParameterTypes.PAGESTART)) {
        pageStartElement = JsonUtils.get(data.getAsJsonObject(), paginationKeys.get(ParameterTypes.PAGESTART));
      } else {
        // update page start directly to rows processed as Next page start
        paginationValues.put(ParameterTypes.PAGESTART,
            jsonExtractorKeys.getProcessedCount() + workUnitStatus.getSetCount());
      }

      if (paginationKeys.containsKey(ParameterTypes.PAGESIZE)) {
        pageSizeElement = JsonUtils.get(data.getAsJsonObject(), paginationKeys.get(ParameterTypes.PAGESIZE));
      } else {
        paginationValues.put(ParameterTypes.PAGESIZE,
            jobKeys.getPaginationInitValues().getOrDefault(ParameterTypes.PAGESIZE, 0L));
      }

      if (paginationKeys.containsKey(ParameterTypes.PAGENO)) {
        pageNumberElement = JsonUtils.get(data.getAsJsonObject(), paginationKeys.get(ParameterTypes.PAGENO));
      } else {
        paginationValues.put(ParameterTypes.PAGENO, jsonExtractorKeys.getCurrentPageNumber());
      }

      if (pageStartElement != null && pageSizeElement != null && !pageStartElement.isJsonNull()
          && !pageSizeElement.isJsonNull()) {
        paginationValues.put(ParameterTypes.PAGESTART, pageStartElement.getAsLong() + pageSizeElement.getAsLong());
        paginationValues.put(ParameterTypes.PAGESIZE, pageSizeElement.getAsLong());
      }
      if (pageNumberElement != null && !pageNumberElement.isJsonNull()) {
        paginationValues.put(ParameterTypes.PAGENO, pageNumberElement.getAsLong() + 1);
      }
    } else if (data.isJsonArray()) {
      paginationValues.put(ParameterTypes.PAGESTART,
          jsonExtractorKeys.getProcessedCount() + data.getAsJsonArray().size());
      paginationValues.put(ParameterTypes.PAGESIZE,
          jobKeys.getPaginationInitValues().getOrDefault(ParameterTypes.PAGESIZE, 0L));
      paginationValues.put(ParameterTypes.PAGENO, jsonExtractorKeys.getCurrentPageNumber());
    }
    return paginationValues;
  }

  /**
   * retrieveSessionKey() parses the response JSON and extract the session key value
   *
   * @param input the Json payload
   * @return the session key if the property is available
   */
  private String retrieveSessionKeyValue(JsonElement input) {
    if (jobKeys.getSessionKeyField().entrySet().size() == 0) {
      return StringUtils.EMPTY;
    }

    if (input.isJsonArray()) {
      String fld = jobKeys.getSessionKeyField().get("name").getAsString();
      List<String> valueList = Lists.newArrayList();
      for (JsonElement v: input.getAsJsonArray()) {
        if (v.isJsonObject()) {
          JsonElement fldValue = JsonUtils.get(v.getAsJsonObject(), fld);
          if (fldValue.isJsonPrimitive()) {
            valueList.add(fldValue.getAsString());
          } else {
            valueList.add(fldValue.toString());
          }
        }
      }
      return Joiner.on("|").join(valueList);
    }

    if (!input.isJsonObject()) {
      return StringUtils.EMPTY;
    }

    JsonObject data = input.getAsJsonObject();

    Iterator<String> members = Splitter.on(JSON_MEMBER_SEPARATOR)
        .omitEmptyStrings()
        .trimResults()
        .split(jobKeys.getSessionKeyField().get("name").getAsString())
        .iterator();

    JsonElement e = data;
    while (members.hasNext()) {
      String member = members.next();
      if (e.getAsJsonObject().has(member)) {
        e = e.getAsJsonObject().get(member);
        if (!members.hasNext()) {
          return e.isJsonNull() ? "null" : e.getAsString();
        }
      }
    }
    return extractorKeys.getSessionKeyValue() == null ? StringUtils.EMPTY : extractorKeys.getSessionKeyValue();
  }

  /**
   *
   * Retrieves the total row count member if it is expected. Without a total row count,
   * this request is considered completed after the first call or when the session
   * completion criteria is met, see {@link JsonExtractor#readRecord(JsonObject)} ()}
   *
   * @param data HTTP response JSON
   * @return the expected total record count if the property is available
   */
  private Long getTotalCountValue(JsonElement data) {
    if (StringUtils.isBlank(jobKeys.getTotalCountField())) {
      if (data.isJsonObject()) {
        if (StringUtils.isNotBlank(jobKeys.getDataField())) {
          JsonElement payload = JsonUtils.get(data.getAsJsonObject(), jobKeys.getDataField());
          if (payload.isJsonNull()) {
            LOG.info("Expected payload at JsonPath={} doesn't exist", jobKeys.getDataField());
            return jsonExtractorKeys.getTotalCount();
          } else if (payload.isJsonArray()) {
            return jsonExtractorKeys.getTotalCount() + payload.getAsJsonArray().size();
          } else if (payload.isJsonObject()) {
            return jsonExtractorKeys.getTotalCount() + 1;
          } else {
            throw new RuntimeException("Unsupported payload type: only JsonArray or JsonObject is supported");
          }
        } else {
          // no total count field and no data field
          return jsonExtractorKeys.getTotalCount() + 1;
        }
      } else if (data.isJsonArray()) {
        return jsonExtractorKeys.getTotalCount() + data.getAsJsonArray().size();
      } else {
        return jsonExtractorKeys.getTotalCount();
      }
    }

    Iterator<String> members = Splitter.on(JSON_MEMBER_SEPARATOR)
        .omitEmptyStrings()
        .trimResults()
        .split(jobKeys.getTotalCountField())
        .iterator();

    JsonElement e = data;
    while (members.hasNext()) {
      String member = members.next();
      if (e.getAsJsonObject().has(member)) {
        e = e.getAsJsonObject().get(member);
        if (!members.hasNext()) {
          return e.getAsLong();
        }
      }
    }
    return jsonExtractorKeys.getTotalCount();
  }

  private void updatePaginationStatus(JsonElement data) {
    // update work unit status, and get ready for next calls, these steps are possible only
    // when data is a JsonObject
    Map<ParameterTypes, Long> pagination = getNextPaginationValues(data);
    workUnitStatus.setPageStart(pagination.getOrDefault(ParameterTypes.PAGESTART, 0L));
    workUnitStatus.setPageSize(pagination.getOrDefault(ParameterTypes.PAGESIZE, 0L));
    workUnitStatus.setPageNumber(pagination.getOrDefault(ParameterTypes.PAGENO, 0L));
  }

  /**
   * Perform limited cleansing so that data can be processed by converters
   *
   * TODO: make a dummy value for Null values
   * @param input the input data to be cleansed
   * @return the cleansed data
   */
  private JsonElement limitedCleanse(JsonElement input) {
    JsonElement output;

    if (input.isJsonObject()) {
      output = new JsonObject();
      for (Map.Entry<String, JsonElement> entry : input.getAsJsonObject().entrySet()) {
        ((JsonObject) output).add(entry.getKey().replaceAll(jobKeys.getSchemaCleansingPattern(),
            jobKeys.getSchemaCleansingReplacement()), limitedCleanse(entry.getValue()));
      }
    } else if (input.isJsonArray()) {
      output = new JsonArray();
      for (JsonElement ele : input.getAsJsonArray()) {
        ((JsonArray) output).add(limitedCleanse(ele));
      }
    } else {
      output = input;
    }
    return output;
  }

  /**
   * Function which iterates through the fields in a row and encrypts the particular field defined in the
   * ms.encrypted.field property.
   * @param input parentKey, JsonElement
   *              parentKey -> holds the key name in case of nested structures
   *                e.g. settings.webprocessor (parentkey = settings)
   * @return row with the field encrypted through the Gobblin Utility
   */
  private JsonObject encryptJsonFields(String parentKey, JsonElement input) {
    JsonObject output = new JsonObject();
    JsonArray encryptionFields = jobKeys.getEncryptionField();
    for (Map.Entry<String, JsonElement> entry : input.getAsJsonObject().entrySet()) {
      JsonElement value = entry.getValue();
      String key = entry.getKey();
      // absolutekey holds the complete path of the key for matching with the encryptedfield
      String absoluteKey = (parentKey.length() == 0) ? key : (parentKey + "." + key);
      // this function assumes that the final value to be encrypted will always be a JsonPrimitive object and in case of
      // of JsonObject it will iterate recursively.
      if (value.isJsonPrimitive() && encryptionFields.contains(new JsonPrimitive(absoluteKey))) {
        String valStr = EncryptionUtils.encryptGobblin(value.isJsonNull() ? "" : value.getAsString(), state);
        output.add(key, new JsonPrimitive(valStr));
      } else if (value.isJsonObject()) {
        output.add(key, encryptJsonFields(absoluteKey, value));
      } else {
        output.add(key, value);
      }
    }
    return output;
  }

  /**
   * Save values that are not in the "data" payload, but will be used in de-normalization.
   * Values are saved by their derived field name.
   *
   * TODO: push down non-string values (low priority)
   *
   * @param response the Json response from source
   * @param derivedFields list of derived fields
   * @return list of values to be used in derived fields
   */
  private JsonObject retrievePushDowns(JsonElement response, Map<String, Map<String, String>> derivedFields) {
    if (response == null || response.isJsonNull() || response.isJsonArray()) {
      return new JsonObject();
    }
    JsonObject data = response.getAsJsonObject();
    JsonObject pushDowns = new JsonObject();
    for (Map.Entry<String, Map<String, String>> entry : derivedFields.entrySet()) {
      String source = entry.getValue().get("source");
      if (JsonUtils.has(data, source)) {
        pushDowns.addProperty(entry.getKey(), JsonUtils.get(data,source).getAsString());
        LOG.info("Identified push down value: {}", pushDowns);
      }
    }
    return pushDowns;
  }

  /**
   * Convert the input stream buffer to a Json object
   * @param input the InputStream buffer
   * @return a Json object of type JsonElement
   */
  private JsonElement extractJson(InputStream input) throws UnsupportedCharsetException {
    LOG.debug("Parsing response InputStream as Json");
    JsonElement data = null;
    if (input != null) {
      data = new JsonParser().parse(new InputStreamReader(input,
          Charset.forName(MSTAGE_SOURCE_DATA_CHARACTER_SET.get(state))));
      connection.closeStream();
    }
    return data;
  }

  /**
   *  Terminate the extraction if:
   *  1. total count has been initialized
   *  2. all expected rows are fetched
   *
   * @param starting the current position, or starting position of next request
   * @return true if all rows retrieve
   */
  @Override
  protected boolean isWorkUnitCompleted(long starting) {
    return super.isWorkUnitCompleted(starting) || starting != 0 && StringUtils.isNotBlank(jobKeys.getTotalCountField())
        && starting >= jsonExtractorKeys.getTotalCount();
  }

  /**
   * If the iterator is null, then it must be the first request
   * @param starting the starting position of the request
   * @return true if the iterator is null, otherwise false
   */
  @Override
  protected boolean isFirst(long starting) {
    return jsonExtractorKeys.getJsonElementIterator() == null;
  }

  /**
   * Add condition to allow total row count can be used to control pagination.
   *
   * @return true if a new page request is needed
   */
  @Override
  protected boolean hasNextPage() {
    return super.hasNextPage() || jsonExtractorKeys.getProcessedCount() < jsonExtractorKeys.getTotalCount();
  }
}

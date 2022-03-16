// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.extractor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.linkedin.cdi.configuration.StaticConstants;
import com.linkedin.cdi.keys.ExtractorKeys;
import com.linkedin.cdi.keys.JobKeys;
import com.linkedin.cdi.keys.JsonExtractorKeys;
import com.linkedin.cdi.util.JsonUtils;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gobblin.configuration.WorkUnitState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;


/**
 * TextExtractor takes an InputStream, applies proper preprocessors, and returns a String output
 */
public class TextExtractor extends MultistageExtractor<JsonArray, JsonObject> {
  private static final Logger LOG = LoggerFactory.getLogger(TextExtractor.class);

  private final static int TEXT_EXTRACTOR_BYTE_LIMIT = 1048576;
  private final static int BUFFER_SIZE = 8192;
  private final static String TEXT_EXTRACTOR_SCHEMA =
      "[{\"columnName\":\"output\",\"isNullable\":true,\"dataType\":{\"type\":\"string\"}}]";

  public JsonExtractorKeys getJsonExtractorKeys() {
    return jsonExtractorKeys;
  }

  private JsonExtractorKeys jsonExtractorKeys = new JsonExtractorKeys();

  public TextExtractor(WorkUnitState state, JobKeys jobKeys) {
    super(state, jobKeys);
    super.initialize(this.jsonExtractorKeys);
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
  protected void setFileDumpExtractorKeys(JsonExtractorKeys jsonExtractorKeys) {
    this.extractorKeys = jsonExtractorKeys;
    this.jsonExtractorKeys = jsonExtractorKeys;
  }

  /**
   * This method rely on the parent class to get a JsonArray formatted schema, and pass it out as
   * a string. Typically we expect the downstream is a CsvToJsonConverter.
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
    return schemaArray;
  }

  @Nullable
  @Override
  public JsonObject readRecord(JsonObject reuse) {
    if (this.jsonExtractorKeys.getTotalCount() == 1) {
      return null;
    }
    if (processInputStream(this.jsonExtractorKeys.getTotalCount())) {
      this.jsonExtractorKeys.setTotalCount(1);
      StringBuffer output = new StringBuffer();
      if (workUnitStatus.getBuffer() == null) {
        LOG.warn("Received a NULL InputStream, end the work unit");
        return null;
      } else {
        try {
          InputStream input = workUnitStatus.getBuffer();
          writeToStringBuffer(input, output);
          input.close();
          JsonObject jsonObject = new JsonObject();
          jsonObject.addProperty("output", output.toString());
          JsonObject outputJson = addDerivedFields(jsonObject);
          return outputJson;
        } catch (Exception e) {
          LOG.error("Error while extracting from source or writing to target", e);
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
        output.append(String.valueOf(buffer, 0, len));
        totalBytes += len;
        if (totalBytes > TEXT_EXTRACTOR_BYTE_LIMIT) {
          LOG.warn("Download limit of {} bytes reached for text extractor ", TEXT_EXTRACTOR_BYTE_LIMIT);
          break;
        }
      }
      is.close();
      LOG.info("TextExtractor: written {} bytes ", totalBytes);
    } catch (IOException e) {
      throw new RuntimeException("Unable to extract text in TextExtractor", e);
    }
  }
  private String processDerivedFieldSource(JsonObject row, String name, Map<String, String> derivedFieldDef) {
    String source = (String)derivedFieldDef.getOrDefault("source", "");
    String inputValue = (String)derivedFieldDef.getOrDefault("value", "");
    boolean isInputValueFromSource = false;
    if (this.jsonExtractorKeys.getPushDowns().entrySet().size() > 0 && this.jsonExtractorKeys.getPushDowns().has(name)) {
      inputValue = this.jsonExtractorKeys.getPushDowns().get(name).getAsString();
      isInputValueFromSource = true;
    } else if (this.isInputValueFromSource(source)) {
      JsonElement ele = JsonUtils.get(row, source);
      if (ele != null && !ele.isJsonNull()) {
        inputValue = ele.getAsString();
        isInputValueFromSource = true;
      }
    }

    return this.generateDerivedFieldValue(name, derivedFieldDef, inputValue, isInputValueFromSource);
  }

  private JsonObject addDerivedFields(JsonObject row) {
    Iterator var2 = this.jobKeys.getDerivedFields().entrySet().iterator();

    while(var2.hasNext()) {
      Map.Entry<String, Map<String, String>> derivedField = (Map.Entry)var2.next();
      String name = (String)derivedField.getKey();
      Map<String, String> derivedFieldDef = (Map)derivedField.getValue();
      String strValue = this.processDerivedFieldSource(row, name, derivedFieldDef);
      String type = (String)((Map)derivedField.getValue()).get("type");
      byte var9 = -1;
      switch(type.hashCode()) {
        case -1034364087:
          if (type.equals("number")) {
            var9 = 5;
          }
          break;
        case -934799095:
          if (type.equals("regexp")) {
            var9 = 2;
          }
          break;
        case -891985903:
          if (type.equals("string")) {
            var9 = 1;
          }
          break;
        case 3120063:
          if (type.equals("epoc")) {
            var9 = 0;
          }
          break;
        case 64711720:
          if (type.equals("boolean")) {
            var9 = 3;
          }
          break;
        case 1958052158:
          if (type.equals("integer")) {
            var9 = 4;
          }
      }

      switch(var9) {
        case 0:
          if (strValue.length() > 0) {
            row.addProperty(name, Long.parseLong(strValue));
          }
          break;
        case 1:
        case 2:
          row.addProperty(name, strValue);
          break;
        case 3:
          row.addProperty(name, Boolean.parseBoolean(strValue));
          break;
        case 4:
          row.addProperty(name, Integer.parseInt(strValue));
          break;
        case 5:
          row.addProperty(name, Double.parseDouble(strValue));
          break;
        default:
          this.failWorkUnit("Unsupported type for derived fields: " + type);
      }
    }

    return row;
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
}
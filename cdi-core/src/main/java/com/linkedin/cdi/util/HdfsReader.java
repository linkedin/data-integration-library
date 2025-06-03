// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.FsInput;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.source.extractor.filebased.TimestampAwareFileBasedHelper;
import org.apache.gobblin.source.extractor.hadoop.AvroFsHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.cdi.configuration.StaticConstants.*;


/**
 * This class is used to load data from HDFS based on location and fields of selection,
 * and it returns the results as JsonArray.
 *
 * The reader will read sub-directories within the given location recursively, and pick up
 * all files in AVRO format by default.
 *
 * All files in the location should have consistent format and contain the fields of selection.
 *
 * @author vbhrill chrli
 */

public class HdfsReader {
  private final static Logger LOG = LoggerFactory.getLogger(HdfsReader.class);
  private final Gson gson = new Gson();
  private final State state;

  private JsonArray transientInputPayload = new JsonArray();

  public HdfsReader(State state) {
    this.state = state;
  }

  public HdfsReader(State state, JsonArray secondaryInputs) {
    this.transientInputPayload = secondaryInputs;
    this.state = state;
  }

  @VisibleForTesting
  public List<String> getFieldsAsList(JsonElement field) {
    List<String> fieldsList = new ArrayList<>();
    if (field.getAsJsonObject().has("fields")) {
      Iterator<JsonElement> iterator = field.getAsJsonObject()
          .get("fields").getAsJsonArray().iterator();
      while (iterator.hasNext()) {
        fieldsList.add(iterator.next().getAsString());
      }
    }
    return fieldsList;
  }

  /**
   * Reads secondary input paths one by one and return the JsonArrays by category
   * @return a Map&lt;String, JsonArray&gt; structure for records by category
   */
  public Map<String, JsonArray> readAll() {
    if (transientInputPayload == null || transientInputPayload.size() == 0) {
      return new HashMap<>();
    }
    Map<String, JsonArray> secondaryInput = new HashMap<>();
    for (JsonElement input: transientInputPayload) {
      JsonArray transientData = new JsonArray();
      JsonElement path = input.getAsJsonObject().get("path");
      List<String> fieldList = getFieldsAsList(input);
      String category = input.getAsJsonObject().has(KEY_WORD_CATEGORY)
          ? input.getAsJsonObject().get(KEY_WORD_CATEGORY).getAsString()
          : KEY_WORD_ACTIVATION;
      if (path != null) {
        transientData.addAll(readRecordsFromPath(path.getAsString(), fieldList, getFilters(input)));
        if (secondaryInput.containsKey(category)) {
          transientData.addAll(secondaryInput.get(category));
        }
        secondaryInput.put(category, transientData);
      }
    }
    return secondaryInput;
  }

  /**
   * Reads a secondary input based on the entry specification. A secondary input
   * may include one or more files in the path.
   *
   * @param secondaryEntry one entry in the ms.secondary.input parameter
   * @return the data read
   */
  public JsonArray readSecondary(JsonObject secondaryEntry) {
    if (!secondaryEntry.has(("path"))) {
      return new JsonArray();
    }

    JsonArray secondaryData = new JsonArray();
    JsonElement path = secondaryEntry.get("path");
    List<String> fieldList = getFieldsAsList(secondaryEntry);
    secondaryData.addAll(readRecordsFromPath(path.getAsString(), fieldList, getFilters(secondaryEntry)));
    return secondaryData;
  }


  public JsonArray toJsonArray(String transientDataInputPayload) {
    try {
      return new Gson().fromJson(transientDataInputPayload, JsonArray.class);
    } catch (Exception e) {
      LOG.error("Error while processing transient input payload.");
      throw new RuntimeException("Error while processing transient input payload. Cannot convert into JsonArray.", e);
    }
  }

  private DataFileReader createDataReader(String path) {
    try {
      GenericDatumReader<GenericRecord> genericDatumReader = new GenericDatumReader<>();
      FsInput fsInput = new FsInput(new Path(path), new Configuration());
      return new DataFileReader(fsInput, genericDatumReader);
    } catch (Exception e) {
      throw new RuntimeException("Error initializing transient data reader", e);
    }
  }

  /**
   * process 1 secondary input path
   * @param inputLocation the secondary input path
   * @param fields the list of fields to be output
   * @param filters the list of filters to ber applied
   * @return a filter list of records
   */
  private JsonArray readRecordsFromPath(
      String inputLocation,
      List<String> fields,
      Map<String, String> filters) {
    JsonArray transientDataArray = new JsonArray();
    String sourceFileBasedFsUri = state.getProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI);
    TimestampAwareFileBasedHelper fsHelper = new AvroFsHelper(state);
    try {
      state.setProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, inputLocation);
      fsHelper.connect();
      List<String> filesToRead = fsHelper.ls(inputLocation);
      for (String singleFile: filesToRead) {
        DataFileReader reader = createDataReader(singleFile);
        transientDataArray.addAll(readFileAsJsonArray(reader, fields, filters));
      }
      return transientDataArray;
      } catch (Exception e) {
      throw new RuntimeException("Error while reading records from location " + inputLocation, e);
    } finally {
      if (sourceFileBasedFsUri != null) {
        state.setProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, sourceFileBasedFsUri);
      }
    }
  }

  /**
   * This method read 1 avro file and store the records in a JsonArray
   *
   * @param preparedReader The avro file reader
   * @param fields The list of fields to output
   * @param filters The filters to apply to each records
   * @return the filtered projection of the avro file in a JsonArray
   */
  private JsonArray readFileAsJsonArray(
      DataFileReader preparedReader,
      List<String> fields,
      Map<String, String> filters) {
    JsonArray transientDataArray = new JsonArray();
    while (preparedReader.hasNext()) {
      GenericRecord record = (GenericRecord) preparedReader.next();
      Schema schema = record.getSchema();
      boolean recordAccepted = true;
      for (Schema.Field field: schema.getFields()) {
        String name = field.name();
        String pattern = filters.getOrDefault(name, ".*");
        if (record.get(name) != null && !record.get(name).toString().matches(pattern)
          || filters.containsKey(name) && record.get(name) == null) {
          recordAccepted = false;
        }
      }
      if (recordAccepted) {
        transientDataArray.add(selectFieldsFromGenericRecord(record, fields));
      }
    }
    return transientDataArray;
  }

  @VisibleForTesting
  private JsonObject selectFieldsFromGenericRecord(GenericRecord record, List<String> fields) {
    JsonObject jsonObject = new JsonObject();
    for (String field: fields) {
      Object valueObject = record.get(field);
      Schema.Type fieldType = record.getSchema().getField(field).schema().getType();
      if (valueObject == null || fieldType == Schema.Type.NULL) {
        jsonObject.add(field, JsonNull.INSTANCE);
      } else if (fieldType == Schema.Type.STRING || fieldType == Schema.Type.UNION) {
        jsonObject.addProperty(field, EncryptionUtils.decryptGobblin(valueObject.toString(), state));
      } else if (fieldType == Schema.Type.ARRAY) {
        jsonObject.add(field, gson.fromJson(valueObject.toString(), JsonArray.class));
      } else if (fieldType == Schema.Type.RECORD) {
        jsonObject.add(field, gson.fromJson(valueObject.toString(), JsonObject.class));
      } else if (fieldType == Schema.Type.INT || fieldType == Schema.Type.LONG) {
        jsonObject.addProperty(field, Long.valueOf(valueObject.toString()));
      } else if (fieldType == Schema.Type.DOUBLE || fieldType == Schema.Type.FLOAT) {
        jsonObject.addProperty(field, Double.valueOf(valueObject.toString()));
      } else if (fieldType == Schema.Type.BOOLEAN) {
        jsonObject.addProperty(field, Boolean.valueOf(valueObject.toString()));
      } else {
        jsonObject.addProperty(field, valueObject.toString());
      }
    }
    return jsonObject;
  }

  /**
   * retrieve the filters from the secondary input definition
   *
   * @param field a single secondary input source
   * @return the filters defined as a map of FieldName: RegEx
   */
  @VisibleForTesting
  private Map<String, String> getFilters(JsonElement field) {
    Map<String, String> filtersMap = new HashMap<>();
    if (field.getAsJsonObject().has("filters")) {
      JsonObject filterDefinition = field.getAsJsonObject().get("filters").getAsJsonObject();
      for (Map.Entry<String, JsonElement> entry : filterDefinition.entrySet()) {
        filtersMap.put(entry.getKey(), entry.getValue().getAsString());
      }
    }
    return filtersMap;
  }
}

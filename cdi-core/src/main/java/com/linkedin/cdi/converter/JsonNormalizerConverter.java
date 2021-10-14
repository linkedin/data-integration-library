// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.converter;

import com.google.common.base.Preconditions;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.linkedin.cdi.util.JsonUtils;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.Converter;
import org.apache.gobblin.converter.SingleRecordIterable;
import org.apache.gobblin.util.EmptyIterable;

import static com.linkedin.cdi.configuration.PropertyCollection.*;
import static com.linkedin.cdi.configuration.StaticConstants.*;


/**
 * JsonNormalizerConverter normalizes records in JsonObject format. The source
 * is expected to be an array of records, or JsonArray. And the converter is
 * fed with JsonObject objects one by one. And at the end, an explicit EOF is
 * expected so that the Normalizer can write the normalized data to downstream.
 *
 * The converter depends on Target Schema, which can share some common fields with
 * the source (input) schema, and one (1) Normalized Field, which is the first
 * field in the target (output) schema but not in the source schema. All common fields
 * between source schema and target schema will have values copied from the very first
 * record of a conversion series.
 *
 * At the end of a conversion series, the normalized records are added to output record
 * as an JsonArray under the Normalized Field. The normalized records include all
 * fields that are not in Target Schema.
 *
 * We can control the batch size of normalized record using ms.normalizer.max.records.per.batch.
 * By default the batch size is 500.
 */
public class JsonNormalizerConverter extends Converter<JsonArray, JsonArray, JsonObject, JsonObject> {
  final private Set<String> outputFields = new HashSet<>();
  private JsonArray normalized = new JsonArray();
  private String normalizedField;
  private JsonObject firstRecord;
  private long maxRecordsPerBatch;
  private JsonArray targetSchema;

  @Override
  public Converter<JsonArray, JsonArray, JsonObject, JsonObject> init(WorkUnitState workUnit) {
    maxRecordsPerBatch = MSTAGE_NORMALIZER_BATCH_SIZE.get(workUnit);
    targetSchema = MSTAGE_TARGET_SCHEMA.get(workUnit);
    return this;
  }

  @Override
  public JsonArray convertSchema(JsonArray inputSchema, WorkUnitState workUnit) {
    for (JsonElement element : targetSchema) {
      String columnName = element.getAsJsonObject().get(KEY_WORD_COLUMN_NAME).getAsString();
      boolean isNullable = element.getAsJsonObject().get(KEY_WORD_IS_NULLABLE).getAsBoolean();
      outputFields.add(columnName);
      if (normalizedField == null && !schemaSearch(inputSchema, columnName) && !isNullable) {
        normalizedField = columnName;
      }
    }

    Preconditions.checkNotNull(normalizedField, "Normalized field is NULL.");
    JsonObject dataType = JsonUtils.get(KEY_WORD_COLUMN_NAME,
        normalizedField, KEY_WORD_DATA_TYPE, targetSchema).getAsJsonObject();
    String trueType = JsonUtils.get(KEY_WORD_TYPE, dataType).getAsString();
    JsonElement values = JsonUtils.get(KEY_WORD_VALUES, dataType);
    if (trueType.equalsIgnoreCase(KEY_WORD_RECORD) && values.isJsonNull()) {
      values = new JsonArray();
      for (JsonElement element: inputSchema) {
        String columnName = element.getAsJsonObject().get(KEY_WORD_COLUMN_NAME).getAsString();
        if (!schemaSearch(targetSchema, columnName)) {
          values.getAsJsonArray().add(element);
        }
      }
      dataType.add(KEY_WORD_VALUES, values);
    }
    return targetSchema;
  }

  @Override
  public Iterable<JsonObject> convertRecord(JsonArray outputSchema, JsonObject inputRecord, WorkUnitState workUnit) {
    if (inputRecord.has(KEY_WORD_EOF) && inputRecord.get(KEY_WORD_EOF).getAsString().equals(KEY_WORD_EOF)) {
      // only output when there's at least one record
      return outputIterable(1);
    }
    // note: the common fields within each batch will have the same value, so we only need to retain one record
    if (firstRecord == null) {
      firstRecord = inputRecord;
    }
    normalized.add(getNormalizingFields(inputRecord));
    return outputIterable(maxRecordsPerBatch);
  }

  /**
   * Output a single record iterable when the size of the normalized array has reached the threshold
   * and empty iterable otherwise.
   * @param threshold the threshold to output
   * @return iterable of JsonObject
   */
  private Iterable<JsonObject> outputIterable(long threshold) {
    if (normalized.size() >= threshold) {
      return new SingleRecordIterable<>(buildNormalizedRecord());
    } else {
      return new EmptyIterable<>();
    }
  }

  /**
   * Distill those fields that are to be normalized
   * @param record the input record
   * @return the part of fields to be normalized
   */
  private JsonObject getNormalizingFields(JsonObject record) {
    JsonObject newRecord = new JsonObject();
    for (Map.Entry<String, JsonElement> entry : record.entrySet()) {
      if (!outputFields.contains(entry.getKey())) {
        newRecord.add(entry.getKey(), entry.getValue());
      }
    }
    return newRecord;
  }

  /**
   * Build a final normalized record
   * @return the normalized record
   */
  private JsonObject buildNormalizedRecord() {
    JsonObject newRecord = new JsonObject();
    for (Map.Entry<String, JsonElement> entry : firstRecord.entrySet()) {
      if (outputFields.contains(entry.getKey())) {
        newRecord.add(entry.getKey(), entry.getValue());
      }
    }

    String columnType = JsonUtils.get(KEY_WORD_COLUMN_NAME,
        normalizedField, KEY_WORD_DATA_TYPE_TYPE, targetSchema).getAsString();
    // filter out null values for map type
    if (columnType.equalsIgnoreCase(KEY_WORD_MAP)) {
      newRecord.add(normalizedField, JsonUtils.filterNull(normalized.get(0).getAsJsonObject()));
    } else if (columnType.equalsIgnoreCase(KEY_WORD_RECORD)) {
      newRecord.add(normalizedField, normalized.get(0));
    } else {
      newRecord.add(normalizedField, normalized);
    }
    // reset the buffer and first record
    normalized = new JsonArray();
    firstRecord = null;
    return newRecord;
  }

  /**
   * search if a given column is in the schema
   * @param schema the schema in JasonArray format
   * @param name the column name
   * @return true if the column is in the schema, otherwise false
   */
  private boolean schemaSearch(JsonArray schema, String name) {
    for (JsonElement element : schema) {
      if (!element.isJsonObject()) {
        return false;
      }
      if (element.getAsJsonObject().get(KEY_WORD_COLUMN_NAME).getAsString().equals(name)) {
        return true;
      }
    }
    return false;
  }
}

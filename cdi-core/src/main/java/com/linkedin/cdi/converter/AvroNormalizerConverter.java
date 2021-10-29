// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.converter;

import com.google.common.base.Optional;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.linkedin.cdi.util.AvroSchemaUtils;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.Converter;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.converter.SingleRecordIterable;
import org.apache.gobblin.util.AvroUtils;
import org.apache.gobblin.util.EmptyIterable;

import static com.linkedin.cdi.configuration.PropertyCollection.*;
import static com.linkedin.cdi.configuration.StaticConstants.*;


/**
 * AvroNormalizerConverter normalizes records in GenericRecord format. The source
 * is expected to be an array of GenericRecord. And the converter is
 * fed with GenericRecord one by one. And at the end, an explicit EOF is
 * expected so that the Normalizer can write the normalized data to downstream.
 *
 * The converter depends on Target Schema, which can share some common fields with
 * the source (input) schema, and one (1) Normalized Field, which is the first
 * field in the target (output) schema but not in the source schema. All common fields
 * between source schema and target schema will have values copied from the very first
 * record of a conversion series.
 *
 * At the end of a conversion series, the normalized records are added to output record
 * as an Avro array under the Normalized Field. The normalized records include all
 * fields that are not in Target Schema.
 *
 * We can control the batch size of normalized record using ms.normalizer.max.records.per.batch.
 * By default the batch size is 500.
 */
public class AvroNormalizerConverter extends Converter<Schema, Schema, GenericRecord, GenericRecord> {
  final private Set<String> outputFields = new HashSet<>();
  private GenericData.Array<GenericRecord> normalized;
  private String normalizedField;
  private int maxRecordsPerBatch;
  private JsonArray targetSchema;
  private GenericRecord firstRecord;
  // schema of a normalizing field record, which are fields not in the output schema
  // and need to be pushed into the normalized field
  private Schema normalizingFieldsRecordSchema;
  // schema of the array containing all normalizing field records, i.e. array[GenericRecord]
  private Schema normalizingFieldsArraySchema;
  // schema of the final normalized record, e.g. {asIs:string, normalized:array[GenericRecord]}
  private Schema normalizedRecordSchema;
  private boolean haveIntermediateSchemas = false;


  @Override
  public Converter<Schema, Schema, GenericRecord, GenericRecord> init(WorkUnitState workUnit) {
    // Avro Array's max capacity is max int. In case of overflow, use the default value 500.
    try {
      maxRecordsPerBatch =
          Math.toIntExact(MSTAGE_NORMALIZER_BATCH_SIZE.get(workUnit));
    } catch (ArithmeticException e) {
      maxRecordsPerBatch = 500;
    }

    targetSchema = MSTAGE_TARGET_SCHEMA.get(workUnit);
    return this;
  }

  @Override
  public Schema convertSchema(Schema schema, WorkUnitState workUnitState) throws SchemaConversionException {
    Schema finalSchema = null;
    for (JsonElement element : targetSchema) {
      String columnName = element.getAsJsonObject().get(KEY_WORD_COLUMN_NAME).getAsString();
      outputFields.add(columnName);
      if (normalizedField == null && !schemaSearch(schema, columnName)) {
        normalizedField = columnName;
      }
    }

    if (!haveIntermediateSchemas) {
      buildIntermediateSchemas(schema);
    }

    finalSchema = AvroSchemaUtils.fromJsonSchema(targetSchema, workUnitState);
    return finalSchema;
  }

  @Override
  public Iterable<GenericRecord> convertRecord(Schema schema, GenericRecord inputRecord, WorkUnitState workUnitState) {
    Optional<Object> eof = AvroUtils.getFieldValue(inputRecord, KEY_WORD_EOF);
    if (eof.isPresent() && eof.get().toString().equals(KEY_WORD_EOF)) {
      // only output when there's at least one record
      return outputIterable(1);
    }
    // note: the common fields among records will have the same value, so we only need to retain one record
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
   * @return iterable of generic record
   */
  private Iterable<GenericRecord> outputIterable(int threshold) {
    if (normalized.size() >= threshold) {
      return new SingleRecordIterable<>(buildNormalizedRecord());
    } else {
      return new EmptyIterable<>();
    }
  }

  /**
   * Utility method to build all intermediate schemas
   * @param schema inputSchema
   */
  private void buildIntermediateSchemas(Schema schema) {
    // build normalizing fields' schema
    normalizingFieldsRecordSchema =
        Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), false);
    // build normalized record's schema
    normalizedRecordSchema = Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), false);

    // populate fields for the record containing normalized fields and the normalized record
    List<Schema.Field> normalizingFields = new ArrayList<>();
    List<Schema.Field> normalizedRecordsFields = new ArrayList<>();
    for (Schema.Field field : schema.getFields()) {
      if (outputFields.contains(field.name())) {
        normalizedRecordsFields.add(AvroSchemaUtils.deepCopySchemaField(field));
      } else {
        normalizingFields.add(AvroSchemaUtils.deepCopySchemaField(field));
      }
    }
    normalizingFieldsRecordSchema.setFields(normalizingFields);
    // create the normalized field array
    normalizingFieldsArraySchema = Schema.createArray(normalizingFieldsRecordSchema);
    // add the normalized field to final schema
    normalizedRecordsFields.add(
        new Schema.Field(normalizedField, normalizingFieldsArraySchema, normalizedField, null));
    normalizedRecordSchema.setFields(normalizedRecordsFields);
    normalized = new GenericData.Array<>(maxRecordsPerBatch, normalizingFieldsArraySchema);

    haveIntermediateSchemas = true;
  }

  /**
   * Distill those fields that are to be normalized
   * @param inputRecord the input record
   * @return the part of fields to be normalized
   */
  private GenericRecord getNormalizingFields(GenericRecord inputRecord) {
    GenericRecord normalizingFieldsRecord = new GenericData.Record(normalizingFieldsRecordSchema);
    // copy values from input record to normalizing fields record
    // fields not found in the input record are padded with null
    for (String fieldName : AvroSchemaUtils.getSchemaFieldNames(normalizingFieldsRecordSchema)) {
      Optional<Object> fieldValue = AvroUtils.getFieldValue(inputRecord, fieldName);
      normalizingFieldsRecord.put(fieldName, fieldValue.isPresent() ? fieldValue.get() : null);
    }
    return normalizingFieldsRecord;
  }

  /**
   * Build a final normalized record
   * @return the normalized record
   */
  private GenericRecord buildNormalizedRecord() {
    GenericRecord normalizedRecord = new GenericData.Record(normalizedRecordSchema);
    for (String fieldName : AvroSchemaUtils.getSchemaFieldNames(firstRecord.getSchema())) {
      if (outputFields.contains(fieldName)) {
        Optional<Object> fieldValue = AvroUtils.getFieldValue(firstRecord, fieldName);
        normalizedRecord.put(fieldName, fieldValue.isPresent() ? fieldValue.get() : null);
      }
    }
    normalizedRecord.put(normalizedField, normalized);
    // reset the buffer
    normalized = new GenericData.Array<>(maxRecordsPerBatch, normalizingFieldsArraySchema);
    return normalizedRecord;
  }

  /**
   * search if a given column is in the schema
   * @param schema the schema in Avro Schema format
   * @param name the column name
   * @return true if the column is in the schema, otherwise false
   */
  private boolean schemaSearch(Schema schema, String name) {
    return AvroSchemaUtils.getSchemaFieldNames(schema).contains(name);
  }
}

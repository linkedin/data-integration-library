// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.extractor;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.JsonArray;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.avro.UnsupportedDateTypeException;
import com.linkedin.cdi.configuration.MultistageProperties;
import com.linkedin.cdi.filter.AvroSchemaBasedFilter;
import com.linkedin.cdi.keys.AvroExtractorKeys;
import com.linkedin.cdi.keys.ExtractorKeys;
import com.linkedin.cdi.keys.JobKeys;
import com.linkedin.cdi.util.AvroSchemaUtils;
import com.linkedin.cdi.util.JsonIntermediateSchema;
import com.linkedin.cdi.util.SchemaUtils;
import org.apache.gobblin.util.AvroUtils;
import org.testng.Assert;

import static org.apache.avro.Schema.Type.*;


/**
 * AvroExtractor reads Avro formatted files from HDFS locations.
 *
 * This extractor will output schema in Avro Schema format.
 *
 * The rows will be pass output to converters in the form of GenericRecord, which represent
 * rows.
 *
 * This extractor can be used to feed into a AvroToJsonConvertor to get json data in the end.
 *
 * @author esong
 */
@Slf4j
public class AvroExtractor extends MultistageExtractor<Schema, GenericRecord> {
  @Getter
  private AvroExtractorKeys avroExtractorKeys = new AvroExtractorKeys();

  public AvroExtractor(WorkUnitState state, JobKeys jobKeys) {
    super(state, jobKeys);
    super.initialize(avroExtractorKeys);
    initialize(avroExtractorKeys);
  }

  @Override
  protected void initialize(ExtractorKeys keys) {
    avroExtractorKeys.logUsage(state);
    avroExtractorKeys.logDebugAll(state.getWorkunit());
  }
    /**
     * Utility function to do a double assignment
     * @param avroExtractorKeys the extractor key
     */
  @VisibleForTesting
  protected void setAvroExtractorKeys(AvroExtractorKeys avroExtractorKeys) {
    this.extractorKeys = avroExtractorKeys;
    this.avroExtractorKeys = avroExtractorKeys;
  }

  /**
   * getSchema will be called by Gobblin to retrieve the schema of the output of this extract.
   * The returned schema will be used in subsequent converters. The alternative schema here suites
   * JsonIntermediateToAvroConverter. Future development can support other converter by making
   * the schema conversion configurable.
   *
   *
   * @return the schema of the extracted record set in AvroSchema
   */
  @SneakyThrows
  @Override
  public Schema getSchema() {
    Schema avroSchema;
    log.debug("Retrieving schema definition");
    if (this.jobKeys.hasOutputSchema()) {
      // take pre-defined fixed schema
      JsonArray schemaArray = jobKeys.getOutputSchema();
      setRowFilter(schemaArray);
      avroSchema = fromJsonSchema(schemaArray);
    } else {
      avroSchema = processInputStream(0) ? avroExtractorKeys.getAvroOutputSchema()
          : fromJsonSchema(createMinimumSchema());
    }
    Assert.assertNotNull(avroSchema);
    return addDerivedFieldsToSchema(avroSchema);
  }

  /**
   * Initialize row filter
   * @param schemaArray schema array
   */
  @Override
  protected void setRowFilter(JsonArray schemaArray) {
    if (rowFilter == null) {
      if (MultistageProperties.MSTAGE_ENABLE_SCHEMA_BASED_FILTERING.getValidNonblankWithDefault(state)) {
        rowFilter = new AvroSchemaBasedFilter(new JsonIntermediateSchema(jobKeys.getOutputSchema()),
            avroExtractorKeys, state);
      }
    }
  }

  /**
   * if pagination is not enabled, this method will iterate through the iterator and send records one
   * by one, each row formatted as a GenericRecord.
   *
   * if pagination is enabled, the method will try to get a new set of data from the Source after
   * the iterator is exhausted.
   *
   * @param reuse not used, just to match interface
   * @return a row of avro data in GenericRecord format
   */
  @Nullable
  @Override
  public GenericRecord readRecord(GenericRecord reuse) {
    if (avroExtractorKeys.getAvroRecordIterator() == null
        && !processInputStream(0)) {
      return null;
    }

    DataFileStream<GenericRecord> avroRecordIterator = avroExtractorKeys.getAvroRecordIterator();

    if (hasNext()) {
      avroExtractorKeys.incrProcessedCount();
      // update work unit status along the way, since we are using iterators
      workUnitStatus.setPageStart(avroExtractorKeys.getProcessedCount());
      workUnitStatus.setPageNumber(avroExtractorKeys.getCurrentPageNumber());
      GenericRecord row = avroRecordIterator.next();
      AvroSchemaBasedFilter avroSchemaBasedFilter = (AvroSchemaBasedFilter) rowFilter;
      if (avroSchemaBasedFilter != null) {
        row = avroSchemaBasedFilter.filter(row);
      }
      return addDerivedFields(row);
    }

    if (!this.eof && extractorKeys.getExplictEof()) {
      eof = true;
      return AvroSchemaUtils.createEOF(state);
    }
    return null;
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

    DataFileStream<GenericRecord> avroRecordIterator;
    try {
      avroRecordIterator = new DataFileStream<>(workUnitStatus.getBuffer(),
          new GenericDatumReader<>());
      avroExtractorKeys.setAvroRecordIterator(avroRecordIterator);
      // store the original schema for further processing
      if (hasNext() && avroExtractorKeys.getAvroOutputSchema() == null) {
        avroExtractorKeys.setAvroOutputSchema(avroRecordIterator.getSchema());
      }
      if (jobKeys.hasOutputSchema()) {
        List<String> schemaColumns = new ArrayList<>(new JsonIntermediateSchema(jobKeys.getOutputSchema())
            .getColumns().keySet());
        List<String> fieldNames = AvroSchemaUtils.getSchemaFieldNames(avroExtractorKeys.getAvroOutputSchema());
        avroExtractorKeys.setIsValidOutputSchema(SchemaUtils.isValidOutputSchema(schemaColumns, fieldNames));
      }
    } catch (Exception e) {
      log.error("Source Error: {}", e.getMessage());
      state.setWorkingState(WorkUnitState.WorkingState.FAILED);
      return false;
    }

    // return false to stop the job under these situations
    if (workUnitStatus.getBuffer() == null
        || avroExtractorKeys.getAvroRecordIterator() == null) {
      return false;
    }
    avroExtractorKeys.incrCurrentPageNumber();

    avroExtractorKeys.logDebugAll(state.getWorkunit());
    workUnitStatus.logDebugAll();
    extractorKeys.logDebugAll(state.getWorkunit());
    return hasNext();
  }

  /**
   * If the iterator is null, then it must be the first request
   * @param starting the starting position of the request
   * @return true if the iterator is null, otherwise false
   */
  @Override
  protected boolean isFirst(long starting) {
    return avroExtractorKeys.getAvroRecordIterator() == null;
  }

  /**
   * Helper function that indicates if there are any records left to read
   * @return true if there are more records and false otherwise
   */
  protected boolean hasNext() {
    DataFileStream<GenericRecord> avroRecordIterator = avroExtractorKeys.getAvroRecordIterator();
    return avroRecordIterator != null && avroRecordIterator.hasNext();
  }

  /**
   * Append the derived field definition to the output schema
   * @param schema current schema
   * @return modified schema
   */
  private Schema addDerivedFieldsToSchema(Schema schema) {
    Set<Map.Entry<String, Map<String, String>>> derivedFields = jobKeys.getDerivedFields().entrySet();
    if (derivedFields.size() == 0) {
      return schema;
    }
    // create the new schema with original fields and derived fields
    Schema newSchema = Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), false);
    List<Schema.Field> fields = AvroUtils.deepCopySchemaFields(schema);
    for (Map.Entry<String, Map<String, String>> derivedField: derivedFields) {
      String name = derivedField.getKey();
      String type = derivedField.getValue().get("type");
      switch (type) {
        case "epoc":
          fields.add(new Schema.Field(name, Schema.create(LONG), name, null));
          break;
        case "string":
        case "regexp":
          fields.add(new Schema.Field(name, Schema.create(STRING), name, null));
          break;
        case "boolean":
          fields.add(new Schema.Field(name, Schema.create(BOOLEAN), name, null));
          break;
        case "integer":
          fields.add(new Schema.Field(name, Schema.create(INT), name, null));
          break;
        case "number":
          fields.add(new Schema.Field(name, Schema.create(DOUBLE), name, null));
          break;
        default:
          failWorkUnit("Unsupported type for derived fields: " + type);
          break;
      }
    }
    // create a new record with the new schema
    newSchema.setFields(fields);
    return newSchema;
  }

  /**
   * calculate and add derived fields,
   * derivedFields map in this in structure {name1 : {type: type1, source: source1, format: format1}}
   * @param row original record
   * @return modified record
   */
  private GenericRecord addDerivedFields(GenericRecord row) {
    Set<Map.Entry<String, Map<String, String>>> derivedFields = jobKeys.getDerivedFields().entrySet();
    int numDerivedFields = derivedFields.size();
    if (numDerivedFields == 0) {
      return row;
    }
    Schema schema = row.getSchema();
    Schema newSchema = addDerivedFieldsToSchema(schema);
    // Create the new record and copy over old fields
    GenericRecord rowWithDerivedFields = new GenericData.Record(newSchema);
    schema.getFields().forEach(field -> {
      String fieldName = field.name();
      rowWithDerivedFields.put(fieldName, row.get(fieldName));
    });
    // process derived fields and add to the new record
    for (Map.Entry<String, Map<String, String>> derivedField: derivedFields) {
      String name = derivedField.getKey();
      Map<String, String> derivedFieldDef = derivedField.getValue();
      String strValue = processDerivedFieldSource(row, derivedFieldDef);
      String type = derivedField.getValue().get("type");
      switch (type) {
        case "epoc":
          if (strValue.length() > 0) {
            rowWithDerivedFields.put(name, Long.parseLong(strValue));
          }
          break;
        case "string":
        case "regexp":
          rowWithDerivedFields.put(name, strValue);
          break;
        case "boolean":
          rowWithDerivedFields.put(name, Boolean.parseBoolean(strValue));
          break;
        case "integer":
          rowWithDerivedFields.put(name, Integer.parseInt(strValue));
          break;
        case "number":
          rowWithDerivedFields.put(name, Double.parseDouble(strValue));
          break;
        default:
          failWorkUnit("Unsupported type for derived fields: " + type);
          break;
      }

    }
    return rowWithDerivedFields;
  }

  /**
   * Process the derived field source to get intermediate value
   * @param row current row being processed
   * @param derivedFieldDef map {type: type1, source: source1, format: format1}
   * @return String value of the derived field
   */
  private String processDerivedFieldSource(GenericRecord row, Map<String, String> derivedFieldDef) {
    String source = derivedFieldDef.getOrDefault("source", StringUtils.EMPTY);
    String inputValue = derivedFieldDef.getOrDefault("value", StringUtils.EMPTY);
    boolean isInputValueFromSource = false;

    // get the base value from the source row if present
    if (isInputValueFromSource(source)) {
      Object ele = row.get(source);
      if (ele != null) {
        inputValue = ele.toString();
        isInputValueFromSource = true;
      }
    }

    return generateDerivedFieldValue(derivedFieldDef, inputValue, isInputValueFromSource);
  }

  /**
   * Utility method to convert JsonArray schema to avro schema
   * @param schema of JsonArray type
   * @return avro schema
   * @throws UnsupportedDateTypeException
   */
  private Schema fromJsonSchema(JsonArray schema) throws UnsupportedDateTypeException {
    return AvroSchemaUtils.fromJsonSchema(schema, state);
  }
}

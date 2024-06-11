// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.extractor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.gson.JsonArray;
import com.linkedin.cdi.filter.AvroSchemaBasedFilter;
import com.linkedin.cdi.keys.AvroExtractorKeys;
import com.linkedin.cdi.keys.ExtractorKeys;
import com.linkedin.cdi.keys.JobKeys;
import com.linkedin.cdi.util.AvroSchemaUtils;
import com.linkedin.cdi.util.JsonIntermediateSchema;
import com.linkedin.cdi.util.SchemaUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.util.AvroUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

import static com.linkedin.cdi.configuration.PropertyCollection.*;
import static com.linkedin.cdi.configuration.StaticConstants.*;
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
public class AvroExtractor extends MultistageExtractor<Schema, GenericRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(AvroExtractor.class);
  private AvroExtractorKeys avroExtractorKeys = new AvroExtractorKeys();

  public AvroExtractorKeys getAvroExtractorKeys() {
    return avroExtractorKeys;
  }

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
  @Override
  public Schema getSchema() {
    Schema avroSchema = null;
    LOG.debug("Retrieving schema definition");
    if (this.jobKeys.hasOutputSchema()) {
      // take pre-defined fixed schema
      JsonArray schemaArray = jobKeys.getOutputSchema();
      setRowFilter(schemaArray);
      avroSchema = fromJsonSchema(schemaArray);
    } else {
      avroSchema = processInputStream(0) ? avroExtractorKeys.getAvroOutputSchema()
          : createMinimumAvroSchema();
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
      if (MSTAGE_ENABLE_SCHEMA_BASED_FILTERING.get(state)) {
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
    super.readRecord(reuse);

    if (avroExtractorKeys.getAvroRecordIterator() == null
        && !processInputStream(0)) {
      return (GenericRecord) endProcessingAndValidateCount();
    }

    if (hasNext()) {
      avroExtractorKeys.incrProcessedCount();
      // update work unit status along the way, since we are using iterators
      workUnitStatus.setPageStart(avroExtractorKeys.getProcessedCount());
      workUnitStatus.setPageNumber(avroExtractorKeys.getCurrentPageNumber());
      GenericRecord row = extractDataField(getNext());
      AvroSchemaBasedFilter avroSchemaBasedFilter = (AvroSchemaBasedFilter) rowFilter;
      if (avroSchemaBasedFilter != null) {
        row = avroSchemaBasedFilter.filter(row);
      }
      return addDerivedFields(row);
    } else {
      connection.closeStream();
      if (hasNextPage() && processInputStream(avroExtractorKeys.getProcessedCount())) {
        return readRecord(reuse);
      }
    }

    if (!this.eof && extractorKeys.getExplictEof()) {
      eof = true;
      return AvroSchemaUtils.createEOF(state);
    }
    return (GenericRecord) endProcessingAndValidateCount();
  }

  /**
   * This is the main method in this extractor, it extracts data from source and perform essential checks.
   *
   * @param starting [0, +INF), points to the last count of record processed, 0 means it's the first of a series of requests
   * @return true if Successful
   */
  @Override
  // Suppressing un-checked casting warning when casting GenericData.Array<GenericRecord>
  // The casting is thoroughly checked by the isArrayOfRecord method, but it does not get rid of the warning
  protected boolean processInputStream(long starting) {
    if (!super.processInputStream(starting)) {
      return false;
    }

    // returning false to end the work unit if the buffer is null
    if (workUnitStatus.getBuffer() == null) {
      return false;
    }

    DataFileStream<GenericRecord> avroRecordIterator;
    try {
      avroRecordIterator = new DataFileStream<>(workUnitStatus.getBuffer(),
          new GenericDatumReader<>());

      avroExtractorKeys.setAvroRecordIterator(avroRecordIterator);
      // save one record to infer the avro schema from data
      if (hasNext() && avroExtractorKeys.getAvroOutputSchema() == null) {
        GenericRecord sampleData = avroRecordIterator.next();
        avroExtractorKeys.setSampleData(AvroSchemaUtils.deepCopy(sampleData.getSchema(), sampleData));
        avroExtractorKeys.setAvroOutputSchema(getAvroSchemaFromData(sampleData));
      }
      if (jobKeys.hasOutputSchema()) {
        List<String> schemaColumns = new ArrayList<>(new JsonIntermediateSchema(jobKeys.getOutputSchema())
            .getColumns().keySet());
        List<String> fieldNames = AvroSchemaUtils.getSchemaFieldNames(avroExtractorKeys.getAvroOutputSchema());
        avroExtractorKeys.setIsValidOutputSchema(
            SchemaUtils.isValidSchemaDefinition(schemaColumns, fieldNames, jobKeys.getDerivedFields().size()));
      }
    } catch (Exception e) {
      LOG.error("Source Error: ", e);
      state.setWorkingState(WorkUnitState.WorkingState.FAILED);
      return false;
    }

    // return false to stop the job under these situations
    if (avroExtractorKeys.getAvroRecordIterator() == null) {
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
   * Helper function that checks if there are sample data or more records in the iterator
   * @return true if there are more records and false otherwise
   */
  protected boolean hasNext() {
    DataFileStream<GenericRecord> avroRecordIterator = avroExtractorKeys.getAvroRecordIterator();
    return avroExtractorKeys.getSampleData() != null || hasNext(avroRecordIterator);
  }

  /**
   * Helper function that indicates if there are any records left to read in the iterator
   * @return true if there are more records and false otherwise
   */
  private boolean hasNext(DataFileStream<GenericRecord> avroRecordIterator) {
    return avroRecordIterator != null && avroRecordIterator.hasNext();
  }

  /**
   * Helper function to get the next record either from sample data or the iterator
   * Should only calls this after {@link #hasNext()}
   * @return next avro record
   */
  private GenericRecord getNext() {
    GenericRecord sampleData = avroExtractorKeys.getSampleData();

    if (sampleData != null) {
      avroExtractorKeys.setSampleData(null);
      return sampleData;
    } else {
      DataFileStream<GenericRecord> avroRecordExtractor = avroExtractorKeys.getAvroRecordIterator();
      return avroRecordExtractor.hasNext() ? avroRecordExtractor.next() : null;
    }
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
      String type = derivedField.getValue().get(KEY_WORD_TYPE);
      switch (type) {
        case KEY_WORD_EPOC:
          fields.add(new Schema.Field(name, Schema.create(LONG), name, null));
          break;
        case KEY_WORD_STRING:
        case KEY_WORD_REGEXP:
          fields.add(new Schema.Field(name, Schema.create(STRING), name, null));
          break;
        case KEY_WORD_BOOLEAN:
          fields.add(new Schema.Field(name, Schema.create(BOOLEAN), name, null));
          break;
        case KEY_WORD_INTEGER:
          fields.add(new Schema.Field(name, Schema.create(INT), name, null));
          break;
        case KEY_WORD_NUMBER:
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
      String strValue = processDerivedFieldSource(row, name, derivedFieldDef);
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
  private String processDerivedFieldSource(GenericRecord row, String name, Map<String, String> derivedFieldDef) {
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

    return generateDerivedFieldValue(name, derivedFieldDef, inputValue, isInputValueFromSource);
  }

  /**
   * Utility method to convert JsonArray schema to avro schema
   * @param schema of JsonArray type
   * @return avro schema
   */
  private Schema fromJsonSchema(JsonArray schema){
    return AvroSchemaUtils.fromJsonSchema(schema, state);
  }

  /**
   * get the avro schema of the data
   * @param sampleData a single record
   * @return avro schema
   */
  private Schema getAvroSchemaFromData(GenericRecord sampleData) {
    Schema sampleDataSchema = sampleData.getSchema();
    String dataFieldPath = jobKeys.getDataField();
    if (StringUtils.isBlank(dataFieldPath)) {
      return sampleDataSchema;
    }
    return createDataFieldRecordSchema(sampleData, dataFieldPath);
  }

  /**
   * Extract the data field from the current row
   * @param row the original data
   * @return a GenericRecord containing the data field
   */
  private GenericRecord extractDataField(GenericRecord row) {
    String dataFieldPath = jobKeys.getDataField();
    if (StringUtils.isBlank(dataFieldPath)) {
      return row;
    }
    // if the data field is not present, the schema will be null and the work unit will fail
    Schema dataFieldRecordSchema = createDataFieldRecordSchema(row, dataFieldPath);
    GenericRecord dataFieldRecord = new GenericData.Record(dataFieldRecordSchema);
    // the value should be present here otherwise the work unit would've have failed
    Object dataFieldValue = AvroUtils.getFieldValue(row, dataFieldPath).get();
    dataFieldRecord.put(extractDataFieldName(dataFieldPath), dataFieldValue);
    return dataFieldRecord;
  }

  /**
   * create the schema of the record that wraps the data field
   * @param data original record
   * @param dataFieldPath path to data field
   * @return avro schema of the wrapping record
   */
  private Schema createDataFieldRecordSchema(GenericRecord data, String dataFieldPath) {
    Schema rowSchema = data.getSchema();
    Optional<Object> fieldValue = AvroUtils.getFieldValue(data, dataFieldPath);
    Schema dataFieldSchema;
    if (fieldValue.isPresent()) {
      Object dataFieldValue = fieldValue.get();
      if (isArrayOfRecord(dataFieldValue)) {
        dataFieldSchema = ((GenericData.Array<GenericRecord>) dataFieldValue).getSchema();
      } else {
        // no need for isPresent check here since the value already exists
        dataFieldSchema = AvroUtils.getFieldSchema(rowSchema, dataFieldPath).get();
      }
      String dataFieldName = extractDataFieldName(dataFieldPath);
      Schema schema = Schema.createRecord(rowSchema.getName(), rowSchema.getDoc(),
          rowSchema.getNamespace(), false);
      List<Schema.Field> schemaFields = new ArrayList<>();
      schemaFields.add(new Schema.Field(dataFieldName, dataFieldSchema, dataFieldSchema.getDoc(), null));
      schema.setFields(schemaFields);
      return schema;
    } else {
      failWorkUnit("Terminate the ingestion because the data.field cannot be found");
      return null;
    }
  }

  /**
   * Use the last value in path as the name of the field. For example, for field1.nestedField1
   * this will return nestedField1.
   * @param dataFieldPath path to the data field
   * @return name of the data field
   */
  private String extractDataFieldName(String dataFieldPath) {
    String[] pathArray = dataFieldPath.split("\\.");
    return pathArray[pathArray.length - 1];
  }

  /**
   * Check if the object is of type GenericData.Array<GenericRecord>
   * Also allowing UNION, as the inner records' type could be UNION
   * @param payload an object
   * @return true if the type is correct and false otherwise
   */
  private boolean isArrayOfRecord(Object payload) {
    if (payload instanceof GenericData.Array<?>) {
      Schema.Type arrayElementType = ((GenericData.Array<?>) payload).getSchema().getElementType().getType();
      return arrayElementType == RECORD || arrayElementType == UNION;
    }
    return false;
  }

  /**
   * Create a minimum avro schema
   * @return avro schema
   */
  private Schema createMinimumAvroSchema() {
    return fromJsonSchema(createMinimumSchema());
  }
}

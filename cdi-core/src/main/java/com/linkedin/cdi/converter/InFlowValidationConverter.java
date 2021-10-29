// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.converter;

import com.google.common.base.Optional;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.linkedin.cdi.util.HdfsReader;
import com.linkedin.cdi.util.JsonUtils;
import java.text.DecimalFormat;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.Converter;
import org.apache.gobblin.util.AvroUtils;
import org.apache.gobblin.util.EmptyIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.cdi.configuration.PropertyCollection.*;
import static com.linkedin.cdi.configuration.StaticConstants.*;


/**
 * This converter does basic count validation based on the Failure Records or Success Records criteria.
 *
 * To use this converter for validation, the main source should be the dataset to be validated,
 * and the secondary input should be the base dataset to validate against.
 *
 * The base dataset can be in a nested column of the secondary input, i.e. a field, which can be
 * retrieved through a JSON path, contains the actual base records.
 *
 * Currently following rules are defined:
 *
 *  fail (upper bound rule): the source should be failed records
 *    Job succeeds when the row count in validation set / row count in base set < threshold
 *    Job fails when the row count in validation set / row count in base set >= threshold
 *
 *  success (lower bound rule): the source should be succeeded records
 *    Job succeeds when the row count in validation set / row count in base set >= threshold
 *    Job fails when the row count in validation set / row count in base set < threshold
 */
public class InFlowValidationConverter extends Converter<Schema, Schema, GenericRecord, GenericRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(InFlowValidationConverter.class);
  int expectedRecordsCount;
  int actualRecordsCount;
  private String field;
  private int threshold;
  private String criteria;
  private String errorColumn;

  @Override
  public Converter<Schema, Schema, GenericRecord, GenericRecord> init(WorkUnitState workUnitState) {
    //Load the input to memory
    expectedRecordsCount = getBaseRowCount(workUnitState);
    fillValidationAttributes(workUnitState);
    return super.init(workUnitState);
  }

  @Override
  public Schema convertSchema(Schema inputSchema, WorkUnitState workUnit) {
    return inputSchema;
  }

  @Override
  public Iterable<GenericRecord> convertRecord(Schema outputSchema, GenericRecord inputRecord, WorkUnitState workUnit) {
    Optional<Object> eof = AvroUtils.getFieldValue(inputRecord, KEY_WORD_EOF);
    if (eof.isPresent() && eof.get().toString().equals(KEY_WORD_EOF)) {
      validateRule();
    } else {
      verifyAndUpdateCount(inputRecord);
    }
    return new EmptyIterable<>();
  }

  private void verifyAndUpdateCount(GenericRecord inputRecord) {
    List<Schema.Field> fieldList = inputRecord.getSchema().getFields();
    if (fieldList.size() == 1 && inputRecord.get(fieldList.get(0).name()) instanceof GenericData.Array) {
      // Check if error column exists and is not null
      if (errorColumn != null) {
        GenericData.Array<GenericRecord> arrayElements = (GenericData.Array) inputRecord.get(fieldList.get(0).name());
        arrayElements.stream().iterator().forEachRemaining(this::updateFailureCount);
      } else {
        actualRecordsCount += ((GenericData.Array<?>) inputRecord.get(fieldList.get(0).name())).size();
      }
    } else {
      actualRecordsCount += (errorColumn == null || inputRecord.get(errorColumn) != null ? 1 : 0);
    }
  }

  private void fillValidationAttributes(WorkUnitState workUnitState) {
    JsonObject validationAttributes =
        MSTAGE_VALIDATION_ATTRIBUTES.get(workUnitState);
    if (validationAttributes.has(KEY_WORD_THRESHOLD)) {
      threshold = validationAttributes.get(KEY_WORD_THRESHOLD).getAsInt();
    }
    if (validationAttributes.has(KEY_WORD_CRITERIA)) {
      criteria = validationAttributes.get(KEY_WORD_CRITERIA).getAsString();
    }
    if (validationAttributes.has(KEY_WORD_ERROR_COLUMN)) {
      errorColumn = validationAttributes.get(KEY_WORD_ERROR_COLUMN).getAsString();
    }
  }

  /**
   * Extract records from secondary input and store the expected record count.
   * If field is configured in the secondary input and field column
   *  is of type array expected record count with array size
   *  else use all the input records as expected size
   * @param workUnitState the work unit state object containing secondary input parameter
   * @return the expected row count
   */
  private int getBaseRowCount(WorkUnitState workUnitState) {
    JsonArray payloads = JsonUtils.filter(KEY_WORD_CATEGORY, KEY_WORD_PAYLOAD,
        MSTAGE_SECONDARY_INPUT.get(workUnitState));

    // by default, we expect 1 record
    if (payloads.size() == 0) {
      return 1;
    }

    // secondary input can have multiple payload entries, and each can configure a "fields" element
    // but for validation purpose, only the first payload entry, and the first field is used.
    JsonElement fields = JsonUtils.get(KEY_WORD_FIELDS, payloads.get(0).getAsJsonObject());
    field = StringUtils.EMPTY;
    if (fields.isJsonArray() && fields.getAsJsonArray().size() > 0) {
      field = fields.getAsJsonArray().get(0).getAsString();
    }

    AtomicInteger rowCount = new AtomicInteger();
    for (JsonElement entry : payloads) {
      JsonObject entryJson = entry.getAsJsonObject();
      JsonArray records = new JsonArray();
      records.addAll(new HdfsReader(workUnitState).readSecondary(entryJson));

      // No of expected records
      if (records.size() > 0
          && StringUtils.isNotBlank(field)
          && (records.get(0).getAsJsonObject().get(field) instanceof JsonArray)) {
        records.forEach(record -> rowCount.addAndGet(record.getAsJsonObject().get(field).getAsJsonArray().size()));
      } else {
        rowCount.addAndGet(records.size());
      }
    }    return rowCount.get();
  }

  private void updateFailureCount(GenericRecord record) {
    if (record.get(errorColumn) != null) {
      actualRecordsCount++;
    }
  }

  /**
   * Validate if failure/success percentage is within configured threshold
   */
  private void validateRule() {
    // check the threshold and throw new Runtime Exception
    float actualPercentage = ((float) actualRecordsCount / expectedRecordsCount) * 100;
    LOG.info("base row count: {}, actual row count: {}", expectedRecordsCount, actualRecordsCount);

    boolean failJob = criteria.equalsIgnoreCase(KEY_WORD_FAIL) && actualPercentage >= threshold
        || criteria.equalsIgnoreCase(KEY_WORD_SUCCESS) && actualPercentage < threshold;

    if (failJob) {
      // Fail the validation by throwing runtime exception
      throw new RuntimeException("Failure Threshold exceeds more than " + threshold + "%");
    } else {
      LOG.info("Validation passed with {} rate {}% {} {}%",
          criteria.equalsIgnoreCase(KEY_WORD_FAIL) ? "failure" : "success",
          new DecimalFormat("##.##").format(actualPercentage),
          criteria.equalsIgnoreCase(KEY_WORD_FAIL) ? "less than" : "greater than or equal",
          threshold);
    }
  }
}

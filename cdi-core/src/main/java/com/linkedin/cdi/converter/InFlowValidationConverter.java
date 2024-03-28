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
import java.util.concurrent.atomic.AtomicLong;
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
 *    Job succeeds when the row count in validation set / row count in base set &lt; threshold
 *    Job fails when the row count in validation set / row count in base set &gt;= threshold
 *
 *  success (lower bound rule): the source should be succeeded records
 *    Job succeeds when the row count in validation set / row count in base set &gt;= threshold
 *    Job fails when the row count in validation set / row count in base set &lt; threshold
 */
public class InFlowValidationConverter extends Converter<Schema, Schema, GenericRecord, GenericRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(InFlowValidationConverter.class);
  long expectedRecordsCount;
  long actualRecordsCount;
  private String field;
  private int threshold;
  private String criteria;
  private String errorColumn;
  private WorkUnitState workUnitState;

  @Override
  public Converter<Schema, Schema, GenericRecord, GenericRecord> init(WorkUnitState workUnitState) {
    this.workUnitState = workUnitState;
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
   * Extract records from secondary input and store the base count for comparison. Base
   * count is the expected value.
   *
   * If field is configured in the secondary input:
   *   - if field column is of type array, then base count counts array size
   *   - if field column is of type numeric, the base count adds the numeric value
   *
   * else the base count counts simply the number of rows, this includes:
   *   - field column is not numeric nor array
   *   - field column is defined but not exists in the secondary input
   *   - field column is not defined at all
   *
   * @param workUnitState the work unit state object containing secondary input parameter
   * @return the expected row count
   */
  private long getBaseRowCount(WorkUnitState workUnitState) {
    JsonArray validations = JsonUtils.filter(KEY_WORD_CATEGORY, KEY_WORD_VALIDATION,
        MSTAGE_SECONDARY_INPUT.get(workUnitState));

    // by default, we expect 1 record
    if (validations.isJsonNull() || validations.size() == 0) {
      return 1;
    }

    // secondary input can have multiple payload entries, and each can configure a "fields" element
    // but for validation purpose, only the first payload entry, and the first field is used.
    JsonElement fields = JsonUtils.get(KEY_WORD_FIELDS, validations.get(0).getAsJsonObject());
    field = StringUtils.EMPTY;
    if (fields.isJsonArray() && fields.getAsJsonArray().size() > 0) {
      field = fields.getAsJsonArray().get(0).getAsString();
    }

    AtomicLong rowCount = new AtomicLong();

    for (JsonElement entry : validations) {
      JsonObject entryJson = entry.getAsJsonObject();
      JsonArray records = new JsonArray();
      records.addAll(new HdfsReader(workUnitState).readSecondary(entryJson));

      // No of expected records
      if (records.size() > 0  && StringUtils.isNotBlank(field)) {
        JsonElement sample = JsonUtils.get(field, records.get(0).getAsJsonObject());
        if (sample.isJsonArray()) {
          records.forEach(record -> rowCount.addAndGet(record.getAsJsonObject().get(field).getAsJsonArray().size()));
        } else if (sample.isJsonPrimitive()) {
          try {
            records.forEach(record -> rowCount.addAndGet(record.getAsJsonObject().get(field).getAsLong()));
          } catch (Exception e) {
            LOG.info("Secondary field is not a long value.");
            rowCount.addAndGet(records.size());
          }
        } else {
          rowCount.addAndGet(records.size());
        }
      } else {
        rowCount.addAndGet(records.size());
      }
    }
    return rowCount.get();
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

    if (criteria.equalsIgnoreCase(KEY_WORD_FAIL) && actualPercentage >= threshold) {
      // Fail the validation by throwing runtime exception
      workUnitState.setWorkingState(WorkUnitState.WorkingState.FAILED);
      throw new RuntimeException("Failure rate exceeds threshold (" + threshold + "%).");
    } else if (criteria.equalsIgnoreCase(KEY_WORD_SUCCESS) && actualPercentage < threshold) {
      // Fail the validation by throwing runtime exception
      workUnitState.setWorkingState(WorkUnitState.WorkingState.FAILED);
      throw new RuntimeException("Success rate is lower than threshold (" + threshold + "%).");
    } else {
      LOG.info("Validation passed with {} rate {}% {} {}%",
          criteria.equalsIgnoreCase(KEY_WORD_FAIL) ? "failure" : "success",
          new DecimalFormat("##.##").format(actualPercentage),
          criteria.equalsIgnoreCase(KEY_WORD_FAIL) ? "less than" : "greater than or equal",
          threshold);
    }
  }
}

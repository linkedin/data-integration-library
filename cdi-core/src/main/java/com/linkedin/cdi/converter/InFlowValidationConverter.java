// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.converter;

import com.google.common.base.Optional;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.linkedin.cdi.configuration.MultistageProperties;
import com.linkedin.cdi.configuration.StaticConstants;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.Converter;
import com.linkedin.cdi.util.HdfsReader;
import org.apache.gobblin.util.AvroUtils;
import org.apache.gobblin.util.EmptyIterable;

import static com.linkedin.cdi.configuration.StaticConstants.*;


/**
 * This converter does basic count validation based on the Failure Records or Success Records criteria.
 */
@Slf4j
public class InFlowValidationConverter extends Converter<Schema, Schema, GenericRecord, GenericRecord> {
  int expectedRecordsCount;
  int actualRecordsCount;
  private String field;
  private int failurePercentage;
  private String criteria;
  private String errorColumn;

  @Override
  public Converter<Schema, Schema, GenericRecord, GenericRecord> init(WorkUnitState workUnitState) {
    //Load the input to memory
    getPayloads(workUnitState);
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
      if (errorColumn != null) {
        updateFailureCount(inputRecord);
      } else {
        throw new RuntimeException("Invalid ms.data.field/ms.validation.attributes configuration. "
            + "InputRecord should be of type Array or should have errorColumn");
      }
    }
  }

  private void fillValidationAttributes(WorkUnitState workUnitState) {
    JsonObject validationAttributes =
        MultistageProperties.MSTAGE_VALIDATION_ATTRIBUTES.getValidNonblankWithDefault(workUnitState);
    if (validationAttributes.has(KEY_WORD_THRESHOLD)) {
      failurePercentage = validationAttributes.get(KEY_WORD_THRESHOLD).getAsInt();
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
   */
  private void getPayloads(WorkUnitState workUnitState) {
    JsonArray payloads = MultistageProperties.MSTAGE_SECONDARY_INPUT.getValidNonblankWithDefault(workUnitState);
    JsonArray records = new JsonArray();
    List<String> fields = new ArrayList<>();
    for (JsonElement entry : payloads) {
      if (!entry.isJsonObject()) {
        log.error("Elements within secondary input should be valid JsonObjects, provided: {}", entry.toString());
      }
      JsonObject entryJson = entry.getAsJsonObject();
      records.addAll(new HdfsReader(workUnitState).readSecondary(entryJson));
      if (entryJson.has(StaticConstants.KEY_WORD_FIELDS)) {
        if (entryJson.get(StaticConstants.KEY_WORD_FIELDS).isJsonArray()) {
          entryJson.get(StaticConstants.KEY_WORD_FIELDS)
              .getAsJsonArray()
              .forEach(arrayItem -> fields.add(arrayItem.getAsString()));
        }
        field = fields.size() >= 1 ? fields.get(0) : StringUtils.EMPTY;
      }
      // No of expected records
      if (records.size() > 0 && StringUtils.isNotBlank(field) && (records.get(0)
          .getAsJsonObject()
          .get(field) instanceof JsonArray)) {
        records.forEach(record -> expectedRecordsCount += record.getAsJsonObject().get(field).getAsJsonArray().size());
      } else if (records.size() > 0) {
        expectedRecordsCount = records.size();
      }
    }
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
    boolean failJob = false;
    // validate rules based on type of records
    if (criteria.equalsIgnoreCase(KEY_WORD_FAIL)) {
      failJob = actualPercentage > failurePercentage;
    } else if (criteria.equalsIgnoreCase(KEY_WORD_SUCCESS)) {
      failJob = (100 - actualPercentage) > failurePercentage;
    }
    log.info("Total expectedRecords: {} , failedRecords: {}", expectedRecordsCount, actualRecordsCount);

    if (failJob) {
      // Fail the validation by throwing runtime exception
      throw new RuntimeException("Failure Threshold exceeds more than " + failurePercentage + "%");
    } else {
      log.info("Validation passed with failure rate {}% less than {}%",
          new DecimalFormat("##.##").format(actualPercentage), failurePercentage);
    }
  }
}

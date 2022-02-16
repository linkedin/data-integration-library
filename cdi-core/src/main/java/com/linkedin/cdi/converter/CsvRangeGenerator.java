// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.converter;

import com.google.common.base.Preconditions;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.Converter;
import org.apache.gobblin.converter.SingleRecordIterable;
import org.apache.gobblin.util.EmptyIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.cdi.configuration.PropertyCollection.*;
import static com.linkedin.cdi.configuration.StaticConstants.*;


/**
 * This converter takes an ordered list of values and generates ranges
 * per given batch size.
 *
 * Given a list [0...50] and batch size 10, it will generate ranges
 * [0, 9]
 * [10, 19]
 * [20, 29]
 * [30, 39]
 * [40, 49]
 * [50, 50]
 */
public class CsvRangeGenerator extends Converter<String, JsonArray, String[], JsonObject> {
  private static final Logger LOG = LoggerFactory.getLogger(CsvRangeGenerator.class);
  private JsonArray targetSchema;
  private String rangeStart = null;
  private String rangeEnd = null;
  private int count = 0;
  private String columnStart;
  private String columnEnd;

  private long batchSize = 0;
  private String rangeMax = null;


  @Override
  public Converter<String, JsonArray, String[], JsonObject> init(WorkUnitState workUnit) {
    // TODO create a default schema of 2 string columns
    targetSchema = MSTAGE_TARGET_SCHEMA.get(workUnit);
    Preconditions.checkArgument(targetSchema.size() == 2, "Target schema have to be 2 columns");
    columnStart = targetSchema.get(0).getAsJsonObject().get(KEY_WORD_COLUMN_NAME).getAsString();
    columnEnd = targetSchema.get(1).getAsJsonObject().get(KEY_WORD_COLUMN_NAME).getAsString();

    // batchSize has to be positive integers
    batchSize = MSTAGE_RANGE_GENERATOR_BATCH_SIZE.get(workUnit);
    rangeMax = MSTAGE_RANGE_GENERATOR_MAX_VALUE.get(workUnit);
    return this;
  }

  @Override
  public JsonArray convertSchema(String inputSchema, WorkUnitState workUnit) {
    Preconditions.checkNotNull(inputSchema, "inputSchema is required.");
    return targetSchema;
  }

  @Override
  public Iterable<JsonObject> convertRecord(JsonArray outputSchema, String[] inputRecord, WorkUnitState workUnit) {
    if (inputRecord[0].equals(KEY_WORD_EOF)) {
      // only output when there's at least one record
      rangeEnd = rangeMax;
      return outputIterable(1);
    }

    if (count == 0) {
      rangeStart = inputRecord[0];
      rangeEnd = inputRecord[0];
      count ++;
    } else if (count % batchSize != 0) {
      rangeEnd = inputRecord[0];
      count ++;
    } else {
      Iterable<JsonObject> output = outputIterable(batchSize);
      rangeStart = inputRecord[0];
      rangeEnd = inputRecord[0];
      count = 1;
      return output;
    }

    return new EmptyIterable<>();
  }

  /**
   * Output a single record iterable when the size of the normalized array has reached the threshold
   * and empty iterable otherwise.
   * @param threshold the threshold to output
   * @return iterable of JsonObject
   */
  private Iterable<JsonObject> outputIterable(long threshold) {
    if (count >= threshold) {
      return new SingleRecordIterable<>(buildRangeRecord());
    } else {
      return new EmptyIterable<>();
    }
  }

  /**
   * Build a final range record
   * @return the range record
   */
  private JsonObject buildRangeRecord() {
    JsonObject newRecord = new JsonObject();
    newRecord.addProperty(columnStart, rangeStart);
    newRecord.addProperty(columnEnd, rangeEnd);
    return newRecord;
  }
}

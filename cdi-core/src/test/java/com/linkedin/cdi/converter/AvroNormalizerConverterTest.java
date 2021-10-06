// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.converter;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.linkedin.cdi.util.AvroSchemaUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.converter.avro.UnsupportedDateTypeException;
import org.apache.gobblin.source.workunit.Extract;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class AvroNormalizerConverterTest {
  AvroNormalizerConverter _avroNormalizerConverter;
  String sourceSchema = "[{\"columnName\":\"asIs\",\"isNullable\":\"false\",\"dataType\":{\"type\":\"string\"}},"
      + "{\"columnName\":\"toBeNormalized1\",\"isNullable\":\"false\",\"dataType\":{\"type\":\"string\"}},"
      + "{\"columnName\":\"toBeNormalized2\",\"isNullable\":\"false\",\"dataType\":{\"type\":\"string\"}}]";
  String targetSchema = "[{\"columnName\":\"asIs\",\"isNullable\":\"false\",\"dataType\":{\"type\":\"string\"}},"
      + "{\"columnName\":\"normalized\",\"isNullable\":\"false\",\"dataType\":{\"type\":\"string\"}}]";
  Schema inputSchema;
  Schema outputSchema;
  WorkUnitState state;

  @BeforeMethod
  public void beforeMethod() throws UnsupportedDateTypeException {
    _avroNormalizerConverter = new AvroNormalizerConverter();
    state = mock(WorkUnitState.class);
    Extract extract = new Extract(Extract.TableType.SNAPSHOT_ONLY, "com.linkedin.test", "test");
    when(state.getProp("ms.target.schema", new JsonArray().toString())).thenReturn(targetSchema);
    when(state.getPropAsLong("ms.normalizer.batch.size", 0L)).thenReturn(2L);
    when(state.getExtract()).thenReturn(extract);
    _avroNormalizerConverter.init(state);
    inputSchema = AvroSchemaUtils.fromJsonSchema(new Gson().fromJson(sourceSchema, JsonArray.class), state);
    outputSchema = AvroSchemaUtils.fromJsonSchema(new Gson().fromJson(targetSchema, JsonArray.class), state);
  }

  @Test
  public void testConvertSchema() throws SchemaConversionException, UnsupportedDateTypeException {
    Schema schema = _avroNormalizerConverter.convertSchema(inputSchema, state);

    Assert.assertEquals(schema,
        AvroSchemaUtils.fromJsonSchema(new Gson().fromJson(targetSchema, JsonArray.class), state));
  }

  @Test
  public void testConvertRecord() throws SchemaConversionException, DataConversionException {
    _avroNormalizerConverter.convertSchema(inputSchema, state);
    GenericRecord inputRecord = new GenericData.Record(inputSchema);
    inputRecord.put("asIs", "dummy");
    inputRecord.put("toBeNormalized1", "dummy");
    inputRecord.put("toBeNormalized2", "dummy");
    // Call twice to make sure the resulting record gives JsonArray size 2
    _avroNormalizerConverter.convertRecord(outputSchema, inputRecord, state);
    Iterable<GenericRecord> recordIterable = _avroNormalizerConverter.convertRecord(outputSchema, inputRecord, state);
    GenericRecord record = recordIterable.iterator().next();
    GenericData.Array<GenericRecord> normalized = (GenericData.Array<GenericRecord>) record.get("normalized");
    Assert.assertEquals(normalized.size(), 2);
    // There's 1 record in the buffer before before passing eof
    _avroNormalizerConverter.convertRecord(outputSchema, record, state);
    GenericRecord eof = AvroSchemaUtils.createEOF(state);
    record = _avroNormalizerConverter.convertRecord(outputSchema, eof, state).iterator().next();
    normalized = (GenericData.Array<GenericRecord>) record.get("normalized");
    Assert.assertEquals(normalized.size(), 1);
    // When there are no records in the buffer calling before eof
    Assert.assertFalse(_avroNormalizerConverter.convertRecord(outputSchema, eof, state).iterator().hasNext());
  }
}
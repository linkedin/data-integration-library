// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.util;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.avro.UnsupportedDateTypeException;
import org.apache.gobblin.source.workunit.Extract;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class AvroSchemaUtilsTest {
  WorkUnitState state;
  String schemaString = "[{\"columnName\":\"asIs\",\"isNullable\":\"false\",\"dataType\":{\"type\":\"string\"}},"
      + "{\"columnName\":\"normalized\",\"isNullable\":\"false\",\"dataType\":{\"type\":\"string\"}}]";
  JsonArray schemaArray = new Gson().fromJson(schemaString, JsonArray.class);
  Schema schema;

  @BeforeMethod
  public void beforeMethod() throws UnsupportedDateTypeException {
    state = mock(WorkUnitState.class);
    Extract extract = new Extract(Extract.TableType.SNAPSHOT_ONLY, "com.linkedin.test", "test");
    when(state.getExtract()).thenReturn(extract);
    schema = AvroSchemaUtils.fromJsonSchema(schemaArray, state);
  }

  @Test
  public void testFromJsonSchema() throws UnsupportedDateTypeException {
    List<Schema.Field> fields = schema.getFields();
    Assert.assertEquals(fields.size(), 2);
    Assert.assertEquals(fields.get(0).name(), "asIs");
    Assert.assertEquals(fields.get(1).name(), "normalized");
  }

  @Test
  public void testGetSchemaFieldNames() throws UnsupportedDateTypeException {
    List<String> fieldNames = AvroSchemaUtils.getSchemaFieldNames(schema);
    Assert.assertEquals(fieldNames.size(), 2);
    Assert.assertEquals(fieldNames.get(0), "asIs");
    Assert.assertEquals(fieldNames.get(1), "normalized");
  }

  @Test
  public void testDeepCopySchemaField() {
    Schema.Field originalField = schema.getField("asIs");
    Schema.Field copiedField = AvroSchemaUtils.deepCopySchemaField(originalField);
    Assert.assertEquals(originalField, copiedField);
  }

  @Test
  public void testCreateEOF() {
    GenericRecord row = AvroSchemaUtils.createEOF(state);
    Assert.assertEquals(row.getSchema().getFields().size(), 1);
    Assert.assertEquals(row.get("EOF"), "EOF");
  }

  @Test
  public void testDeepCopy() {
    GenericRecord row = AvroSchemaUtils.createEOF(state);
    GenericRecord copiedRow = AvroSchemaUtils.deepCopy(row.getSchema(), row);
    Assert.assertEquals(row, copiedRow);
  }
}
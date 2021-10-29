// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.converter;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import org.apache.gobblin.configuration.WorkUnitState;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


@Test
public class JsonNormalizerConverterTest {
  JsonNormalizerConverter underTest;
  String sourceSchemaOrdinary = "[{\"columnName\":\"asIs\",\"isNullable\":\"false\",\"dataType\":{\"type\":\"string\"}},"
      + "{\"columnName\":\"toBeNormalized1\",\"isNullable\":\"false\",\"dataType\":{\"type\":\"string\"}},"
      + "{\"columnName\":\"toBeNormalized2\",\"isNullable\":\"false\",\"dataType\":{\"type\":\"string\"}}]";
  String targetSchemaOrdinary = "[{\"columnName\":\"asIs\",\"isNullable\":\"false\",\"dataType\":{\"type\":\"string\"}},"
      + "{\"columnName\":\"normalized\",\"isNullable\":\"false\",\"dataType\":{\"type\":\"string\"}}]";
  String sourceSchemaWithNullableFields = "[{\"columnName\":\"toBeNormalized0\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}},"
      + "{\"columnName\":\"asIs\",\"isNullable\":\"false\",\"dataType\":{\"type\":\"string\"}},"
      + "{\"columnName\":\"toBeNormalized1\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}},"
      + "{\"columnName\":\"toBeNormalized2\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}}]";
  String targetSchemaWithNullableFields = "[{\"columnName\":\"asIs\",\"isNullable\":\"false\",\"dataType\":{\"type\":\"string\"}}, "
      + "{\"columnName\":\"nullable\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}},"
      + "{\"columnName\":\"normalized\",\"isNullable\":\"false\",\"dataType\":{\"type\":\"map\", \"values\": \"string\"}}]";
  JsonArray inputSchema;
  JsonArray outputSchema;
  WorkUnitState state;


  private void setup(String sourceSchema, String targetSchema, int batchSize) {
    underTest = new JsonNormalizerConverter();
    state = new WorkUnitState();
    state.setProp("ms.target.schema", targetSchema);
    state.setProp("ms.normalizer.batch.size", batchSize);
    underTest.init(state);
    inputSchema = new Gson().fromJson(sourceSchema, JsonArray.class);
    outputSchema = new Gson().fromJson(targetSchema, JsonArray.class);
  }

  @Test
  public void testConvertSchemaAndRecord() {
    setup(sourceSchemaOrdinary, targetSchemaOrdinary, 2);
    // test convert schema
    Assert.assertEquals(underTest.convertSchema(inputSchema, state), outputSchema);

    // test convert record
    JsonObject record = new JsonObject();
    record.addProperty("asIs", "dummy");
    record.addProperty("toBeNormalized1", "dummy");
    record.addProperty("toBeNormalized2", "dummy");
    // Call twice to make sure the resulting record gives JsonArray size 2
    underTest.convertRecord(outputSchema, record, state);
    Iterable<JsonObject> recordIterable = underTest.convertRecord(outputSchema, record, state);
    JsonObject jsonObject = recordIterable.iterator().next();
    // There's 1 record in the buffer before before passing eof
    underTest.convertRecord(outputSchema, record,state);
    JsonObject eof = new JsonObject();
    eof.addProperty("EOF", "EOF");
    jsonObject = underTest.convertRecord(outputSchema, eof, state).iterator().next();
    Assert.assertEquals(jsonObject.getAsJsonArray("normalized").size(), 1);
    // When there are no records in the buffer calling before eof
    Assert.assertFalse(underTest.convertRecord(outputSchema, eof, state).iterator().hasNext());
  }

  public void testConvertSchemaAndRecordWithNullableFields() {
    setup(sourceSchemaWithNullableFields, targetSchemaWithNullableFields, 1);
    // test convert schema
    Assert.assertEquals(underTest.convertSchema(inputSchema, state), outputSchema);

    // test convert record
    JsonObject record = new JsonObject();
    record.addProperty("asIs", "dummy");
    record.addProperty("nullable", "dummy");
    record.addProperty("toBeNormalized1", "dummy");
    record.addProperty("toBeNormalized2", "dummy");
    Iterable<JsonObject> recordIterable = underTest.convertRecord(outputSchema, record, state);
    JsonObject jsonObject = recordIterable.iterator().next();
    Assert.assertEquals(jsonObject.entrySet().size(), 3);
    Assert.assertEquals(jsonObject.getAsJsonObject("normalized").entrySet().size(), 2);
    // There's 1 record in the buffer before before passing eof
    JsonObject recordWithNullableField = new JsonObject();
    recordWithNullableField.addProperty("asIs", "dummy");
    recordWithNullableField.add("toBeNormalized0", JsonNull.INSTANCE);
    // map type should not include this null value
    recordWithNullableField.addProperty("toBeNormalized0", (String) null);
    recordWithNullableField.addProperty("toBeNormalized1", "dummy");
    recordWithNullableField.addProperty("toBeNormalized2", "dummy");
    recordIterable = underTest.convertRecord(outputSchema, recordWithNullableField, state);
    jsonObject = recordIterable.iterator().next();
    Assert.assertEquals(jsonObject.entrySet().size(), 2);
    Assert.assertEquals(jsonObject.getAsJsonObject("normalized").entrySet().size(), 2);
    JsonObject eof = new JsonObject();
    eof.addProperty("EOF", "EOF");
    // When there are no records in the buffer calling before eof
    Assert.assertFalse(underTest.convertRecord(outputSchema, eof, state).iterator().hasNext());
  }

}
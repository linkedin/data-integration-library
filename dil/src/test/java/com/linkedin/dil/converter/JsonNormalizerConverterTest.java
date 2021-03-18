// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.dil.converter;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.gobblin.configuration.WorkUnitState;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


@Test
public class JsonNormalizerConverterTest {
  JsonNormalizerConverter underTest;
  String sourceSchema = "[{\"columnName\":\"asIs\",\"isNullable\":\"false\",\"dataType\":{\"type\":\"string\"}},"
      + "{\"columnName\":\"toBeNormalized1\",\"isNullable\":\"false\",\"dataType\":{\"type\":\"string\"}},"
      + "{\"columnName\":\"toBeNormalized2\",\"isNullable\":\"false\",\"dataType\":{\"type\":\"string\"}}]";
  String targetSchema = "[{\"columnName\":\"asIs\",\"isNullable\":\"false\",\"dataType\":{\"type\":\"string\"}},"
      + "{\"columnName\":\"normalized\",\"isNullable\":\"false\",\"dataType\":{\"type\":\"string\"}}]";
  JsonArray inputSchema;
  JsonArray outputSchema;
  WorkUnitState state;

  @BeforeMethod
  public void beforeMethod() {
    underTest = new JsonNormalizerConverter();
    state = new WorkUnitState();
    state.setProp("ms.target.schema", targetSchema);
    state.setProp("ms.normalizer.batch.size", 2);
    underTest.init(state);
    inputSchema = new Gson().fromJson(sourceSchema, JsonArray.class);
    outputSchema = new Gson().fromJson(targetSchema, JsonArray.class);
  }

  @Test
  public void testConvertSchema() {
    Assert.assertTrue(outputSchema.equals(underTest.convertSchema(inputSchema, state)));
  }

  @Test
  public void testConvertRecord() {
    underTest.convertSchema(inputSchema, state);
    JsonObject record = new JsonObject();
    record.addProperty("asIs", "dummy");
    record.addProperty("toBeNormalized1", "dummy");
    record.addProperty("toBeNormalized2", "dummy");
    // Call twice to make sure the resulting record gives JsonArray size 2
    underTest.convertRecord(outputSchema, record, state);
    Iterable<JsonObject> recordIterable = underTest.convertRecord(outputSchema, record, state);
    JsonObject jsonObject = recordIterable.iterator().next();
    Assert.assertEquals(jsonObject.getAsJsonArray("normalized").size(), 2);
    // There's 1 record in the buffer before before passing eof
    underTest.convertRecord(outputSchema, record,state);
    JsonObject eof = new JsonObject();
    eof.addProperty("EOF", "EOF");
    jsonObject = underTest.convertRecord(outputSchema, eof, state).iterator().next();
    Assert.assertEquals(jsonObject.getAsJsonArray("normalized").size(), 1);
    // When there are no records in the buffer calling before eof
    Assert.assertFalse(underTest.convertRecord(outputSchema, eof, state).iterator().hasNext());
  }
}
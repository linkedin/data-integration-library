// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.util;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test
public class JsonIntermediateSchemaTest {
  private Gson gson = new Gson();

  @Mock
  private JsonIntermediateSchema jsonIntermediateSchema;

  @BeforeClass
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void test_jisColumn_first_constructor() {
    String jsonElementTypeString = "[{\"City\":\"Seattle\"}]";
    JsonIntermediateSchema.JisColumn jisColumn =
        jsonIntermediateSchema.new JisColumn("name", true, jsonElementTypeString);
    Assert.assertEquals("name", jisColumn.getColumnName());
    Assert.assertTrue(jisColumn.getIsNullable());
    Assert.assertEquals(JsonElementTypes.UNION, jisColumn.getDataType().type);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void test_jisColumn_second_constructor_failed() {
    JsonObject obj = new JsonObject();
    obj.addProperty("name", "tester");
    jsonIntermediateSchema.new JisColumn(obj);
  }

  @Test
  public void test_jisColumn_second_constructor_succeeded() {
    JsonObject dataTypeObj = new JsonObject();
    dataTypeObj.addProperty("name", "tester");
    dataTypeObj.addProperty("type", "[[\"name\"]]");
    JsonArray jsonArray = new JsonArray();
    jsonArray.add("symbolA");
    jsonArray.add("symbolB");
    dataTypeObj.add("symbols", jsonArray);

    JsonObject obj = new JsonObject();
    obj.addProperty("columnName", "testColumn");
    obj.addProperty("isNullable", false);
    obj.add("dataType", dataTypeObj);
    JsonIntermediateSchema.JisColumn jisColumn = jsonIntermediateSchema.new JisColumn(obj);
    Assert.assertEquals("testColumn", jisColumn.getColumnName());
    Assert.assertFalse(jisColumn.isNullable);
    Assert.assertEquals(JsonElementTypes.UNION, jisColumn.getDataType().getType());

    obj = new JsonObject();
    dataTypeObj.addProperty("type", "ENUM");
    obj.add("dataType", dataTypeObj);
    jisColumn = jsonIntermediateSchema.new JisColumn(obj);
    Assert.assertEquals("unknown", jisColumn.getColumnName());
    Assert.assertTrue(jisColumn.isNullable);
    Assert.assertEquals(JsonElementTypes.ENUM, jisColumn.getDataType().getType());
    Assert.assertEquals(jsonArray, jisColumn.getDataType().getSymbols());
    Assert.assertEquals("tester", jisColumn.getDataType().getName());
  }

  @Test
  public void test_JsonIntermediateSchema_constructor_succeeded() {
    JsonObject dataTypeObj = new JsonObject();
    dataTypeObj.addProperty("name", "tester");
    dataTypeObj.addProperty("type", "[[\"name\"]]");
    JsonArray jsonArray = new JsonArray();
    JsonObject obj = new JsonObject();
    obj.addProperty("columnName", "testColumn");
    obj.addProperty("isNullable", false);
    obj.add("dataType", dataTypeObj);
    jsonArray.add(obj);

    JsonIntermediateSchema jsonIntermediateSchema = new JsonIntermediateSchema(jsonArray);
    Assert.assertEquals(jsonIntermediateSchema.schemaName, "root");
    JsonIntermediateSchema.JisColumn jisColumn = jsonIntermediateSchema.getColumns().get("testColumn");
    Assert.assertEquals("testColumn", jisColumn.getColumnName());
    Assert.assertFalse(jisColumn.getIsNullable());
    Assert.assertEquals(JsonElementTypes.UNION, jisColumn.getDataType().getType());
    Assert.assertEquals("tester", jisColumn.getDataType().name);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void test_JsonIntermediateSchema_constructor_non_object_failed() {
    JsonArray jsonArray = new JsonArray();
    jsonArray.add("tester");
    new JsonIntermediateSchema(jsonArray);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void test_JsonIntermediateSchema_constructor_non_failed() {
    JsonArray jsonArray = new JsonArray();
    jsonArray.add((JsonElement) null);
    new JsonIntermediateSchema(jsonArray);
  }
}

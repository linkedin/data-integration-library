// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.util;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonArray;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test
public class WorkUnitStatusTest {

  /**
   * testing the builder function
   */
  @Test
  public void testDataMethods() {
    String expected = "WorkUnitStatus(totalCount=10, setCount=0, pageNumber=1, pageStart=0, pageSize=100, buffer=null, messages={name=text}, sessionKey=)";
    Map<String, String> messages = new HashMap<>();
    messages.put("name", "text");
    Assert.assertEquals(expected, WorkUnitStatus.builder()
        .pageNumber(1)
        .pageSize(100)
        .totalCount(10)
        .messages(messages)
        .sessionKey("")
        .build()
        .toString());
  }

  /**
   * test getting schema
   * scenario 1: default value
   * scenario 2: source provided value
   * scenario 3: source provided invalid value
   */
  public void testGetSchema() {
    // when there is no source provided schema, the getSchema() method
    // should just return a new JsonSchema object
    Assert.assertEquals(WorkUnitStatus.builder().build().getSchema(), new JsonArray());

    String originSchema = "{\"id\":{\"type\":\"string\"}}";
    SchemaBuilder builder = SchemaBuilder.fromJsonSchema(originSchema);
    Map<String, String> messages = new HashMap<>();
    messages.put("schema", builder.buildAltSchema().toString());
    Assert.assertEquals(WorkUnitStatus.builder().messages(messages).build().getSchema().toString(),
        "[{\"columnName\":\"id\",\"isNullable\":false,\"dataType\":{\"type\":\"string\"}}]");

    // source schema is invalid
    WorkUnitStatus.builder().messages(ImmutableMap.of("schema", "{\"id\": {\"type\": \"string\"}")).build().getSchema();

    // source schema is null
    Assert.assertEquals(WorkUnitStatus.builder().messages(null).build().getSchema(), new JsonArray());
  }
}

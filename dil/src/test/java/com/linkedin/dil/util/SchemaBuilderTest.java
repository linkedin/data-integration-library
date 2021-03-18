// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.dil.util;

import java.util.HashMap;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test
public class SchemaBuilderTest {
  @Test
  public void testReverseJsonSchema() {
    String originSchema = "{\"id\":{\"type\":\"string\"}}";
    SchemaBuilder builder = SchemaBuilder.fromJsonSchema(originSchema);
    Assert.assertEquals(builder.buildJsonSchema().toString(), originSchema);

    originSchema = "{\"id\":{\"type\":[\"string\",\"null\"]}}";
    builder = SchemaBuilder.fromJsonSchema(originSchema);
    Assert.assertEquals(builder.buildJsonSchema().toString(), originSchema);

    originSchema = "{\"methods\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}}}";
    builder = SchemaBuilder.fromJsonSchema(originSchema);
    Assert.assertEquals(builder.buildJsonSchema().toString(), originSchema);
  }

  @Test
  public void testAltSchema() {
    String originSchema = "{\"id\":{\"type\":\"string\"}}";
    SchemaBuilder builder = SchemaBuilder.fromJsonSchema(originSchema);
    Assert.assertEquals(builder.buildAltSchema().toString(),
        "[{\"columnName\":\"id\",\"isNullable\":false,\"dataType\":{\"type\":\"string\"}}]");
    Assert.assertEquals(builder.buildAltSchema(new HashMap<>(), false, null, null, true).toString(),
        "[{\"columnName\":\"id\",\"isNullable\":true,\"dataType\":{\"type\":\"string\"}}]");

    originSchema = "{\"id\":{\"type\":[\"string\",\"null\"]}}";
    builder = SchemaBuilder.fromJsonSchema(originSchema);
    Assert.assertEquals(builder.buildAltSchema().toString(),
        "[{\"columnName\":\"id\",\"isNullable\":true,\"dataType\":{\"type\":\"string\"}}]");

    originSchema = "{\"methods\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}}}";
    builder = SchemaBuilder.fromJsonSchema(originSchema);
    Assert.assertEquals(builder.buildAltSchema().toString(),
        "[{\"columnName\":\"methods\",\"isNullable\":false,\"dataType\":{\"type\":\"array\",\"name\":\"methods\",\"items\":\"string\"}}]");
  }
}

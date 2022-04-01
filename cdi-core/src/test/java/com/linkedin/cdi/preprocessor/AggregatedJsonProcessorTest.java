// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.preprocessor;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import org.junit.Assert;
import org.testng.annotations.Test;


public class AggregatedJsonProcessorTest {
  @Test
  public void testProcess() throws IOException {
    String sample = "{\"items\": [{\"tableName\": \"tableName\", \"columnNames\": [\"id\"], \"rows\": [[\"1234\"], [null]]}]}";
    InputStream input = new ByteArrayInputStream(sample.getBytes(StandardCharsets.UTF_8));
    JsonObject params = new JsonObject();
    params.addProperty("header","items.0.columnNames" );
    params.addProperty("data","items.0.rows" );
    params.addProperty("fields","items.0.tableName" );
    InputStreamProcessor processor = new AggregatedJsonProcessor(params);
    InputStream output = processor.process(input);
    JsonElement json = new JsonParser().parse(new InputStreamReader(output, StandardCharsets.UTF_8));
    JsonObject jsonObject = json.getAsJsonObject();
    Assert.assertTrue(jsonObject.entrySet().size() == 3);
  }
}

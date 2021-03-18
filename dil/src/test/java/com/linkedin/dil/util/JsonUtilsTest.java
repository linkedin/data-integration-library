// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.dil.util;

import com.google.gson.JsonObject;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test
public class JsonUtilsTest {
  @Test
  public void testDeepCopy() {
    JsonObject source = new JsonObject();
    source.addProperty("name", "value");

    JsonObject replica = JsonUtils.deepCopy(source).getAsJsonObject();
    JsonObject same = source;

    source.remove("name");
    source.addProperty("name", "newValue");

    Assert.assertEquals(source.get("name").getAsString(), same.get("name").getAsString());
    Assert.assertNotEquals(source.get("name").getAsString(), replica.get("name").getAsString());
  }

  @Test
  public void testContains() {
    JsonObject a = new JsonObject();
    JsonObject b = new JsonObject();

    a.addProperty("name1", "value1");
    a.addProperty("name2", "value2");
    b.addProperty("name1", "value1");

    Assert.assertTrue(JsonUtils.contains(a, b));
    Assert.assertTrue(JsonUtils.contains("{\"name1\": \"value1\", \"name2\": \"value2\"}", b));

    b.addProperty("name2", "value2x");
    Assert.assertFalse(JsonUtils.contains(a, b));
    Assert.assertFalse(JsonUtils.contains("{\"name1\": \"value1\", \"name2\": \"value2\"}", b));

    b.addProperty("name3", "value3");
    Assert.assertFalse(JsonUtils.contains(a, b));
  }

  @Test
  public void testReplace() {
    JsonObject a = new JsonObject();
    JsonObject b = new JsonObject();

    a.addProperty("name1", "value1");
    b.addProperty("name1", "newValue1");

    Assert.assertEquals(JsonUtils.replace(a, b).toString(), "{\"name1\":\"newValue1\"}");

    a.addProperty("name2", "value1");
    Assert.assertEquals(JsonUtils.replace(a, b).toString(), "{\"name1\":\"newValue1\",\"name2\":\"value1\"}");
  }
}

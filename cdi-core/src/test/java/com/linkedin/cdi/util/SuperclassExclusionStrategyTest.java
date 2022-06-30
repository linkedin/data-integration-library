// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.util;

import com.google.gson.Gson;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test
public class SuperclassExclusionStrategyTest {

  private static class TestParentClass {
    public String field1 = "parentField1";
  }

  private static class TestSubClass extends TestParentClass {
    public String field1  = "childField1";
    public String field2  = "childField2";
  }

  @Test
  public void testSuperclassExclusionStrategy() {
    Gson gson = JsonUtils.GSON_WITH_SUPERCLASS_EXCLUSION;
    TestSubClass testSubClass = new TestSubClass();
    String testSubClassStr = gson.toJson(testSubClass);
    Assert.assertEquals(testSubClassStr, "{\"field2\":\"childField2\",\"field1\":\"parentField1\"}");
  }

}

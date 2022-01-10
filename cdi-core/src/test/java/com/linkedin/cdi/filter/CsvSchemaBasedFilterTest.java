// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.filter;

import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.linkedin.cdi.keys.CsvExtractorKeys;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;


public class CsvSchemaBasedFilterTest {
  @Test
  public void testFilter() {
    String[] input = "AA,BB,CC".split(",");
    List<Integer> columnProjection = Lists.newArrayList(0, 2, 1);
    String[] output = new CsvSchemaBasedFilter(new JsonArray(),
        new CsvExtractorKeys()).filter(input, columnProjection);
    Assert.assertEquals(output[0], input[0]);
    Assert.assertEquals(output[1], input[2]);
    Assert.assertEquals(output[2], input[1]);

    columnProjection = Lists.newArrayList(0, 2, 1, 0);
    output = new CsvSchemaBasedFilter(new JsonArray(),
        new CsvExtractorKeys()).filter(input, columnProjection);
    Assert.assertEquals(output[0], input[0]);
    Assert.assertEquals(output[1], input[2]);
    Assert.assertEquals(output[2], input[1]);
    Assert.assertEquals(output[3], input[0]);

    columnProjection = Lists.newArrayList(0, 1, 2, 3);
    output = new CsvSchemaBasedFilter(new JsonArray(),
        new CsvExtractorKeys()).filter(input, columnProjection);
    Assert.assertTrue(output[3].isEmpty());

    Assert.assertNull(new CsvSchemaBasedFilter(new JsonArray(),
        new CsvExtractorKeys()).filter(input, Lists.newArrayList()));
  }
}

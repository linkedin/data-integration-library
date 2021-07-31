// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.filter;

import com.linkedin.cdi.util.JsonIntermediateSchema;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MultistageSchemaBasedFilterTest {

  @Test
  public void testFilter() {
    JsonIntermediateSchema schema = Mockito.mock(JsonIntermediateSchema.class);
    MultistageSchemaBasedFilter filter = new MultistageSchemaBasedFilter(schema);
    Assert.assertEquals(filter.filter("input"), null);
  }
}
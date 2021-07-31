// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.filter;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import java.lang.reflect.Method;
import com.linkedin.cdi.util.JsonElementTypes;
import com.linkedin.cdi.util.JsonIntermediateSchema;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


@Test
public class JsonSchemaBasedFilterTest {
  private Gson gson = new Gson();
  private JsonSchemaBasedFilter JsonSchemaBasedFilter;

  @BeforeMethod
  public void Setup(){
    JsonIntermediateSchema schema = Mockito.mock(JsonIntermediateSchema.class);
    JsonSchemaBasedFilter = new JsonSchemaBasedFilter(schema);
  }

  /**
   * Test filter(JsonIntermediateSchema.JisDataType dataType, JsonElement input)
   */
  @Test
  public void testFilterWithJsonJsonElementParameter() throws Exception {
    Method method = JsonSchemaBasedFilter.class.getDeclaredMethod("filter", JsonIntermediateSchema.JisDataType.class, JsonElement.class);
    method.setAccessible(true);

    JsonIntermediateSchema.JisDataType jisDataType = Mockito.mock(JsonIntermediateSchema.JisDataType.class);
    when(jisDataType.isPrimitive()).thenReturn(false);

    JsonElement jsonElement = gson.fromJson("[]", JsonElement.class);
    when(jisDataType.getType()).thenReturn(JsonElementTypes.ARRAY);
    when(jisDataType.getItemType()).thenReturn(jisDataType);
    Assert.assertEquals(method.invoke(JsonSchemaBasedFilter, jisDataType, jsonElement), jsonElement);

    when(jisDataType.getType()).thenReturn(JsonElementTypes.OBJECT);
    Assert.assertEquals(method.invoke(JsonSchemaBasedFilter, jisDataType, jsonElement), null);
  }
}

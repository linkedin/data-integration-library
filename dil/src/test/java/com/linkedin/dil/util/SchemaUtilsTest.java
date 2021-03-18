// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.dil.util;

import java.util.Arrays;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SchemaUtilsTest {

  @Test
  public void testIsValidOutputSchema() {
    // valid schema
    List<String> schemaColumns = Arrays.asList("a", "b");
    List<String> sourceColumns = Arrays.asList("a", "B", "C");
    Assert.assertTrue(SchemaUtils.isValidOutputSchema(schemaColumns, sourceColumns));

    // valid schema
    schemaColumns = Arrays.asList("a", "c");
    sourceColumns = Arrays.asList("a", "B", "C");
    Assert.assertTrue(SchemaUtils.isValidOutputSchema(schemaColumns, sourceColumns));

    // some columns in the schema is nowhere to be found in the source
    schemaColumns = Arrays.asList("a", "e");
    sourceColumns = Arrays.asList("a", "B", "C");
    Assert.assertFalse(SchemaUtils.isValidOutputSchema(schemaColumns, sourceColumns));

    // order mismatch
    schemaColumns = Arrays.asList("c", "a", "b");
    sourceColumns = Arrays.asList("a", "B", "C");
    Assert.assertFalse(SchemaUtils.isValidOutputSchema(schemaColumns, sourceColumns));
  }
}
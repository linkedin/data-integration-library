// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.util;

import java.util.List;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class SchemaUtils {

  // Following best practices for utility classes to have a private constructor
  private SchemaUtils() {
    throw new IllegalStateException("Utility class");
  }

  /**
   * A schema is valid when all its valid schema are present in source and in the same order.
   * Column names' matching is case insensitive.
   * @param schemaColumns column names defined in the output schema
   * @param sourceColumns column names at the source
   * @return true if all columns are matching and false other wise
   *
   *
   * Example 1: schemaColumns: [A, c], sourceColumns: [a, B, C] ==> true
   * Example 2: schemaColumns: [A, e], sourceColumns: [a, B, C] ==> false
   *
   */
  public static boolean isValidOutputSchema(List<String> schemaColumns, List<String> sourceColumns) {
    int i = 0;
    int j = 0;
    while (i < sourceColumns.size() && j < schemaColumns.size()) {
      if (sourceColumns.get(i).equalsIgnoreCase(schemaColumns.get(j))) {
        j++;
      }
      i++;
    }
    boolean isValidSchema = j == schemaColumns.size();
    if (!isValidSchema) {
      log.error(
          "Schema columns and source columns do not match: " + "undefined columns in schema or column order mismatch");
      log.debug("Schema column: {}", schemaColumns);
      log.debug("Source columns: {}", sourceColumns);
    }
    return isValidSchema;
  }
}

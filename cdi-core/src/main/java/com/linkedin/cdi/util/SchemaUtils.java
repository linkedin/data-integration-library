// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.util;

import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import java.util.LinkedList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.cdi.configuration.StaticConstants.*;


public class SchemaUtils {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaUtils.class);
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
      LOG.error(
          "Schema columns and source columns do not match: " + "undefined columns in schema or column order mismatch");
      LOG.debug("Schema column: {}", schemaColumns);
      LOG.debug("Source columns: {}", sourceColumns);
    }
    return isValidSchema;
  }

  /**
   * Check if the field definition at the JSON path is nullable. For nested structures,
   * if the top level is nullable, the field is nullable.
   *
   * Any Json path not exist in the schema is nullable
   *
   * @param jsonPath a JSON path list
   * @param schemaArray a JSON schema array
   * @return true if nullable
   */
  public static boolean isNullable(List<String> jsonPath, JsonArray schemaArray) {
    if (jsonPath.size() == 0 || schemaArray == null || schemaArray.size() == 0) {
      return false;
    }
    JsonArray isNullable = JsonUtils.filter(KEY_WORD_COLUMN_NAME, jsonPath.get(0), schemaArray, KEY_WORD_IS_NULLABLE);
    if (isNullable.size() == 0 || isNullable.get(0) == JsonNull.INSTANCE || isNullable.get(0).getAsBoolean()) {
      return true;
    }
    List<String> subPath = jsonPath.subList(1, jsonPath.size());
    JsonArray dataType = JsonUtils.filter(KEY_WORD_COLUMN_NAME, jsonPath.get(0), schemaArray, KEY_WORD_DATA_TYPE);
    if (dataType.size() > 0 && dataType.get(0) != JsonNull.INSTANCE) {
      // try sub record
      JsonArray typeDef = JsonUtils.filter(KEY_WORD_TYPE, KEY_WORD_RECORD, dataType, KEY_WORD_VALUES);
      if (typeDef.size() > 0 && typeDef.get(0) != JsonNull.INSTANCE) {
        return isNullable(subPath, typeDef.get(0).getAsJsonArray());
      }

      // try sub array
      typeDef = JsonUtils.filter(KEY_WORD_TYPE, KEY_WORD_ARRAY, dataType, KEY_WORD_ITEMS + "." + KEY_WORD_DATA_TYPE + "." + KEY_WORD_VALUES);
      if (typeDef.size() > 0 && typeDef.get(0) != JsonNull.INSTANCE) {
        return isNullable(subPath, typeDef.get(0).getAsJsonArray());
      }
    }
    // no more sub element in schema definition
    return subPath.size() > 0;
  }

  /**
   * Check if the field definition at the JSON path is nullable. For nested structures,
   * if the top level is nullable, the field is nullable.
   *
   * Any Json path not exist in the schema is nullable
   *
   * @param jsonPath a JSON path string separated by "."
   * @param schemaArray a JSON schema array
   * @return true if nullable
   */
  public static boolean isNullable(String jsonPath, JsonArray schemaArray) {
    return isNullable(Lists.newArrayList(jsonPath.split("\\.")), schemaArray);
  }
}

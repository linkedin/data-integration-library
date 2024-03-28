// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.util;

import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
   * A schema definition is valid when all defined columns are present in the source.
   * - To determine existence, column names are case insensitive.
   * - The order of columns can be different.
   * - Defined columns can contain extra ones to allow derived fields.
   *
   * @param definedColumns column names defined in the output schema
   * @param sourceColumns column names at the source
   * @param derivedFields the number of derived fields
   * @return true if first N columns are all existing in source and false other wise
   *
   *
   * Example 1: definedColumns: [A, c], sourceColumns: [a, B, C] ==&gt; true, B in source will be ignored in projection
   * Example 2: definedColumns: [A, e], sourceColumns: [a, B, C] ==&gt; false
   * Example 3: definedColumns: [A, B, C], sourceColumns: [A, B] ==&gt; true, C is assumed to be a derived field
   *
   */
  public static boolean isValidSchemaDefinition(
      List<String> definedColumns,
      List<String> sourceColumns,
      int derivedFields) {
    Set<String> columns = new HashSet<>();
    sourceColumns.forEach(x -> columns.add(x.toLowerCase()));

    for (int i = 0; i < definedColumns.size() - derivedFields; i++) {
      if (!columns.contains(definedColumns.get(i).toLowerCase())) {
        LOG.error("Defined Schema does not match source.");
        LOG.error("Schema column: {}", definedColumns);
        LOG.error("Source columns: {}", sourceColumns);
        return false;
      }
    }
    return true;
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

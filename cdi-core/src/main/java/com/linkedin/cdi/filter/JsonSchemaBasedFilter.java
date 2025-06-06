// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.filter;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.util.Map;
import com.linkedin.cdi.util.JsonElementTypes;
import com.linkedin.cdi.util.JsonIntermediateSchema;
import com.linkedin.cdi.util.JsonUtils;


/**
 * Filter Json records by Json Intermediate schema
 *
 * TODO handle UNIONs
 *
 * @author kgoodhop, chrli
 *
 */
public class JsonSchemaBasedFilter extends MultistageSchemaBasedFilter<JsonObject> {

  public JsonSchemaBasedFilter(JsonIntermediateSchema schema) {
    super(schema);
  }

  /**
   * top level filter function
   * @param input the input row object
   * @return the filtered row object
   */
  @Override
  public JsonObject filter(JsonObject input) {
    return this.filter(schema, input);
  }

  private JsonElement filter(JsonIntermediateSchema.JisDataType dataType, JsonElement input) {
    if (dataType.isPrimitive()) {
      return input.isJsonPrimitive() ? filter(dataType, input.getAsJsonPrimitive()) : null;
    } else if (dataType.getType() == JsonElementTypes.RECORD) {
      return filter(dataType.getChildRecord(), input.getAsJsonObject());
    } else if (dataType.getType() == JsonElementTypes.ARRAY) {
      return filter(dataType.getItemType(), input.getAsJsonArray());
    } else if (dataType.getType() == JsonElementTypes.MAP) {
      JsonObject output = new JsonObject();
      for (Map.Entry<String, JsonElement> entry: input.getAsJsonObject().entrySet()) {
        output.add(entry.getKey(), filter(dataType.getItemType().getChildRecord(), entry.getValue().getAsJsonObject()));
      }
      return output;
    }
    return null;
  }

  private JsonPrimitive filter(JsonIntermediateSchema.JisDataType dataType, JsonPrimitive input) {
    return dataType.isPrimitive() ? JsonUtils.deepCopy(input).getAsJsonPrimitive() : null;
  }

  /**
   * process the JsonArray
   *
   * @param dataType should be the item type of the JsonArray
   * @param input JsonArray object
   * @return filtered JsonArray object
   */
  private JsonArray filter(JsonIntermediateSchema.JisDataType dataType, JsonArray input) {
    JsonArray output = new JsonArray();
    for (JsonElement element: input) {
      output.add(filter(dataType, element));
    }
    return output;
  }

  private JsonObject filter(JsonIntermediateSchema schema, JsonObject input) {
    JsonObject output = new JsonObject();
    for (Map.Entry<String, JsonElement> entry: input.entrySet()) {
      if (schema.getColumns().containsKey(entry.getKey())) {
        output.add(entry.getKey(),
            filter(schema.getColumns().get(entry.getKey()).getDataType(), entry.getValue()));
      }
    }
    return output;
  }
}

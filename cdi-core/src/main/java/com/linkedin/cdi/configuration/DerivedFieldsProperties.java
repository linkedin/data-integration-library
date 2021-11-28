// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.configuration;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import java.util.HashMap;
import java.util.Map;
import org.apache.gobblin.configuration.State;
import org.joda.time.DateTimeZone;

import static com.linkedin.cdi.configuration.StaticConstants.*;


public class DerivedFieldsProperties extends JsonArrayProperties {
  /**
   * Constructor with implicit default value
   * @param config property name
   */
  DerivedFieldsProperties(String config) {
    super(config);
  }

  @Override
  public boolean isValid(State state) {
    if (super.isValid(state) && !isBlank(state)) {
      // Derived fields should meet general JsonArray configuration requirements
      // and contain only JsonObject items that each has a "name" element and a "formula" element
      JsonArray derivedFields = GSON.fromJson(state.getProp(getConfig()), JsonArray.class);
      for (JsonElement field : derivedFields) {
        if (!field.isJsonObject()
            || !field.getAsJsonObject().has(KEY_WORD_NAME)
            || !field.getAsJsonObject().has(KEY_WORD_FORMULA)
            || !field.getAsJsonObject().get(KEY_WORD_FORMULA).isJsonObject()) {
          return false;
        }

        JsonObject formula = field.getAsJsonObject().get(KEY_WORD_FORMULA).getAsJsonObject();
        if (formula.has(KEY_WORD_TYPE) && formula.get(KEY_WORD_TYPE).getAsString().equals(KEY_WORD_EPOC)) {
          if (formula.has(KEY_WORD_TIMEZONE)) {
            String timezone = formula.get(KEY_WORD_TIMEZONE).getAsString();
            try {
              DateTimeZone.forID(timezone);
            } catch (Exception e) {
              return false;
            }
          }
        }
      }
    }
    return super.isValid(state);
  }

  /**
   * Sample derived field configuration:
   * [{"name": "activityDate", "formula": {"type": "epoc", "source": "fromDateTime", "format": "yyyy-MM-dd'T'HH:mm:ss'Z'"}}]
   *
   * Currently, only "epoc" and "string" are supported as derived field type.
   * For epoc type:
   * - Data will be saved as milliseconds in long data type.
   * - And the source data is supposed to be a date formatted as a string.
   *
   * TODO: support more types.
   *
   * @return derived fields and their definitions
   */
  public Map<String, Map<String, String>> getAsMap(State state) {
    if (isBlank(state) || !isValid(state)) {
      return new HashMap<>();
    }

    Map<String, Map<String, String>> derivedFields = new HashMap<>();
    for (JsonElement field: get(state)) {
      // change the formula part, which is JsonObject, into map
      derivedFields.put(field.getAsJsonObject().get(KEY_WORD_NAME).getAsString(),
          GSON.fromJson(
              field.getAsJsonObject().get(KEY_WORD_FORMULA).getAsJsonObject().toString(),
              new TypeToken<HashMap<String, String>>() { }.getType()));
    }

    return derivedFields;
  }
}

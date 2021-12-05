// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.configuration;

import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.linkedin.cdi.util.DateTimeUtils;
import com.linkedin.cdi.util.JsonUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gobblin.configuration.State;
import org.joda.time.DateTimeZone;

import static com.linkedin.cdi.configuration.StaticConstants.*;

/**
 * Watermark definitions
 */
public class WatermarkProperties extends JsonArrayProperties {
  final private static Pair<String, String> DATETIME_WATERMARK_DEFAULT = new ImmutablePair<String, String>("2020-01-01", "P0D");

  /**
   * Constructor with implicit default value
   * @param config property name
   */
  WatermarkProperties(String config) {
    super(config);
  }

  @Override
  public boolean isValid(State state) {
    final List<String> types = Lists.newArrayList("unit", "datetime");
    if (super.isValid(state) && !isBlank(state)) {
      JsonArray value = GSON.fromJson(state.getProp(getConfig()), JsonArray.class);
      if (value.size() == 0) {
        return false;
      }
      int dateTimeWatermarks = 0;
      int unitWatermarks = 0;
      for (JsonElement def: value) {
        if (!def.isJsonObject()) {
          return false;
        }

        if (!def.getAsJsonObject().has(KEY_WORD_NAME) || !def.getAsJsonObject().has(KEY_WORD_TYPE)) {
          return false;
        }

        String type = def.getAsJsonObject().get(KEY_WORD_TYPE).getAsString();
        if (types.stream().noneMatch(t -> t.equals(type))) {
          return false;
        }

        // a datetime watermark must have a "range" element, and the element
        // must have a "from" and a "to"
        if (type.equalsIgnoreCase("datetime")) {
          dateTimeWatermarks++;
          if (!def.getAsJsonObject().has(KEY_WORD_RANGE) || !def.getAsJsonObject().get(KEY_WORD_RANGE).isJsonObject()) {
            return false;
          }
          JsonObject range = def.getAsJsonObject().get(KEY_WORD_RANGE).getAsJsonObject();
          if (!range.has(KEY_WORD_FROM) || ! range.has(KEY_WORD_TO)) {
            return false;
          }

          // FROM has to be either a valid date time string or a valid pattern
          String from = range.get(KEY_WORD_FROM).getAsString();
          if (!DateTimeUtils.check(from)
              && !from.matches(REGEXP_TIME_DURATION_PATTERN)) {
            return false;
          }

          // check timezone string in the FROM value
          if (from.matches(REGEXP_TIME_DURATION_PATTERN)) {
            if (from.contains("\\.")) {
              try {
                DateTimeZone.forID(from.split("\\.")[1]);
              } catch (Exception e) {
                return false;
              }
            }
          }

          // TO has to be either a valid date time string or a valid pattern, including "-"
          String to = range.get(KEY_WORD_TO).getAsString();
          if (!to.equals("-")
              && !to.matches(REGEXP_TIME_DURATION_PATTERN)
              && !DateTimeUtils.check(to)) {
            return false;
          }

          // check timezone string in the TO value
          if (to.matches(REGEXP_TIME_DURATION_PATTERN)) {
            if (to.contains("\\.")) {
              try {
                DateTimeZone.forID(to.split("\\.")[1]);
              } catch (Exception e) {
                return false;
              }
            }
          }
        }

        // a unit watermark must have a "units" element, and the element
        // can be an array or a string of comma separated values
        if (type.equalsIgnoreCase("unit")) {
          unitWatermarks++;
          if (!def.getAsJsonObject().has(KEY_WORD_UNITS)) {
            return false;
          }

          JsonElement units = def.getAsJsonObject().get(KEY_WORD_UNITS);
          if (!units.isJsonPrimitive() && !units.isJsonArray()) {
            return false;
          }
        }

        if (dateTimeWatermarks > 1 || unitWatermarks > 1) {
          return false;
        }
      }
    }
    return super.isValid(state);
  }

  /**
   * Parse out the date range of the datetime watermark
   * @param state state object
   * @return date range
   */
  public Pair<String, String> getRange(State state) {
    JsonArray dateWatermark = JsonUtils.filter(KEY_WORD_TYPE, "datetime", get(state));
    if (dateWatermark.isJsonNull()) {
      return DATETIME_WATERMARK_DEFAULT;
    }
    JsonObject range = dateWatermark.get(0).getAsJsonObject().get(KEY_WORD_RANGE).getAsJsonObject();
    return new ImmutablePair<>(range.get(KEY_WORD_FROM).getAsString(), range.get(KEY_WORD_TO).getAsString());
  }

  /**
   * Parse out the units of unit watermark if exist
   * @param state state object
   * @return units
   */
  public List<String> getUnits(State state) {
    JsonArray unitWatermark = JsonUtils.filter(KEY_WORD_TYPE, "unit", get(state));
    if (unitWatermark.isJsonNull()) {
      return new ArrayList<>();
    }

    // units can be a comma delimited string or a Json array of strings
    JsonElement units = unitWatermark.get(0).getAsJsonObject().get("units");
    if (units.isJsonArray()) {
      List<String> unitList = new ArrayList<>();
      for( JsonElement unit: units.getAsJsonArray()) {
        unitList.add(unit.getAsString());
      }
      return unitList;
    } else if (units.isJsonPrimitive()) {
      List<String> unitList = Lists.newArrayList();
      Arrays.stream(units.getAsString().split(KEY_WORD_COMMA)).forEach(x -> unitList.add(x.trim()));
      return unitList;
    }
    return null;
  }
}

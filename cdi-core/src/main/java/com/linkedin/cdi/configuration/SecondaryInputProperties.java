// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.configuration;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.linkedin.cdi.util.HdfsReader;
import com.linkedin.cdi.util.JsonUtils;
import java.util.HashMap;
import java.util.Map;
import org.apache.gobblin.configuration.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.cdi.configuration.StaticConstants.*;


public class SecondaryInputProperties extends JsonArrayProperties {
  private static final Logger LOG = LoggerFactory.getLogger(SecondaryInputProperties.class);
  final private static int RETRY_DELAY_IN_SEC_DEFAULT = 300;
  final private static int RETRY_COUNT_DEFAULT = 3;

  /**
   * Constructor with implicit default value
   * @param config property name
   */
  SecondaryInputProperties(String config) {
    super(config);
  }

  /**
   * Check if authentication is configured in secondary input
   * @return true if secondary input contains an authentication definition
   */
  public boolean isAuthenticationEnabled(State state) {
    for (JsonElement entry: get(state)) {
      if (JsonUtils.get(KEY_WORD_CATEGORY, entry.getAsJsonObject()) != JsonNull.INSTANCE) {
        if (JsonUtils.get(KEY_WORD_CATEGORY, entry.getAsJsonObject()).getAsString()
              .equalsIgnoreCase(KEY_WORD_AUTHENTICATION)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Read authentication and activation secondary input records and payload definitions (not records)
   *
   * @return a set of JsonArrays of data read from locations specified in SECONDARY_INPUT
   *         property organized by category, in a Map<String, JsonArray> structure
   */
  public Map<String, JsonArray> readAllContext(State state) {
    Map<String, JsonArray> secondaryInputs = new HashMap<>();
    for (JsonElement entry: get(state)) {
      if (!entry.getAsJsonObject().has(KEY_WORD_PATH)) {
        continue;
      }

      String category = entry.getAsJsonObject().has(KEY_WORD_CATEGORY)
          ? entry.getAsJsonObject().get(KEY_WORD_CATEGORY).getAsString()
          : KEY_WORD_ACTIVATION;

      JsonArray categoryData = secondaryInputs.computeIfAbsent(category, x -> new JsonArray());
      if (category.equalsIgnoreCase(KEY_WORD_ACTIVATION) || category.equalsIgnoreCase(KEY_WORD_AUTHENTICATION)) {
        categoryData.addAll(new HdfsReader(state).readSecondary(entry.getAsJsonObject()));
      }

      if (entry.getAsJsonObject().has(KEY_WORD_PATH) && category.equalsIgnoreCase(KEY_WORD_PAYLOAD)) {
        categoryData.add(entry);
      }

    }
    return secondaryInputs;
  }
}

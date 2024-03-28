// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.configuration;

import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.linkedin.cdi.util.HdfsReader;
import com.linkedin.cdi.util.JsonUtils;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gobblin.configuration.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.cdi.configuration.StaticConstants.*;


public class SecondaryInputProperties extends JsonArrayProperties {
  private static final Logger LOG = LoggerFactory.getLogger(SecondaryInputProperties.class);
  final private static int RETRY_DELAY_IN_SEC_DEFAULT = 300;
  final private static int RETRY_COUNT_DEFAULT = 3;
  final private static List<String> CATEGORY_NAMES = Lists.newArrayList();
  final public static String CATEGORY = KEY_WORD_CATEGORY;

  public enum Categories {
    ACTIVATION(KEY_WORD_ACTIVATION),
    AUTHENTICATION(KEY_WORD_AUTHENTICATION),
    PAYLOAD(KEY_WORD_PAYLOAD),
    VALIDATION(KEY_WORD_VALIDATION);
    public final String name;
    /**
     * initialize the enum item with a default name
     * @param name the title of the enum item
     */
    Categories(String name) {
      this.name = name;
    }

    public boolean equals(String category) {
      return category.equalsIgnoreCase(name);
    }
  }

  /**
   * Constructor with implicit default value
   * @param config property name
   */
  SecondaryInputProperties(String config) {
    super(config);
    Arrays.stream(Categories.values()).forEach(x -> CATEGORY_NAMES.add(x.name));
  }

  @Override
  public boolean isValid(State state) {
    if (super.isValid(state) && !isBlank(state)) {
      JsonArray value = GSON.fromJson(state.getProp(getConfig()), JsonArray.class);

      // check path fields, make sure they are present
      for (JsonElement entry : value) {
        if (!entry.isJsonObject() || !entry.getAsJsonObject().has(KEY_WORD_PATH)) {
          return false;
        }
      }

      // check categories, make sure they are spelled properly
      for (JsonElement si : value) {
        if (JsonUtils.get(CATEGORY, si.getAsJsonObject()) != JsonNull.INSTANCE) {
          String category = JsonUtils.get(CATEGORY, si.getAsJsonObject()).getAsString();
          if (CATEGORY_NAMES.stream().noneMatch(x -> x.equals(category))) {
            return false;
          }
        }
      }
    }
    return super.isValid(state);
  }

  /**
   * reads the authentication content
   *
   * @param state the state object
   * @return  the secondary input entries
   */
  public Map<String, JsonArray> readAuthenticationToken(State state) {
    Map<String, JsonArray> secondaryInputs = new HashMap<>();
    JsonArray categoryData = secondaryInputs.computeIfAbsent(Categories.AUTHENTICATION.name, x -> new JsonArray());
    categoryData.addAll(new HdfsReader(state).readSecondary(getAuthenticationDefinition(state).getAsJsonObject()));
    return secondaryInputs;
  }

  /**
   * Read authentication and activation secondary input records and payload definitions (not records)
   *
   * @return a set of JsonArrays of data read from locations specified in SECONDARY_INPUT
   *         property organized by category, in a Map&lt;String, JsonArray&gt; structure
   */
  public Map<String, JsonArray> readAllContext(State state) {
    Map<String, JsonArray> secondaryInputs = new HashMap<>();
    for (JsonElement entry: get(state)) {
      if (!entry.getAsJsonObject().has(KEY_WORD_PATH)) {
        continue;
      }

      String category = entry.getAsJsonObject().has(CATEGORY)
          ? entry.getAsJsonObject().get(CATEGORY).getAsString()
          : Categories.ACTIVATION.name;

      JsonArray categoryData = secondaryInputs.computeIfAbsent(category, x -> new JsonArray());
      if (Categories.ACTIVATION.equals(category) || Categories.AUTHENTICATION.equals(category)) {
        categoryData.addAll(new HdfsReader(state).readSecondary(entry.getAsJsonObject()));
      } else if (entry.getAsJsonObject().has(KEY_WORD_PATH)) {
        categoryData.add(entry);
      }
    }
    return secondaryInputs;
  }

  /**
   * Check if authentication is configured in secondary input
   * @return true if secondary input contains an authentication definition
   */
  public boolean isAuthenticationEnabled(State state) {
    return getAuthenticationDefinition(state).entrySet().size() > 0;
  }

  /**
   * Get the authentication part of the secondary input,
   * @param state state object
   * @return the authentication secondary input
   */
  private JsonObject getAuthenticationDefinition(State state) {
    for (JsonElement entry : get(state)) {
      if (entry.isJsonObject() && entry.getAsJsonObject().has(CATEGORY)) {
        String category = entry.getAsJsonObject().get(CATEGORY).getAsString();
        if (Categories.AUTHENTICATION.equals(category)) {
          return entry.getAsJsonObject();
        }
      }
    }
    return new JsonObject();
  }

  /**
   *  This method populates the retry parameters (delayInSec, retryCount) via the secondary input.
   *   These values are used to retry connection whenever the "authentication" type category is defined and the token hasn't
   *   been populated yet. If un-defined, they will retain the default values as specified by RETRY_DEFAULT_DELAY and
   *   RETRY_DEFAULT_COUNT.
   *
   *   For e.g.
   *   ms.secondary.input : "[{"path": "/util/avro_retry", "fields": ["uuid"],
   *   "category": "authentication", "retry": {"delayInSec" : "1", "retryCount" : "2"}}]"
   * @param state the state record
   * @return the retry delay and count in a map structure
   */
  public Map<String, Long> getAuthenticationRetry(State state) {
    long retryDelay = RETRY_DELAY_IN_SEC_DEFAULT;
    long retryCount = RETRY_COUNT_DEFAULT;
    Map<String, Long> retry = new HashMap<>();

    if (JsonUtils.get(KEY_WORD_RETRY, getAuthenticationDefinition(state)) != JsonNull.INSTANCE) {
      JsonObject retryFields = getAuthenticationDefinition(state).get(KEY_WORD_RETRY).getAsJsonObject();
      retryDelay = retryFields.has(KEY_WORD_RETRY_DELAY_IN_SEC)
          ? retryFields.get(KEY_WORD_RETRY_DELAY_IN_SEC).getAsLong() : retryDelay;
      retryCount = retryFields.has(KEY_WORD_RETRY_COUNT)
          ? retryFields.get(KEY_WORD_RETRY_COUNT).getAsLong() : retryCount;
    }

    retry.put(KEY_WORD_RETRY_DELAY_IN_SEC, retryDelay);
    retry.put(KEY_WORD_RETRY_COUNT, retryCount);
    return retry;
  }
}

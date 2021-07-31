// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.util;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;


/**
 * Utility functions for string manipulation
 */
public interface VariableUtils {
  Gson GSON = new Gson();
  String OPENING_RE = "\\{\\{";
  String OPENING = "{{";
  String CLOSING = "}}";
  Pattern PATTERN = Pattern.compile(OPENING_RE + "([a-zA-Z0-9_\\-.$]+)" + CLOSING);

  /**
   * Replace placeholders or variables in a JsonObject
   *
   * @param templateJsonObject the template JsonObject with placeholders
   * @param parameters the replacement values
   * @return the replaced JsonObject
   * @throws UnsupportedEncodingException
   */
  static JsonObject replace(JsonObject templateJsonObject, JsonObject parameters) throws UnsupportedEncodingException {
    String replacedString = replaceWithTracking(templateJsonObject.toString(), parameters, false).getKey();
    return GSON.fromJson(replacedString, JsonObject.class);
  }

  /**
   * Replace placeholders or variables in a JsonObject
   *
   * @param templateJsonObject the template JsonObject with placeholders
   * @param parameters the replacement values
   * @param encode whether to encode the value string, note this function will not encode
   *               the template string in any case, the encoding only applies to the replacement values
   * @return the replaced JsonObject
   * @throws UnsupportedEncodingException
   */
  static JsonObject replace(JsonObject templateJsonObject, JsonObject parameters, Boolean encode)
      throws UnsupportedEncodingException {
    String replacedString = replaceWithTracking(templateJsonObject.toString(), parameters, encode).getKey();
    return GSON.fromJson(replacedString, JsonObject.class);
  }

  /**
   *
   * @param templateString a template string with placeholders or variables
   * @param parameters the replacement values coded in a JsonObject format
   * @return a pair made of replaced string and whatever parameters that were not used
   * @throws UnsupportedEncodingException
   */
  static Pair<String, JsonObject> replaceWithTracking(String templateString, JsonObject parameters)
      throws UnsupportedEncodingException {
    return replaceWithTracking(templateString, parameters, false);
  }

  /**
   *
   * @param templateString a template string with placeholders or variables
   * @param parameters the replacement values coded in a JsonObject format
   * @param encode whether to encode the value string, note this function will not encode
   *               the template string in any case, the encoding only applies to the replacement values
   * @return a pair made of replaced string and whatever parameters that were not used
   * @throws UnsupportedEncodingException
   */
  static Pair<String, JsonObject> replaceWithTracking(String templateString, JsonObject parameters, Boolean encode)
      throws UnsupportedEncodingException {
    String replacedString = templateString;
    JsonObject remainingParameters = new JsonObject();

    List<String> variables = getVariables(templateString);

    for (Map.Entry<String, JsonElement> entry : parameters.entrySet()) {
      if (variables.contains(entry.getKey())) {
        replacedString = replacedString.replace(OPENING + entry.getKey() + CLOSING,
            encode ? URLEncoder.encode(entry.getValue().getAsString(), "UTF-8") : entry.getValue().getAsString());
      } else {
        remainingParameters.add(entry.getKey(), entry.getValue());
      }
    }
    return new ImmutablePair<>(replacedString, remainingParameters);
  }

  /**
   * retrieve a list of placeholders or variables from the template, placeholders or variables are
   * identified by alpha numeric strings surrounded by {{}}
   *
   * @param templateString the template with placeholders or variables
   * @return a list of placeholders or variables
   */
  static List<String> getVariables(String templateString) {
    List<String> paramList = Lists.newArrayList();
    Matcher matcher = PATTERN.matcher(templateString);
    while (matcher.find()) {
      paramList.add(matcher.group(1));
    }
    return paramList;
  }

  /**
   * Validates if a string has variables
   * @param templateString
   * @return true if {{}} is found else false
   */
  static boolean hasVariable(String templateString) {
    return PATTERN.matcher(templateString).find();
  }
}

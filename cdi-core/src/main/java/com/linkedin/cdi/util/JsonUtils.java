// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.util;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.testng.Assert;


public interface JsonUtils {
  Gson GSON = new Gson();
  Gson GSON_WITH_SUPERCLASS_EXCLUSION = new GsonBuilder()
      .setExclusionStrategies(new SuperclassExclusionStrategy())
      .create();

  /**
   * This deepCopy is a workaround. When it is possible to upgrade Gson to 2.8.1+,
   * we shall change code to use Gson deepCopy.
   *
   * This function is intended to use use small Json, like schema objects. It is not
   * suitable to deep copy large Json objects.
   *
   * @param source the source Json object, can be JsonArray, JsonObject, or JsonPrimitive
   * @return the deeply copied Json object
   */
  static JsonElement deepCopy(JsonElement source) {
    return GSON.fromJson(source.toString(), source.getClass());
  }

  /**
   * Check if JsonObject A contains everything in b
   * @param superObject the super set JsonObject
   * @param subObject the sub set JsonObject
   * @return if superObject doesn't have an element in b, or the value of an element in superObject differs with
   * the same element in b, return false, else return true
   */
  static boolean contains(JsonObject superObject, JsonObject subObject) {
    for (Map.Entry<String, JsonElement> entry: subObject.entrySet()) {
      if (!superObject.has(entry.getKey())
          || !superObject.get(entry.getKey()).toString().equalsIgnoreCase(entry.getValue().toString())) {
        return false;
      }
    }
    return true;
  }

  /**
   * Check if JsonObject A contains everything in b
   * @param superString the super set JsonObject
   * @param subObject the sub set JsonObject
   * @return if a doesn't have an element in b, or the value of an element in a differs with
   * the same element in b, return false, else return true
   */
  static boolean contains(String superString, JsonObject subObject) {
    JsonObject a = GSON.fromJson(superString, JsonObject.class);
    return contains(a, subObject);
  }

  /**
   * Check if a JsonObject contains a key and compares its value with the given value
   * @return if jsonObject doesn't have an element key, or its value of the element key differs with
   * the value given, return false, else return true
   */
  static boolean getAndCompare(String key, String value, JsonObject jsonObject) {
    return jsonObject.has(key) && jsonObject.get(key).getAsString().equals(value);
  }

  /**
   * Replace parts of Original JsonObject with substitutes
   * @param origObject the original JsonObject
   * @param newComponent the substitution values
   * @return the replaced JsonObject
   */
  static JsonObject replace(JsonObject origObject, JsonObject newComponent) {
    JsonObject replacedObject = new JsonObject();
    for (Map.Entry<String, JsonElement> entry: origObject.entrySet()) {
      if (newComponent.has(entry.getKey())) {
        replacedObject.add(entry.getKey(), newComponent.get(entry.getKey()));
      } else {
        replacedObject.add(entry.getKey(), entry.getValue());
      }
    }
    return replacedObject;
  }

  /**
   * This function makes up the inefficiency in GSON by creating an
   * JsonObject and add a pair of properties to it, and return the newly
   * created JsonObject
   *
   * @param key the property key
   * @param value the property value
   * @return the newly created JsonObject
   */
  static JsonObject createAndAddProperty(String key, String value) {
    JsonObject newObject = new JsonObject();
    newObject.addProperty(key, value);
    return newObject;
  }

  /**
   * This function makes up the inefficiency in GSON by creating an
   * JsonObject and add a new element to it, and return the newly
   * created JsonObject
   *
   * @param name the name of the new element
   * @param element the new element
   * @return the newly created JsonObject
   */
  static JsonObject createAndAddElement(String name, JsonElement element) {
    JsonObject newObject = new JsonObject();
    newObject.add(name, element);
    return newObject;
  }

  /**
   * From an array of KV pairs, retrieve the record value
   * @param key key name
   * @param kvPairs the array or KV pairs
   * @return the value if the key exists
   */
  static JsonElement get(final String key, final JsonArray kvPairs) {
    for (JsonElement element: kvPairs) {
      if (element.isJsonObject() && element.getAsJsonObject().has(key)) {
        return element.getAsJsonObject().get(key);
      }
    }
    return JsonNull.INSTANCE;
  }

  /**
   * From an array of JsonObjects, retrieve a value by searching by key-value pair, and
   * once the JsonObject is found, it returns the element located by the JsonPath,
   * specified by returnKey. This utility is mostly used for schema manipulations.
   *
   * @param searchKey key name to search in order to identify the JsonObject
   * @param value value to match in order to identify the JsonObject
   * @param returnKey the Json path to identify the return value
   * @param objArray the array of JsonObjects
   * @return the identified element of a JsonObject within the array
   */
  static JsonElement get(final String searchKey, final String value, final String returnKey, final JsonArray objArray) {
    for (JsonElement element: objArray) {
      if (element.isJsonObject()
          && element.getAsJsonObject().has(searchKey)
          && element.getAsJsonObject().get(searchKey).getAsString().equalsIgnoreCase(value)) {
        return get(returnKey, element.getAsJsonObject());
      }
    }
    return JsonNull.INSTANCE;
  }

  /**
   * Get a JsonElement from a JsonObject based on the given JsonPath
   *
   * @param row the record contains the data element
   * @param jsonPath the JsonPath (string) how to get the data element
   * @return the data element at the JsonPath position, or JsonNull if error
   */
  static JsonElement get(JsonObject row, String jsonPath) {
    return get(jsonPath, row);
  }

  /**
   * Check if a JsonElement is available in a JsonObject given a JsonPath
   * @param row the record
   * @param jsonPath the JsonPath (string) how to get the data element
   * @return true if present, false otherwise
   */
  static boolean has(JsonObject row, String jsonPath) {
    return !get(jsonPath, row).isJsonNull();
  }

  /**
   * Get a JsonElement from a JsonObject based on the given JsonPath
   *
   * @param nested the record contains the data element
   * @param jsonPath the JsonPath (string) how to get the data element
   * @return the data element at the JsonPath position, or JsonNull if error
   */
  static JsonElement get(String jsonPath, JsonObject nested) {
    Assert.assertNotNull(jsonPath);
    List<String> path = Lists.newArrayList(jsonPath.split("\\."));

    if (path.size() == 0 || nested == null || nested.isJsonNull()) {
      return JsonNull.INSTANCE;
    }
    return get(path.iterator(), nested);
  }

  /**
   * Get a JsonElement from an arbitrary JsonElement based on the given JsonPath
   *
   * @param nested the JsonElement to search
   * @param jsonPath the JsonPath (Iterator of String) how to get the data element
   * @return the data element at the JsonPath position, or JsonNull if error
   */
  static JsonElement get(Iterator<String> jsonPath, JsonElement nested) {
    if (nested.isJsonObject()) {
      return get(jsonPath, nested.getAsJsonObject());
    } else if (nested.isJsonArray()) {
      return get(jsonPath, nested.getAsJsonArray());
    }
    return jsonPath.hasNext() ? JsonNull.INSTANCE : nested;
  }

  /**
   * Get a JsonElement from a JsonObject based on the given JsonPath
   *
   * @param nested the JsonObject to search
   * @param jsonPath the JsonPath (Iterator of String) how to get the data element
   * @return the data element at the JsonPath position, or JsonNull if error
   */
  static JsonElement get(Iterator<String> jsonPath, JsonObject nested) {
    if (!jsonPath.hasNext()) {
      return nested;
    }
    String name = jsonPath.next();
    return nested != null && !nested.isJsonNull() && nested.has(name)
        ? get(jsonPath, nested.get(name))
        : JsonNull.INSTANCE;
  }

  /**
   * Get a JsonElement from a JsonArray based on the given JsonPath
   *
   * @param nested the JsonArray to search
   * @param jsonPath the JsonPath (Iterator of String) how to get the data element
   * @return the data element at the JsonPath position, or JsonNull if error
   */
  static JsonElement get(Iterator<String> jsonPath, JsonArray nested) {
    if (!jsonPath.hasNext()) {
      return nested;
    }
    String indexStr = jsonPath.next();
    try {
      int index = Integer.parseInt(indexStr);
      return nested != null && !nested.isJsonNull() && index >= 0 && index < nested.size()
          ? get(jsonPath, nested.get(index))
          : JsonNull.INSTANCE;
    } catch (Exception e) {
      return JsonNull.INSTANCE;
    }
  }

  /**
   * From an array of JsonObjects, filter by searching by key-value pair, and
   * once the JsonObject is found, it returns the element located by the JsonPath,
   * specified by returnKey, or the whole record if no returnKey is specified
   *
   * This function doesn't deepCopy the returned elements to avoid allocating extra
   * spaces.
   *
   * @param searchKey key name to search in order to identify the JsonObject
   * @param value value to match in order to identify the JsonObject
   * @param objArray the array of JsonObjects
   * @param returnKey the Json path to identify the return value
   * @return the filtered elements within the array
   */
  static JsonArray filter(final String searchKey, final String value, final JsonArray objArray, final String returnKey) {
    JsonArray output = new JsonArray();
    for (JsonElement element: objArray) {
      if (element.isJsonObject()
          && element.getAsJsonObject().has(searchKey)
          && element.getAsJsonObject().get(searchKey).getAsString().equalsIgnoreCase(value)) {
        if (StringUtils.isEmpty(returnKey)) {
          output.add(element);
        } else {
          output.add(get(returnKey, element.getAsJsonObject()));
        }
      }
    }
    return output;
  }

  /**
   * From an array of JsonObjects, filter by searching by key-value pair.
   *
   * @param searchKey key name to search in order to identify the JsonObject
   * @param value value to match in order to identify the JsonObject
   * @param objArray the array of JsonObjects
   * @return the filtered elements within the array
   */
  static JsonArray filter(final String searchKey, final String value, final JsonArray objArray) {
    return filter(searchKey, value, objArray, null);
  }

  /**
   * Filter out any null values in a JsonObject
   * @param input JsonObject with nulls
   * @return the filtered jsonObject
   */
  static JsonObject filterNull(final JsonObject input) {
    JsonObject output = new JsonObject();
    for (Map.Entry<String, JsonElement> entry : input.entrySet()) {
      String key = entry.getKey();
      JsonElement value = entry.getValue();
      if (!value.isJsonNull()) {
        output.add(key, value);
      }
    }
    return output;
  }
}

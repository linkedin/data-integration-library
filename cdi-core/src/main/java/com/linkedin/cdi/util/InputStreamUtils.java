// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.util;

import com.google.gson.JsonArray;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;


public interface InputStreamUtils {
  /**
   * Convert a list of strings to an InputStream
   * @param stringList a list of strings
   * @return an InputStream made of the list
   */
  static InputStream convertListToInputStream(List<String> stringList) {
    return CollectionUtils.isEmpty(stringList) ? null
        : new ByteArrayInputStream(String.join("\n", stringList).getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Convert a list of strings with a name to an InputStream of JsonArray
   * @param name the Json field name
   * @param stringList a list of strings
   * @return an InputStream made of the named list
   */
  static InputStream convertListToInputStream(String name, List<String> stringList) {
    if (CollectionUtils.isEmpty(stringList) || StringUtils.isBlank(name)) {
      return null;
    }
    JsonArray output = new JsonArray();
    for (String value: stringList) {
      output.add(JsonUtils.createAndAddProperty(name, value));
    }
    return new ByteArrayInputStream(output.toString().getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Extract the text from input stream using UTF-8 encoding
   * @param input the InputStream, which most likely is from an HttpResponse
   * @return the String extracted from InputStream, if the InputStream cannot be converted to a String
   * then an exception is thrown
   */
  static String extractText(InputStream input) throws IOException {
    return extractText(input, StandardCharsets.UTF_8.name());
  }

  /**
   * Extract the text from input stream using given character set encoding
   * @param input the InputStream, which most likely is from an HttpResponse
   * @param charSetName the character set name
   * @return the String extracted from InputStream, if the InputStream cannot be converted to a String
   * then an exception is thrown
   */
  static String extractText(InputStream input, String charSetName) throws IOException {
    return IOUtils.toString(input, charSetName);
  }
}

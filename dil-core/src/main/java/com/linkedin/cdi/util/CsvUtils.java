// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.util;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;


public interface CsvUtils {
  /**
   * Method to convert string to supplement unicode values if exists.
   * Ex: input u005c will be converted to \\u005c
   * @param value input string
   * @return unicode value
   */
  static String unescape(String value) {
    if (value != null) {
      value = value.toLowerCase();
      if (value.matches("^u[A-Fa-f0-9]{4}")) {
        return StringEscapeUtils.unescapeJava("\\" + value);
      }
      return value;
    }
    return StringUtils.EMPTY;
  }
}

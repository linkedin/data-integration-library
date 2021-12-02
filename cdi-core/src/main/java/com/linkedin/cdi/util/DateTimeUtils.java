// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.util;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import static com.linkedin.cdi.configuration.StaticConstants.*;


/**
 * a general datetime parsing utility
 *
 * Note: Joda supports only up to milliseconds, if data has microseconds, it will be truncated
 *
 * Note: Joda doesn't like "America/Los_Angeles", but rather it accepts PST or -08:00, therefore
 * long form timezone names are not supported.
 */

public interface DateTimeUtils {
  DateTimeFormatter DATE_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd");
  Map<String, DateTimeFormatter> FORMATS = new ImmutableMap.Builder<String, DateTimeFormatter>()
      .put("\\d{4}-\\d{2}-\\d{2}", DateTimeFormat.forPattern("yyyy-MM-dd"))
      .put("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))
      .put("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{1}", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.S"))
      .put("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{2}", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SS"))
      .put("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3}", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS"))
      .put("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{4}", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSS"))
      .put("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{5}", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSS"))
      .put("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{6}", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS"))
      .put("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}", DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss"))
      .put("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{1}", DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.S"))
      .put("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{2}", DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SS"))
      .put("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}", DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"))
      .put("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{4}", DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSS"))
      .put("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{5}", DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSS"))
      .put("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{6}", DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS"))
      .build();
  Map<String, DateTimeFormatter> FORMATS_WITH_ZONE = new ImmutableMap.Builder<String, DateTimeFormatter>()
      // date time string with timezone specified as +/- hh:mm
      .put("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}[-+]+\\d{2}:?\\d{2}", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ssZ"))
      .put("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{1}[-+]+\\d{2}:?\\d{2}", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SZ"))
      .put("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{2}[-+]+\\d{2}:?\\d{2}", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSZ"))
      .put("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3}[-+]+\\d{2}:?\\d{2}", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSZ"))
      .put("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{4}[-+]+\\d{2}:?\\d{2}", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSZ"))
      .put("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{5}[-+]+\\d{2}:?\\d{2}", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSZ"))
      .put("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{6}[-+]+\\d{2}:?\\d{2}", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSSZ"))

      // date time string with timezone specified with time zone ids, like PST
      // date time string with timezone specified with long form time zone ids, like America/Los_Angeles, is not working
      .put("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}[a-zA-Z\\/\\_]+", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ssz"))
      .put("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{1}[a-zA-Z\\/\\_]+", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.Sz"))
      .put("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{2}[a-zA-Z\\/\\_]+", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSz"))
      .put("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{3}[a-zA-Z\\/\\_]+", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSz"))
      .put("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{4}[a-zA-Z\\/\\_]+", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSz"))
      .put("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{5}[a-zA-Z\\/\\_]+", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSz"))
      .put("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{6}[a-zA-Z\\/\\_]+", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSSz"))

      // date time string with timezone specified as +/- hh:mm
      .put("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}[-+]+\\d{2}:?\\d{2}", DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ"))
      .put("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{1}[-+]+\\d{2}:?\\d{2}", DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SZ"))
      .put("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{2}[-+]+\\d{2}:?\\d{2}", DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSZ"))
      .put("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}[-+]+\\d{2}:?\\d{2}", DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ"))
      .put("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{4}[-+]+\\d{2}:?\\d{2}", DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSZ"))
      .put("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{5}[-+]+\\d{2}:?\\d{2}", DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSZ"))
      .put("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{6}[-+]+\\d{2}:?\\d{2}", DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSZ"))

      // date time string with timezone specified with short form time zone ids, like PST
      // date time string with timezone specified with long form time zone ids, like America/Los_Angeles, is not working
      .put("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}[a-zA-Z\\/\\_]+", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ssz"))
      .put("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{1}[a-zA-Z\\/\\_]+", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.Sz"))
      .put("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{2}[a-zA-Z\\/\\_]+", DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSz"))
      .put("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}[a-zA-Z\\/\\_]+", DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSz"))
      .put("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{4}[a-zA-Z\\/\\_]+", DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSz"))
      .put("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{5}[a-zA-Z\\/\\_]+", DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSz"))
      .put("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{6}[a-zA-Z\\/\\_]+", DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSz"))
      .build();

  static DateTime parse(String dtString) {
    return parse(dtString, TZ_LOS_ANGELES);
  }

  /**
   * Parse the date time string against a predefined list of formats. If none of them match,
   * the input string is truncated to first 10 characters in hope of matching to basic ISO date
   * format of yyyy-MM-dd
   * @param dtString the date time value string
   * @param timezone the timezone of the string
   * @return the parsed Date Time object
   */
  static DateTime parse(String dtString, String timezone) {
    DateTimeZone timeZone = DateTimeZone.forID(timezone.isEmpty() ? TZ_LOS_ANGELES : timezone);
    try {
      for (String format : FORMATS.keySet()) {
        if (dtString.matches(format)) {
          return FORMATS.get(format).withZone(timeZone).parseDateTime(dtString);
        }
      }
      // ignore timezone parameter if the date time string itself has time zone information
      for (String format : FORMATS_WITH_ZONE.keySet()) {
        if (dtString.matches(format)) {
          return FORMATS_WITH_ZONE.get(format).parseDateTime(dtString);
        }
      }
    } catch (Exception e) {
      return DATE_FORMATTER.withZone(timeZone).parseDateTime(dtString.substring(0, 10));
    }
    return DATE_FORMATTER.withZone(timeZone).parseDateTime(dtString.substring(0, 10));
  }

  /**
   * Check if the date time string is in one of the expected formats
   *
   * @param dtString the date time value string
   * @return true if the date time string is recognizable
   */
  static boolean check(String dtString) {
    for (String format : FORMATS.keySet()) {
      if (dtString.matches(format)) {
        return true;
      }
    }
    for (String format : FORMATS_WITH_ZONE.keySet()) {
      if (dtString.matches(format)) {
        return true;
      }
    }
    return false;
  }
  /**
   * Parse the datetime string against a custom datetime format. This version
   * doesn't try the best effort to guess the actual format.
   *
   * Acceptable timezones are: UTC, GMT, America/Los_Angeles, America/New_York, etc
   *
   * @param dtString datetime string
   * @param dtFormat  format of datetime string
   * @param tzString timezone of the epoch value
   * @return the parsed Date Time object
   */
  static DateTime parse(String dtString, String dtFormat, String tzString) {
    try {
      DateTimeZone datetimeZone = DateTimeZone.forID(StringUtils.isBlank(tzString) ? TZ_LOS_ANGELES : tzString);
      DateTimeFormatter datetimeFormatter = DateTimeFormat.forPattern(dtFormat).withZone(datetimeZone);
      return datetimeFormatter.parseDateTime(
          dtString.length() > dtFormat.length() ? dtString.substring(0, dtFormat.length()) : dtString);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}

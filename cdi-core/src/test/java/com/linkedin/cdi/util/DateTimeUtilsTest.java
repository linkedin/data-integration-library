// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.util;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test
public class DateTimeUtilsTest {
  @Test
  public void testParser() {
    Assert.assertEquals(DateTimeUtils.parse("2020-01-01").toString("yyyy-MM-dd"), "2020-01-01");
    Assert.assertEquals(DateTimeUtils.parse("2020-01-01 11:11:11").toString("yyyy-MM-dd HH:mm:ss"),
        "2020-01-01 11:11:11");
    Assert.assertEquals(DateTimeUtils.parse("2020-01-01T11:11:11").toString("yyyy-MM-dd'T'HH:mm:ss"),
        "2020-01-01T11:11:11");
    Assert.assertEquals(DateTimeUtils.parse("2020-01-01T11:11:11.9").toString("yyyy-MM-dd'T'HH:mm:ss.SSS"),
        "2020-01-01T11:11:11.900");
    Assert.assertEquals(DateTimeUtils.parse("2020-01-01T11:11:11.99").toString("yyyy-MM-dd'T'HH:mm:ss.SSS"),
        "2020-01-01T11:11:11.990");
    Assert.assertEquals(DateTimeUtils.parse("2020-01-01T11:11:11.999").toString("yyyy-MM-dd'T'HH:mm:ss.SSS"),
        "2020-01-01T11:11:11.999");
    // test microseconds truncation as Joda supports only milliseconds
    Assert.assertEquals(DateTimeUtils.parse("2020-01-01T11:11:11.9999").toString("yyyy-MM-dd'T'HH:mm:ss.SSS"),
        "2020-01-01T11:11:11.999");
    Assert.assertEquals(DateTimeUtils.parse("2020-01-01T11:11:11.99999").toString("yyyy-MM-dd'T'HH:mm:ss.SSS"),
        "2020-01-01T11:11:11.999");
    Assert.assertEquals(DateTimeUtils.parse("2020-01-01T11:11:11.999999").toString("yyyy-MM-dd'T'HH:mm:ss.SSS"),
        "2020-01-01T11:11:11.999");

    Assert.assertEquals(DateTimeUtils.parse("2020-01-01 10:00:00-07:00")
        .withZone(DateTimeZone.UTC)
        .toString("yyyy-MM-dd'T'HH:mm:ss.SSSZ"), "2020-01-01T17:00:00.000+0000");
    Assert.assertEquals(DateTimeUtils.parse("2020-01-01 10:00:00.000-0700")
        .withZone(DateTimeZone.UTC)
        .toString("yyyy-MM-dd'T'HH:mm:ss.SSSZ"), "2020-01-01T17:00:00.000+0000");
    Assert.assertEquals(DateTimeUtils.parse("2020-01-01 10:00:00.000-07:00")
        .withZone(DateTimeZone.UTC)
        .toString("yyyy-MM-dd'T'HH:mm:ss.SSSZ"), "2020-01-01T17:00:00.000+0000");

    Assert.assertEquals(DateTimeUtils.parse("2020-01-01T10:00:00-0700")
        .withZone(DateTimeZone.UTC)
        .toString("yyyy-MM-dd'T'HH:mm:ss.SSSZ"), "2020-01-01T17:00:00.000+0000");
    Assert.assertEquals(DateTimeUtils.parse("2020-01-01T10:00:00.000-0700")
        .withZone(DateTimeZone.UTC)
        .toString("yyyy-MM-dd'T'HH:mm:ss.SSSZ"), "2020-01-01T17:00:00.000+0000");
    Assert.assertEquals(DateTimeUtils.parse("2020-01-01T10:00:00.000-07:00")
        .withZone(DateTimeZone.UTC)
        .toString("yyyy-MM-dd'T'HH:mm:ss.SSSZ"), "2020-01-01T17:00:00.000+0000");

    Assert.assertEquals(DateTimeUtils.parse("2020-01-01 10:00:00PST").getZone().getID(), "America/Los_Angeles");
    Assert.assertEquals(
        DateTimeUtils.parse("2020-01-01 10:00:00PST").withZone(DateTimeZone.UTC).toString("yyyy-MM-dd'T'HH:mm:ss.SSSZ"),
        "2020-01-01T18:00:00.000+0000");

    // time will be truncated in this case because Joda doesn't process long form timezone name well
    Assert.assertEquals(DateTimeUtils.parse("2020-01-01 10:00:00America/Los_Angeles")
        .withZone(DateTimeZone.UTC)
        .toString("yyyy-MM-dd'T'HH:mm:ss.SSSZ"), "2020-01-01T08:00:00.000+0000");

    // time will be truncated in this case because of unrecognizable format
    Assert.assertEquals(
        DateTimeUtils.parse("2020-01-01 10:00").withZone(DateTimeZone.UTC).toString("yyyy-MM-dd'T'HH:mm:ss.SSSZ"),
        "2020-01-01T08:00:00.000+0000");
  }

  /**
   *  Timezone: "America/Los_Angeles will be used if the timezone parameter is empty
   */
  @Test
  public void parse_emptyTimezone_defaultTimezone() {
    Assert.assertEquals(
        DateTimeUtils.parse("2020-01-01 10:00", "").withZone(DateTimeZone.UTC).toString("yyyy-MM-dd'T'HH:mm:ss.SSSZ"),
        "2020-01-01T08:00:00.000+0000");

    Assert.assertEquals(DateTimeUtils.parse("2020-01-01 10:00:30", "")
        .withZone(DateTimeZone.UTC)
        .toString("yyyy-MM-dd'T'HH:mm:ss.SSSZ"), "2020-01-01T18:00:30.000+0000");

    Assert.assertEquals(DateTimeUtils.parse("2020-01-01 10:00:00-07:00", "")
        .withZone(DateTimeZone.UTC)
        .toString("yyyy-MM-dd'T'HH:mm:ss.SSSZ"), "2020-01-01T17:00:00.000+0000");
  }

  /**
   * Exception is expected when dtString is null
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void parse_nulldtString_illegalArgumentException() {
    DateTimeUtils.parse(null, "");
  }

  /**
   * Exception is expected when timezone is null
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void parse_nullTimezone_illegalArgumentException() {
    DateTimeUtils.parse("", null);
  }

  @Test
  public void testParseWithCustomFormat() {
    // test UTC, GMT
    Assert.assertEquals(DateTimeUtils.parse("20210101", "yyyyMMdd", "UTC").getMillis(), 1609459200000L);
    Assert.assertEquals(DateTimeUtils.parse("20210101", "yyyyMMdd", "GMT").getMillis(), 1609459200000L);

    // test default timezone
    Assert.assertEquals(DateTimeUtils.parse("20210101", "yyyyMMdd", "America/Los_Angeles").getMillis(), 1609488000000L);
    Assert.assertEquals(DateTimeUtils.parse("20210101", "yyyyMMdd", "").getMillis(), 1609488000000L);
    Assert.assertEquals(DateTimeUtils.parse("20210101", "yyyyMMdd", null).getMillis(), 1609488000000L);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testParseWithCustomFormatException1() {
    // incorrectly call, end up with wrong function
    Assert.assertEquals(DateTimeUtils.parse("20210101", "yyyyMMdd").getMillis(), 0);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testParseWithCustomFormatException2() {
    Assert.assertEquals(DateTimeUtils.parse("20210101", "yyyy-MM-dd", "GMT").getMillis(), 1609459200000L);
    //Assert.assertEquals(DateTimeUtils.parse("20210101", "yyyyMMdd", "America/Los_Angeles").getMillis(), 1609488000000L);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testParseWithCustomFormatException3() {
    Assert.assertEquals(DateTimeUtils.parse("2021010100", "yyyy-MM-dd", "GMT").getMillis(), 1609459200000L);
    //Assert.assertEquals(DateTimeUtils.parse("20210101", "yyyyMMdd", "America/Los_Angeles").getMillis(), 1609488000000L);
  }

  @Test
  public void testJodaTime() {
    // difference less than 10 seconds
    Assert.assertTrue(Math.abs(DateTime.now().getMillis() - DateTime.now().withZone(DateTimeZone.UTC).getMillis()) < 10 * 1000L);
  }
}

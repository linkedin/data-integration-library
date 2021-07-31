// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.util;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.UnsupportedEncodingException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test
public class VariableUtilsTest {
  private final static String TEMPLATE = "{\"%s\":\"%s\"}";
  private final static String TEST_DATE_STRING = "2019-11-01 12:00:00";
  private final static String START_DATE_NAME = "startDate";
  private final static String END_DATE_NAME = "endDate";
  private Gson gson;
  private JsonObject parameters;

  @BeforeClass
  public void setUp() {
    gson = new Gson();
  }

  @Test
  void testReplaceWithTracking() throws UnsupportedEncodingException {
    String template = "\\'{{Activity.CreatedAt}}\\' >= \\'{{startDate}}\\' and \\'{{Activity.CreatedAt}}\\' < \\'{{endDate}}\\'";
    JsonObject parameters = new JsonObject();
    parameters.addProperty("startDate", "2019-11-01 12:00:00");
    parameters.addProperty("endDate", "2019-11-02 12:00:00");
    String expected = "\\'{{Activity.CreatedAt}}\\' >= \\'2019-11-01 12:00:00\\' and \\'{{Activity.CreatedAt}}\\' < \\'2019-11-02 12:00:00\\'";
    Assert.assertEquals(VariableUtils.replaceWithTracking(template, parameters, false).getKey(), expected);
    Assert.assertEquals(VariableUtils.replaceWithTracking(template, parameters).getKey(), expected);

    expected = "\\'{{Activity.CreatedAt}}\\' >= \\'2019-11-01+12%3A00%3A00\\' and \\'{{Activity.CreatedAt}}\\' < \\'2019-11-02+12%3A00%3A00\\'";
    Assert.assertEquals(VariableUtils.replaceWithTracking(template, parameters, true).getKey(), expected);
  }

  /**
   * Test: parameters contains value for placeholders in template
   * Expected: placeholder replaced
   * @throws UnsupportedEncodingException
   */
  @Test
  public void testReplaceWithTrackingII() throws UnsupportedEncodingException {
    parameters = new JsonObject();
    parameters.addProperty(START_DATE_NAME, TEST_DATE_STRING);
    Assert.assertEquals(VariableUtils.replace(gson.fromJson(String.format(TEMPLATE, START_DATE_NAME, "{{startDate}}"), JsonObject.class), parameters).toString(),
        String.format(TEMPLATE, START_DATE_NAME, TEST_DATE_STRING));
  }

  /**
   * Test: parameters doesn't contains value for placeholders in template
   * Expected: placeholder not replaced
   * @throws UnsupportedEncodingException
   */
  @Test
  public void testReplace() throws UnsupportedEncodingException {
    String expected = String.format(String.format(TEMPLATE, START_DATE_NAME, "{{startDate}}"));
    parameters = new JsonObject();
    parameters.addProperty(END_DATE_NAME, TEST_DATE_STRING);
    Assert.assertEquals(VariableUtils.replaceWithTracking(expected, parameters, false),
        new ImmutablePair<>(expected, gson.fromJson(String.format(TEMPLATE, END_DATE_NAME, TEST_DATE_STRING), JsonObject.class)));
  }
}

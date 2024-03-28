// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.util;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.linkedin.cdi.factory.http.HttpRequestMethod;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.powermock.api.mockito.PowerMockito.*;


/**
 * Unit test for {@link HttpRequestMethod}
 * @author chrli
 *
 */

@Test
@PrepareForTest({VariableUtils.class})
@PowerMockIgnore("jdk.internal.reflect.*")
public class HttpRequestMethodTest extends PowerMockTestCase {

  final static String FROM_DATETIME = "2017-01-02T00:00:00-0800";
  final static String TO_DATETIME = "2019-10-25T15:00:00-0700";
  final static String HTTP_POST_FIX = "HTTP/1.1";
  final static String VERSION_2 = "v2";
  final static String CONTENT_TYPE = "Content-Type";
  final static String CONTENT_TYPE_VALUE = "application/x-www-form-urlencoded";
  final static String BASE_URI = "https://domain/%s/calls";
  final static String BASE_URI_WITH_ONLYFROM_PARAMS = "https://domain/%s/calls?fdt=%s";
  final static String BASE_URI_WITH_TOANDFROM_PARAMS = "https://domain/%s/calls?fdt=%s&tdt=%s";
  private static Gson gson = new Gson();
  private Map<String, String> headers;
  private String expected;
  private JsonObject parameters;

  static JsonObject generateParameterString(String fromDateTime, String toDateTime, String version) {
    String parameterString = String.format("{\"fromDateTime\":\"%s\",\"toDateTime\":\"%s\"}", fromDateTime, toDateTime);
    if (!Strings.isNullOrEmpty(version)) {
      parameterString =
          String.format("{\"fromDateTime\":\"%s\",\"toDateTime\":\"%s\",\"version\":\"%s\"}", fromDateTime, toDateTime, version);
    }
    return gson.fromJson(parameterString, JsonObject.class);
  }

  @BeforeMethod
  public void setUp() {
    headers = new HashMap<>();
  }

  /**
   * Test HttpGet_xe method with parameters
   * Note: This works when the URI template has all the variable parameters.
   * If there are any additional parameters, then URIBuilder encodes all parameters while building.
   *
   * @throws UnsupportedEncodingException
   */
  /*
  @Test
  public void testGetXeHttpGetRequest1() throws UnsupportedEncodingException {
    expected = String.format(
        "%s %s %s",
        "GET",
        String.format(BASE_URI_WITH_TOANDFROM_PARAMS, VERSION_2, FROM_DATETIME, URLEncoder.encode(TO_DATETIME, StandardCharsets.UTF_8.toString())),
        HTTP_POST_FIX);
    parameters = generateParameterString(FROM_DATETIME, TO_DATETIME, VERSION_2);
    String uriTemplate = String.format(BASE_URI_WITH_ONLYFROM_PARAMS, "{{version}}", "{{fromDateTime}}");

    HttpUriRequest getRequest =
        HttpRequestMethod.GET_XE.getHttpRequest(
            uriTemplate, parameters, headers);
    Assert.assertEquals(getRequest.toString(), expected);

    addContentType();
    getRequest = HttpRequestMethod.GET_XE.getHttpRequest(uriTemplate, parameters, headers);
    Assert.assertEquals(getRequest.toString(), expected);
  }
  */
  @Test
  public void testGetXeHttpGetRequest() throws UnsupportedEncodingException {
    expected = String.format(
        "%s %s %s",
        "GET",
        String.format(BASE_URI_WITH_TOANDFROM_PARAMS, VERSION_2, FROM_DATETIME, TO_DATETIME),
        HTTP_POST_FIX);
    parameters = generateParameterString(FROM_DATETIME, TO_DATETIME, VERSION_2);
    String uriTemplate = String.format(BASE_URI_WITH_TOANDFROM_PARAMS, "{{version}}", "{{fromDateTime}}", "{{toDateTime}}");

    HttpUriRequest getRequest =
        HttpRequestMethod.GET_XE.getHttpRequest(
            uriTemplate, parameters, headers);
    Assert.assertEquals(getRequest.toString(), expected);

    addContentType();
    getRequest = HttpRequestMethod.GET_XE.getHttpRequest(uriTemplate, parameters, headers);
    Assert.assertEquals(getRequest.toString(), expected);
  }

  /**
   * Test HttpGet method with parameters
   * @throws UnsupportedEncodingException
   */
  @Test
  public void testGetHttpGetRequest() throws UnsupportedEncodingException {
    expected = String.format(
        "%s %s %s",
        "GET",
        String.format("%s?fromDateTime=%s&toDateTime=%s", String.format(BASE_URI, VERSION_2),
            URLEncoder.encode(FROM_DATETIME, StandardCharsets.UTF_8.toString()),
            URLEncoder.encode(TO_DATETIME, StandardCharsets.UTF_8.toString())),
        HTTP_POST_FIX);
    parameters = generateParameterString(FROM_DATETIME, TO_DATETIME, VERSION_2);
    HttpUriRequest getRequest = HttpRequestMethod.GET.getHttpRequest(String.format(BASE_URI, "{{version}}"), parameters, headers);
    Assert.assertEquals(getRequest.toString(), expected);

    addContentType();
    getRequest = HttpRequestMethod.GET.getHttpRequest(String.format(BASE_URI, "{{version}}"), parameters, headers);
    Assert.assertEquals(getRequest.toString(), expected);
  }

  /**
   * Test HttpDelete method
   * @throws UnsupportedEncodingException
   */
  @Test
  public void testGetHttpDeleteRequest() throws IOException {
    String expected = String.format("%s %s %s", "DELETE", String.format(BASE_URI, VERSION_2), HTTP_POST_FIX);
    parameters = generateParameterString(FROM_DATETIME, TO_DATETIME, "");

    HttpDelete deleteRequest = (HttpDelete) HttpRequestMethod.DELETE.getHttpRequest(String.format(BASE_URI, VERSION_2), parameters, headers);
    Assert.assertEquals(deleteRequest.toString(), expected);

    addContentType();
    deleteRequest = (HttpDelete) HttpRequestMethod.DELETE.getHttpRequest(String.format(BASE_URI, VERSION_2), parameters, headers);
    Assert.assertEquals(deleteRequest.toString(), expected);
  }

  /**
   * Test HttpPost method with parameters
   * @throws IOException
   */
  @Test
  public void testGetHttpPostRequest() throws IOException {
    expected = String.format("%s %s %s", "POST", String.format(BASE_URI, VERSION_2), HTTP_POST_FIX);
    parameters = generateParameterString(FROM_DATETIME, TO_DATETIME, "");
    HttpPost postRequest = (HttpPost) HttpRequestMethod.POST.getHttpRequest(String.format(BASE_URI, VERSION_2), parameters, headers);
    Assert.assertEquals(expected, postRequest.toString());
    Assert.assertEquals(parameters.toString(), IOUtils.toString(postRequest.getEntity().getContent(), StandardCharsets.UTF_8));

    addContentType();
    postRequest = (HttpPost) HttpRequestMethod.POST.getHttpRequest(String.format(BASE_URI, VERSION_2), parameters, headers);
    Assert.assertEquals(postRequest.toString(), expected);
  }

  /**
   * Test HttpPut method with parameters
   * @throws IOException
   */
  @Test
  public void testGetHttpPutRequest() throws IOException {
    expected = String.format("%s %s %s", "PUT", String.format(BASE_URI, VERSION_2), HTTP_POST_FIX);
    parameters = generateParameterString(FROM_DATETIME, TO_DATETIME, "");

    HttpPut putRequest = (HttpPut) HttpRequestMethod.PUT.getHttpRequest(String.format(BASE_URI, VERSION_2), parameters, headers);
    Assert.assertEquals(expected, putRequest.toString());
    Assert.assertEquals(parameters.toString(), IOUtils.toString(putRequest.getEntity().getContent(), StandardCharsets.UTF_8));

    addContentType();
    putRequest = (HttpPut) HttpRequestMethod.PUT.getHttpRequest(String.format(BASE_URI, VERSION_2), parameters, headers);
    Assert.assertEquals(expected, putRequest.toString());
  }

  /**
   * Test getHttpRequest
   */
  @Test
  public void testGetHttpRequest() throws IOException {
    PowerMockito.mockStatic(VariableUtils.class);
    String uri = String.format(BASE_URI, VERSION_2);
    Map<String, String> headers = ImmutableMap.of(CONTENT_TYPE, CONTENT_TYPE_VALUE);
    String expected = String.format("%s %s %s", "POST", uri, HTTP_POST_FIX);
    JsonObject parameters = HttpRequestMethodTest.generateParameterString(FROM_DATETIME, TO_DATETIME, "");
    when(VariableUtils.replaceWithTracking(uri, parameters, true)).thenReturn(new ImmutablePair<>(uri, parameters));
    when(VariableUtils.replaceWithTracking(headers.get(CONTENT_TYPE), parameters)).thenReturn(new ImmutablePair<>(CONTENT_TYPE, parameters));
    Assert.assertEquals(HttpRequestMethod.POST.getHttpRequest(String.format(BASE_URI, VERSION_2), parameters, headers).toString(), expected);
  }

  private void addContentType() {
    headers.clear();
    headers.put(CONTENT_TYPE, CONTENT_TYPE_VALUE);
  }
}

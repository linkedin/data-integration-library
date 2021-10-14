// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.keys;

import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import com.linkedin.cdi.configuration.MultistageProperties;
import com.linkedin.cdi.factory.http.HttpRequestMethod;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gobblin.configuration.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.cdi.configuration.PropertyCollection.*;


/**
 * This structure holds static parameters that are commonly used in HTTP protocol.
 *
 * @author chrli
 */
public class HttpKeys extends JobKeys {
  private static final Logger LOG = LoggerFactory.getLogger(HttpKeys.class);
  final private static List<MultistageProperties> ESSENTIAL_PARAMETERS = Lists.newArrayList(
      SOURCE_CONN_USERNAME,
      SOURCE_CONN_PASSWORD,
      MSTAGE_AUTHENTICATION,
      MSTAGE_HTTP_REQUEST_METHOD,
      MSTAGE_HTTP_REQUEST_HEADERS,
      MSTAGE_SESSION_KEY_FIELD);

  private JsonObject authentication = new JsonObject();
  private JsonObject httpRequestHeaders = new JsonObject();
  private Map<String, String> httpRequestHeadersWithAuthentication = new HashMap<>();
  private String httpRequestMethod = HttpRequestMethod.GET.toString();
  private JsonObject initialParameters = new JsonObject();
  private Map<String, List<Integer>> httpStatuses = new HashMap<>();
  private Map<String, List<String>> httpStatusReasons = new HashMap<>();

  @Override
  public void logDebugAll() {
    super.logDebugAll();
    LOG.debug("These are values in HttpSource");
    LOG.debug("Http Request Headers: {}", httpRequestHeaders);
    //LOG.debug("Http Request Headers with Authentication: {}", httpRequestHeadersWithAuthentication.toString());
    LOG.debug("Http Request Method: {}", httpRequestMethod);
    LOG.debug("Http Statuses: {}", httpStatuses);
    LOG.debug("Initial values of dynamic parameters: {}", initialParameters);
  }

  @Override
  public void logUsage(State state) {
    super.logUsage(state);
    for (MultistageProperties p: ESSENTIAL_PARAMETERS) {
      LOG.info("Property {} ({}) has value {} ", p.toString(), p.getClassName(), p.getProp(state));
    }
  }

  public JsonObject getAuthentication() {
    return authentication;
  }

  public void setAuthentication(JsonObject authentication) {
    this.authentication = authentication;
  }

  public JsonObject getHttpRequestHeaders() {
    return httpRequestHeaders;
  }

  public void setHttpRequestHeaders(JsonObject httpRequestHeaders) {
    this.httpRequestHeaders = httpRequestHeaders;
  }

  public Map<String, String> getHttpRequestHeadersWithAuthentication() {
    return httpRequestHeadersWithAuthentication;
  }

  public void setHttpRequestHeadersWithAuthentication(Map<String, String> httpRequestHeadersWithAuthentication) {
    this.httpRequestHeadersWithAuthentication = httpRequestHeadersWithAuthentication;
  }

  public String getHttpRequestMethod() {
    return httpRequestMethod;
  }

  public void setHttpRequestMethod(String httpRequestMethod) {
    this.httpRequestMethod = httpRequestMethod;
  }

  public JsonObject getInitialParameters() {
    return initialParameters;
  }

  public void setInitialParameters(JsonObject initialParameters) {
    this.initialParameters = initialParameters;
  }

  public Map<String, List<Integer>> getHttpStatuses() {
    return httpStatuses;
  }

  public void setHttpStatuses(Map<String, List<Integer>> httpStatuses) {
    this.httpStatuses = httpStatuses;
  }

  public Map<String, List<String>> getHttpStatusReasons() {
    return httpStatusReasons;
  }

  public void setHttpStatusReasons(Map<String, List<String>> httpStatusReasons) {
    this.httpStatusReasons = httpStatusReasons;
  }
}

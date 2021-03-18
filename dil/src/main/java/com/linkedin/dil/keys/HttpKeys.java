// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.dil.keys;

import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.State;
import com.linkedin.dil.configuration.MultistageProperties;
import com.linkedin.dil.util.HttpRequestMethod;


/**
 * This structure holds static parameters that are commonly used in HTTP protocol.
 *
 * @author chrli
 */
@Slf4j
@Getter (AccessLevel.PUBLIC)
@Setter(AccessLevel.PUBLIC)
public class HttpKeys extends JobKeys {
  final private static List<MultistageProperties> ESSENTIAL_PARAMETERS = Lists.newArrayList(
      MultistageProperties.SOURCE_CONN_USERNAME,
      MultistageProperties.SOURCE_CONN_PASSWORD,
      MultistageProperties.MSTAGE_AUTHENTICATION,
      MultistageProperties.MSTAGE_HTTP_REQUEST_METHOD,
      MultistageProperties.MSTAGE_HTTP_REQUEST_HEADERS,
      MultistageProperties.MSTAGE_SESSION_KEY_FIELD);

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
    log.debug("These are values in HttpSource");
    log.debug("Http Request Headers: {}", httpRequestHeaders);
    //log.debug("Http Request Headers with Authentication: {}", httpRequestHeadersWithAuthentication.toString());
    log.debug("Http Request Method: {}", httpRequestMethod);
    log.debug("Http Statuses: {}", httpStatuses);
    log.debug("Initial values of dynamic parameters: {}", initialParameters);
  }

  @Override
  public void logUsage(State state) {
    super.logUsage(state);
    for (MultistageProperties p: ESSENTIAL_PARAMETERS) {
      log.info("Property {} ({}) has value {} ", p.toString(), p.getClassName(), p.getValidNonblankWithDefault(state));
    }
  }
}

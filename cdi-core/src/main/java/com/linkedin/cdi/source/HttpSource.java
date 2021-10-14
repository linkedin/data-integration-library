// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.source;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.linkedin.cdi.connection.HttpConnection;
import com.linkedin.cdi.extractor.MultistageExtractor;
import com.linkedin.cdi.keys.HttpKeys;
import com.linkedin.cdi.util.EncryptionUtils;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.codec.binary.Base64;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.Extractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.cdi.configuration.PropertyCollection.*;


/**
 *
 * HttpSource is a generic Gobblin source connector for HTTP based data sources including
 * Rest API
 *
 * @author chrli
 */
@SuppressWarnings("unchecked")
public class HttpSource extends MultistageSource<Schema, GenericRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(HttpSource.class);
  private final static Gson GSON = new Gson();
  private final static String BASIC_TOKEN_PREFIX = "Basic";
  private final static String BEARER_TOKEN_PREFIX = "Bearer";
  final static String OAUTH_TOKEN_PREFIX = "OAuth";
  final static String TOKEN_PREFIX_SEPARATOR = " ";
  private HttpKeys httpSourceKeys;

  public HttpKeys getHttpSourceKeys() {
    return httpSourceKeys;
  }

  public void setHttpSourceKeys(HttpKeys httpSourceKeys) {
    this.httpSourceKeys = httpSourceKeys;
  }

  public HttpSource() {
    httpSourceKeys = new HttpKeys();
    jobKeys = httpSourceKeys;
  }

  protected void initialize(State state) {
    super.initialize(state);
    httpSourceKeys.logUsage(state);
    httpSourceKeys.setHttpRequestHeaders(getRequestHeader(state));
    httpSourceKeys.setHttpRequestMethod(MSTAGE_HTTP_REQUEST_METHOD.get(state));
    httpSourceKeys.setAuthentication(MSTAGE_AUTHENTICATION.get(state));
    httpSourceKeys.setHttpRequestHeadersWithAuthentication(getHeadersWithAuthentication(state));
    httpSourceKeys.setHttpStatuses(getHttpStatuses(state));
    httpSourceKeys.setHttpStatusReasons(getHttpStatusReasons(state));
    httpSourceKeys.logDebugAll();
  }

  /**
   * Create extractor based on the input WorkUnitState, the extractor.class
   * configuration, and a new HttpConnection
   *
   * @param state WorkUnitState passed in from Gobblin framework
   * @return the MultistageExtractor object
   */
  @Override
  public Extractor<Schema, GenericRecord> getExtractor(WorkUnitState state) {
    initialize(state);
    MultistageExtractor<Schema, GenericRecord> extractor =
        (MultistageExtractor<Schema, GenericRecord>) super.getExtractor(state);
    extractor.setConnection(new HttpConnection(state, this.httpSourceKeys, extractor.getExtractorKeys()));
    return extractor;
  }

  /**
   * Support:
   *   Basic Http Authentication
   *   Bearer token with Authorization header only, not including access_token in URI or Entity Body
   *
   * see Bearer token reference: https://tools.ietf.org/html/rfc6750
   *
   * @param state source state
   * @return header tag with proper encryption of tokens
   */
  @VisibleForTesting
  Map<String, String> getAuthenticationHeader(State state) {
    if (httpSourceKeys.getAuthentication().entrySet().size() == 0) {
      return new HashMap<>();
    }

    String authMethod = httpSourceKeys.getAuthentication().get("method").getAsString();
    if (!authMethod.toLowerCase().matches("basic|bearer|oauth|custom")) {
      LOG.warn("Unsupported authentication type: " + authMethod);
      return new HashMap<>();
    }

    String token = "";
    if (httpSourceKeys.getAuthentication().has("token")) {
      token = EncryptionUtils.decryptGobblin(httpSourceKeys.getAuthentication().get("token").getAsString(), state);
    } else {
      String u = EncryptionUtils.decryptGobblin(SOURCE_CONN_USERNAME.get(state), state);
      String p = EncryptionUtils.decryptGobblin(SOURCE_CONN_PASSWORD.get(state), state);
      token = u + ":" + p;
    }

    if (httpSourceKeys.getAuthentication().get("encryption").getAsString().equalsIgnoreCase("base64")) {
      token = new String(Base64.encodeBase64((token).getBytes(StandardCharsets.UTF_8)),
          StandardCharsets.UTF_8);
    }

    String header = "";
    if (authMethod.equalsIgnoreCase(BASIC_TOKEN_PREFIX)) {
      header = BASIC_TOKEN_PREFIX + TOKEN_PREFIX_SEPARATOR + token;
    } else if (authMethod.equalsIgnoreCase(BEARER_TOKEN_PREFIX)) {
      header = BEARER_TOKEN_PREFIX + TOKEN_PREFIX_SEPARATOR + token;
    } else if (authMethod.equalsIgnoreCase(OAUTH_TOKEN_PREFIX)) {
      header = OAUTH_TOKEN_PREFIX + TOKEN_PREFIX_SEPARATOR + token;
    } else {
      header = token;
    }
    return new ImmutableMap.Builder<String, String>().put(httpSourceKeys.getAuthentication().get("header").getAsString(), header).build();
  }

  private Map<String, String> getHeadersWithAuthentication(State state) {
    Map<String, String> headers = toStringStringMap(httpSourceKeys.getHttpRequestHeaders());
    headers.putAll(getAuthenticationHeader(state));
    return headers;
  }

  private Map<String, String> toStringStringMap(JsonObject json) {
    return GSON.fromJson(json.toString(),
        new TypeToken<HashMap<String, String>>() { }.getType());
  }

  private Map<String, List<Integer>> getHttpStatuses(State state) {
    Map<String, List<Integer>> statuses = new HashMap<>();
    JsonObject jsonObject = MSTAGE_HTTP_STATUSES.get(state);
    for (Map.Entry<String, JsonElement> entry: jsonObject.entrySet()) {
      String key = entry.getKey();
      JsonElement value = jsonObject.get(key);
      if (!value.isJsonNull() && value.isJsonArray()) {
        statuses.put(key, GSON.fromJson(value.toString(), new TypeToken<List<Integer>>() { }.getType()));
      }
    }
    return statuses;
  }

  private Map<String, List<String>> getHttpStatusReasons(State state) {
    Map<String, List<String>> reasons = new HashMap<>();
    JsonObject jsonObject = MSTAGE_HTTP_STATUS_REASONS.get(state);
    for (Map.Entry<String, JsonElement> entry: jsonObject.entrySet()) {
      String key = entry.getKey();
      JsonElement value = jsonObject.get(key);
      if (!value.isJsonNull() && value.isJsonArray()) {
        reasons.put(key, GSON.fromJson(value.toString(), new TypeToken<List<String>>() { }.getType()));
      }
    }
    return reasons;
  }

  /**
   * read the ms.http.request.headers and decrypt values if encrypted
   * @param state the source state
   * @return the decrypted http request headers
   */
  private JsonObject getRequestHeader(State state) {
    JsonObject headers = MSTAGE_HTTP_REQUEST_HEADERS.get(state);
    JsonObject decrypted = new JsonObject();
    for (Map.Entry<String, JsonElement> entry: headers.entrySet()) {
      String key = entry.getKey();
      decrypted.addProperty(key, EncryptionUtils.decryptGobblin(headers.get(key).getAsString(), state));
    }
    return decrypted;
  }
}
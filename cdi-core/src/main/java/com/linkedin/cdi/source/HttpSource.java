// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.source;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.linkedin.cdi.connection.HttpConnection;
import com.linkedin.cdi.extractor.MultistageExtractor;
import com.linkedin.cdi.keys.HttpKeys;
import com.linkedin.cdi.util.SecretManager;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.Extractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.cdi.configuration.PropertyCollection.*;
import static com.linkedin.cdi.configuration.StaticConstants.*;


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
    httpSourceKeys.setHttpRequestHeaders(getRequestHeader(state));
    httpSourceKeys.setHttpRequestMethod(MSTAGE_HTTP_REQUEST_METHOD.get(state));
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

  private Map<String, String> getHeadersWithAuthentication(State state) {
    Map<String, String> headers = toStringStringMap(httpSourceKeys.getHttpRequestHeaders());
    headers.putAll(MSTAGE_AUTHENTICATION.getAsMap(state));
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
      decrypted.addProperty(key, SecretManager.getInstance(state).decrypt(headers.get(key).getAsString()));
    }
    return decrypted;
  }
}
// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.configuration;

import com.google.gson.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.configuration.State;


/**
 * SSL Parameters
 */
public class SslProperties extends JsonObjectProperties{
  final private static String KEY_STORE_TYPE = "keyStoreType";
  final private static String KEY_STORE_PATH = "keyStorePath";
  final private static String KEY_STORE_PASSWORD = "keyStorePassword";
  final private static String KEY_PASSWORD = "keyPassword";
  final private static String TRUST_STORE_PATH = "trustStorePath";
  final private static String TRUST_STORE_PASSWORD = "trustStorePassword";
  final private static String CONNECTION_TIMEOUT = "connectionTimeoutSeconds";
  final private static String SOCKET_TIMEOUT = "socketTimeoutSeconds";
  final private static String VERSION = "version";
  final private static String VERSION_DEFAULT = "TLSv1.2";
  final private static String KEY_STORE_TYPE_DEFAULT = "pkcs12";

  /**
   * Constructor with implicit default value
   * @param config property name
   */
  SslProperties(String config) {
    super(config);
  }

  public String getVersion(State state) {
    JsonObject value = get(state);
    if (value.has(VERSION)) {
      return value.get(VERSION).getAsString();
    }
    return VERSION_DEFAULT;
  }

  public String getKeyStoreType(State state) {
    JsonObject value = get(state);
    if (value.has(KEY_STORE_TYPE)) {
      return value.get(KEY_STORE_TYPE).getAsString();
    }
    return KEY_STORE_TYPE_DEFAULT;
  }

  public String getKeyStorePath(State state) {
    JsonObject value = get(state);
    if (value.has(KEY_STORE_PATH)) {
      return get(state).getAsJsonObject().get(KEY_STORE_PATH).getAsString();
    }
    return StringUtils.EMPTY;
  }

  public String getKeyStorePassword(State state) {
    JsonObject value = get(state);
    if (value.has(KEY_STORE_PASSWORD)) {
      return get(state).getAsJsonObject().get(KEY_STORE_PASSWORD).getAsString();
    }
    return StringUtils.EMPTY;
  }

  public String getKeyPassword(State state) {
    JsonObject value = get(state);
    if (value.has(KEY_PASSWORD)) {
      return value.get(KEY_PASSWORD).getAsString();
    }
    return StringUtils.EMPTY;
  }

  public String getTrustStorePath(State state) {
    JsonObject value = get(state);
    if (value.has(TRUST_STORE_PATH)) {
      return value.get(TRUST_STORE_PATH).getAsString();
    }
    return StringUtils.EMPTY;
  }

  public String getTrustStorePassword(State state) {
    JsonObject value = get(state);
    if (value.has(TRUST_STORE_PASSWORD)) {
      return value.get(TRUST_STORE_PASSWORD).getAsString();
    }
    return StringUtils.EMPTY;
  }

  public Integer getConnectionTimeoutMillis(State state) {
    JsonObject value = get(state);
    if (value.has(CONNECTION_TIMEOUT) && value.get(CONNECTION_TIMEOUT).getAsInt() >= 0) {
      return 1000 * value.get(CONNECTION_TIMEOUT).getAsInt();
    }
    return 60 * 1000;
  }

  public Integer getSocketTimeoutMillis(State state) {
    JsonObject value = get(state);
    if (value.has(SOCKET_TIMEOUT) && value.get(SOCKET_TIMEOUT).getAsInt() >= 0) {
      return 1000 * value.get(SOCKET_TIMEOUT).getAsInt();
    }
    return 60 * 1000;
  }
}

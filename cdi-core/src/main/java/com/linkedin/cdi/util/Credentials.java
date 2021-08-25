// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.util;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.password.PasswordManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Credentials {
  private static final Logger LOG = LoggerFactory.getLogger(Credentials.class);

  public static String getPrivateKey(State state) {
    return PasswordManager.getInstance(state)
        .readPassword(state.getProp(ConfigurationKeys.SOURCE_CONN_PRIVATE_KEY));
  }

  public static String getPassword(State state) {
    return PasswordManager.getInstance(state)
        .readPassword(state.getProp(ConfigurationKeys.SOURCE_CONN_PASSWORD));
  }

  public static String getKnownHosts(State state) {
    return state.getProp(ConfigurationKeys.SOURCE_CONN_KNOWN_HOSTS);
  }

  public static String getUserName(State state) {
    return state.getProp(ConfigurationKeys.SOURCE_CONN_USERNAME);
  }

  public static String getHostName(State state) {
    return state.getProp(ConfigurationKeys.SOURCE_CONN_HOST_NAME);
  }

  public static int getPort(State state) {
    return state.getPropAsInt(ConfigurationKeys.SOURCE_CONN_PORT, ConfigurationKeys.SOURCE_CONN_DEFAULT_PORT);
  }

  public static String getProxyHost(State state) {
    return state.getProp(ConfigurationKeys.SOURCE_CONN_USE_PROXY_URL);
  }

  public static int getProxyPort(State state) {
    return state.getPropAsInt(ConfigurationKeys.SOURCE_CONN_USE_PROXY_PORT, -1);
  }

  public static void log(State state) {
    LOG.info("privateKey: {}", getPrivateKey(state));
    LOG.info("knownHosts: {}", getKnownHosts(state));
    LOG.info("userName: {}", getUserName(state));
    LOG.info("hostName: {}", getHostName(state));
    LOG.info("port: {}", getPort(state));
    LOG.info("proxyHost: {}" , getProxyHost(state));
    LOG.info("proxyPort: {}", getProxyPort(state));
  }
}

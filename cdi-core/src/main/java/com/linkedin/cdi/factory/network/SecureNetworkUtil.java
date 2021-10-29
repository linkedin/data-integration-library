// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.factory.network;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContexts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class SecureNetworkUtil {
  private static final Logger LOG = LoggerFactory.getLogger(SecureNetworkUtil.class);

  private SecureNetworkUtil() {
  }

  static public SSLContext createSSLContext(
      KeyCertRetriever keyCertRetriever,
      String sslVersion
  ) throws IOException {
    InputStream keystoreInput = null;
    try {
      keystoreInput = new FileInputStream(keyCertRetriever.getKeyStoreFilePath());
      final KeyStore keystore = KeyStore.getInstance(keyCertRetriever.getKeyStoreType());
      keystore.load(keystoreInput, keyCertRetriever.getKeyStorePassword().toCharArray());
      return SSLContexts.custom()
          .setProtocol(sslVersion)
          .loadKeyMaterial(keystore, keyCertRetriever.getKeyPassword().toCharArray())
          .loadTrustMaterial(new File(keyCertRetriever.getTrustStoreFilePath()), keyCertRetriever.getTrustStorePassword().toCharArray())
          .build();
    } catch (Exception e) {
      LOG.error("Fatal error: couldn't create SSLContext.", e);
      if (keystoreInput != null) {
        keystoreInput.close();
      }
      throw new RuntimeException(e);
    } finally {
      if (keystoreInput != null) {
        keystoreInput.close();
      }
    }
  }

  static private HttpClientBuilder createHttpClientBuilder(
      SSLContext sslContext,
      int secondConnTTL,
      int connectionRouteMax,
      int connectionTotalMax,
      int connectionTimeoutMillis,
      int socketTimeoutMillis) {
    return createHttpClientBuilder(sslContext,
        secondConnTTL,
        connectionRouteMax,
        connectionTotalMax,
        connectionTimeoutMillis,
        socketTimeoutMillis,
        null, -1);
  }

  static private HttpClientBuilder createHttpClientBuilder(
      SSLContext sslContext,
      int secondConnTTL,
      int connectionRouteMax,
      int connectionTotalMax,
      int connectionTimeoutMillis,
      int socketTimeoutMillis,
      String proxyUrl,
      int proxyPort) {
    Registry<ConnectionSocketFactory> registry = RegistryBuilder.<ConnectionSocketFactory>create()
        .register("https", new SSLConnectionSocketFactory(sslContext, new NoopHostnameVerifier()))
        .register("http", new PlainConnectionSocketFactory()).build();
    HttpClientBuilder builder = HttpClientBuilder.create();
    builder.setSSLSocketFactory(new SSLConnectionSocketFactory(sslContext, new NoopHostnameVerifier()));
    PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager(registry);
    connectionManager.setDefaultMaxPerRoute(connectionRouteMax);
    connectionManager.setMaxTotal(connectionTotalMax);
    builder.setConnectionManager(connectionManager);
    builder.setConnectionTimeToLive(secondConnTTL, TimeUnit.SECONDS);

    if (proxyPort > 0 && !StringUtils.isEmpty(proxyUrl)) {
      builder.setProxy(new HttpHost(proxyUrl, proxyPort));
    }

    // add default connection timeout and socket timeout
    RequestConfig requestConfig = RequestConfig.custom()
        .setConnectTimeout(connectionTimeoutMillis)
        .setSocketTimeout(socketTimeoutMillis)
        .build();
    builder.setDefaultRequestConfig(requestConfig);
    return builder;
  }

  static public HttpClientBuilder createSecureHttpClientBuilder(
      KeyCertRetriever keyCertRetriever,
      int secondConnTTL,
      int connectionRouteMax,
      int connectionTotalMax,
      int connectionTimeoutMillis,
      int socketTimeoutMillis,
      String sslVersion,
      String proxyUrl,
      int proxyPort
  ) {
    try {
      SSLContext sslContext = createSSLContext(keyCertRetriever, sslVersion);
      return StringUtils.isEmpty(proxyUrl) && proxyPort <= 0
          ? createHttpClientBuilder(sslContext,
              secondConnTTL,
              connectionRouteMax,
              connectionTotalMax,
              connectionTimeoutMillis,
              socketTimeoutMillis)
          : createHttpClientBuilder(sslContext,
              secondConnTTL,
              connectionRouteMax,
              connectionTotalMax,
              connectionTimeoutMillis,
              socketTimeoutMillis,
              proxyUrl, proxyPort);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}

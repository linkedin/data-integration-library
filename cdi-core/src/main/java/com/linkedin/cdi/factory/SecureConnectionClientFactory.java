// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.factory;

import com.linkedin.cdi.factory.network.KeyCertRetriever;
import com.linkedin.cdi.factory.network.SecureNetworkUtil;
import com.linkedin.cdi.factory.reader.JsonFileReader;
import com.linkedin.cdi.factory.reader.SchemaReader;
import com.linkedin.cdi.factory.sftp.SftpChannelClient;
import com.linkedin.cdi.factory.sftp.SftpClient;
import java.sql.Connection;
import org.apache.gobblin.configuration.State;
import org.apache.http.client.HttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.utils.AttributeMap;

import static com.linkedin.cdi.configuration.PropertyCollection.*;


/**
 * This is the secure implementation that supports secure network (SSL, TLS, HTTPS etc).
 *
 * This is built for integrations with systems inside LinkedIn that doesn't require GaaP proxying
 *
 */
public class SecureConnectionClientFactory extends DefaultConnectionClientFactory {
  private static final Logger LOG = LoggerFactory.getLogger(SecureConnectionClientFactory.class);

  /**
   * Initiate an HTTP client
   * @param state the State of execution environment
   * @return an HTTP client object
   */
  @Override
  public HttpClient getHttpClient(State state) {
    return SecureNetworkUtil.createSecureHttpClientBuilder(
        new KeyCertRetriever(state),
        MSTAGE_HTTP_CONN_TTL_SECONDS.get(state),
        MSTAGE_HTTP_CONN_PER_ROUTE_MAX.get(state),
        MSTAGE_HTTP_CONN_MAX.get(state),
        MSTAGE_SSL.getConnectionTimeoutMillis(state),
        MSTAGE_SSL.getSocketTimeoutMillis(state),
        MSTAGE_SSL.getVersion(state),
        null, -1).build();
  }

  /**
   * Initiate an S3 HTTP client
   * @param state the state of execution environment
   * @param config S3 parameters
   * @return an S3 HTTP client object
   */
  @Override
  public SdkHttpClient getS3Client(State state, AttributeMap config) {
    return super.getS3Client(state, config);
  }
  /**
   * Initiate a JDBC Connection
   * @param jdbcUrl plain or encrypted URL
   * @param userId plain or encrypted user name
   * @param cryptedPassword plain or encrypted password
   * @param state source or work unit state that can provide the encryption master key location
   * @return a JDBC connection
   */
  @Override
  public Connection getJdbcConnection(String jdbcUrl, String userId, String cryptedPassword, State state) {
    return super.getJdbcConnection(jdbcUrl, userId, cryptedPassword, state);
  }

  /**
   * Initiate a Secure Channel client for SFTP Connection
   * @param state the state of execution environment
   * @return a SFTP channel client
   */
  @Override
  public SftpClient getSftpChannelClient(State state)  {
    //return new SftpChannelClient(state, createSslContext(state));
    return new SftpChannelClient(state);
  }

  /**
   * Initiate a SchemaReader
   * @param state the state of execution environment
   * @return a SchemaReader
   */
  @Override
  public SchemaReader getSchemaReader(State state) {
    return new JsonFileReader();
  }
}

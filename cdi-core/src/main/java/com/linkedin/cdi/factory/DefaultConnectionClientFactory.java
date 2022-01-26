// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.factory;

import com.linkedin.cdi.factory.reader.JsonFileReader;
import com.linkedin.cdi.factory.reader.SchemaReader;
import com.linkedin.cdi.factory.sftp.SftpChannelClient;
import com.linkedin.cdi.factory.sftp.SftpClient;
import com.linkedin.cdi.util.SecretManager;
import java.sql.Connection;
import java.sql.DriverManager;
import org.apache.gobblin.configuration.State;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.utils.AttributeMap;

import static software.amazon.awssdk.http.SdkHttpConfigurationOption.*;


/**
 * This is the default implementation
 */
public class DefaultConnectionClientFactory implements ConnectionClientFactory {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultConnectionClientFactory.class);

  /**
   * Initiate an HTTP client
   * @param state the State of execution environment
   * @return an HTTP client object
   */
  @Override
  public HttpClient getHttpClient(State state) {
    return HttpClientBuilder.create().build();
  }

  /**
   * Initiate an S3 HTTP client
   * @param state the state of execution environment
   * @param config S3 parameters
   * @return an S3 HTTP client object
   */
  @Override
  public SdkHttpClient getS3Client(State state, AttributeMap config) {
    return ApacheHttpClient.builder()
        .connectionTimeout(config.get(CONNECTION_TIMEOUT))
        .build();
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
    try {
      return DriverManager.getConnection(
          SecretManager.getInstance(state).decrypt(jdbcUrl),
          SecretManager.getInstance(state).decrypt(userId),
          SecretManager.getInstance(state).decrypt(cryptedPassword));
    } catch (Exception e) {
      LOG.error("Error creating JDBC connection", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Initiate a Secure Channel client for SFTP Connection
   * @param state the state of execution environment
   * @return a SFTP channel client
   */
  @Override
  public SftpClient getSftpChannelClient(State state) {
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

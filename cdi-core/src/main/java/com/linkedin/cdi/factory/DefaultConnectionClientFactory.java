// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.factory;

import com.jcraft.jsch.JSch;
import com.linkedin.cdi.factory.reader.SchemaReader;
import com.linkedin.cdi.util.EncryptionUtils;
import java.sql.Connection;
import java.sql.DriverManager;
import org.apache.gobblin.configuration.State;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.utils.AttributeMap;

import static software.amazon.awssdk.http.SdkHttpConfigurationOption.*;


/**
 * This is the default implementation
 */
public class DefaultConnectionClientFactory implements ConnectionClientFactory {
  /**
   * Initiate an HTTP client
   * @param state the State of exeuction environment
   * @return an HTTP client object
   */
  public HttpClient getHttpClient(State state) {
    return HttpClientBuilder.create().build();
  }

  /**
   * Initiate an S3 HTTP client
   * @param state the state of execution environment
   * @param config S3 parameters
   * @return an S3 HTTP client object
   */
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
  public Connection getJdbcConnection(String jdbcUrl, String userId, String cryptedPassword, State state) {
    try {
      return DriverManager.getConnection(
          EncryptionUtils.decryptGobblin(jdbcUrl, state),
          EncryptionUtils.decryptGobblin(userId, state),
          EncryptionUtils.decryptGobblin(cryptedPassword, state));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
<<<<<<< HEAD
=======
   * Initiate a Secure Channel for SFTP Connection
   * @param state the state of execution environment
   * @return a SFTP secure channel
   */
  public JSch getSecureChannel(State state) {
    // TODO implement the default SFTP secure channel
    return null;
  }

  /**
>>>>>>> 569c190... Consolidate factor classes
   * Initiate a SchemaReader
   * @param state the state of execution environment
   * @return a SchemaReader
   */
  public SchemaReader getSchemaReader(State state) {
    // There is no default schema reader currently
    return null;
  }
}

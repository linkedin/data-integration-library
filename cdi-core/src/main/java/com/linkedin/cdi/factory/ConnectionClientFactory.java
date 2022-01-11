// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.factory;

import com.linkedin.cdi.factory.reader.SchemaReader;
import com.linkedin.cdi.factory.sftp.SftpClient;
import java.sql.Connection;
import org.apache.gobblin.configuration.State;
import org.apache.http.client.HttpClient;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.utils.AttributeMap;


/**
 * The interface for dynamic client creation based on environment
 */
public interface ConnectionClientFactory {
  /**
   * Initiate an HTTP client
   * @param state the State of execution environment
   * @return an HTTP client object
   */
  HttpClient getHttpClient(State state);

  /**
   * Initiate an S3 HTTP client
   * @param state the state of execution environment
   * @param config S3 parameters
   * @return an S3 HTTP client object
   */
  SdkHttpClient getS3Client(State state, AttributeMap config);

  /**
   * Initiate a JDBC Connection
   * @param jdbcUrl plain or encrypted URL
   * @param userId plain or encrypted user name
   * @param cryptedPassword plain or encrypted password
   * @param state the state of execution environment
   * @return a JDBC connection
   */
  Connection getJdbcConnection(String jdbcUrl, String userId, String cryptedPassword, State state);

  /**
   * Initiate a Secure Channel client for SFTP Connection
   * @param state the state of execution environment
   * @return a SFTP channel client
   */
  SftpClient getSftpChannelClient(State state);

  /**
   * Initiate a SchemaReader
   * @param state the state of execution environment
   * @return a SchemaReader
   */
  SchemaReader getSchemaReader(State state);
}

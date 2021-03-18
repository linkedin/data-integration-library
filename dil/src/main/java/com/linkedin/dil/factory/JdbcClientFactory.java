// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.dil.factory;

import java.sql.Connection;
import org.apache.gobblin.configuration.State;


/**
 * The interface for dynamic JDBC Connection creation based on environment.
 */
public interface JdbcClientFactory {
  /**
   * @param jdbcUrl plain or encrypted URL
   * @param userId plain or encrypted user name
   * @param cryptedPassword plain or encrypted password
   * @param state source or work unit state that can provide the encryption master key location
   * @return a JDBC connection
   */
  Connection getConnection(String jdbcUrl, String userId, String cryptedPassword, State state);
}

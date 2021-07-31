// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.factory;

import java.sql.Connection;
import java.sql.DriverManager;
import org.apache.gobblin.configuration.State;
import com.linkedin.cdi.util.EncryptionUtils;


/**
 * An implementation to create an default JDBC connection
 */
public class DefaultJdbcClientFactory implements JdbcClientFactory {
  public Connection getConnection(String jdbcUrl, String userId, String cryptedPassword, State state) {
    try {
      return DriverManager.getConnection(
          EncryptionUtils.decryptGobblin(jdbcUrl, state),
          EncryptionUtils.decryptGobblin(userId, state),
          EncryptionUtils.decryptGobblin(cryptedPassword, state));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}

// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.util;

import com.google.common.base.Preconditions;
import java.net.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * a list of databases
 */
public enum Database {
  MYSQL("MySql", "com.mysql.cj.jdbc.Driver"),
  SQLSERVER("SqlServer", "com.microsoft.sqlserver.jdbc.SQLServerDriver"),
  ORACLE("Oracle", "oracle.jdbc.driver.OracleDriver"),
  HSQLDB("HSqlDb", "org.hsqldb.jdbcDriver");

  private static final Logger LOG = LoggerFactory.getLogger(Database.class);
  final static String PROTOCOL_PREFIX = "jdbc:";

  private String name;
  private String dbType;
  private String defaultDriver;

  public String getName() {
    return name;
  }

  public String getDbType() {
    return dbType;
  }

  public String getDefaultDriver() {
    return defaultDriver;
  }

  Database(String name, String driver) {
    this.name = name;
    this.dbType = name.toLowerCase();
    this.defaultDriver = driver;
  }

  static public Database fromUrl(String jdbcUrl) {
    Preconditions.checkArgument(jdbcUrl.matches("jdbc:(mysql|sqlserver|oracle|hsqldb):.*"), "jdbcUrl");
    String uri = jdbcUrl.substring(PROTOCOL_PREFIX.length());
    return Database.valueOf(URI.create(uri).getScheme().toUpperCase());
  }
}

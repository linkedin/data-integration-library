// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.dil.util;

import com.google.common.base.Preconditions;
import java.net.URI;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


/**
 * a list of databases
 */
@Slf4j
public enum Database {
  MYSQL("MySql", "com.mysql.cj.jdbc.Driver"),
  SQLSERVER("SqlServer", "com.microsoft.sqlserver.jdbc.SQLServerDriver"),
  ORACLE("Oracle", "oracle.jdbc.driver.OracleDriver"),
  HSQLDB("HSqlDb", "org.hsqldb.jdbcDriver");

  final static String PROTOCOL_PREFIX = "jdbc:";

  @Getter private String name;
  @Getter private String dbType;
  @Getter private String defaultDriver;

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

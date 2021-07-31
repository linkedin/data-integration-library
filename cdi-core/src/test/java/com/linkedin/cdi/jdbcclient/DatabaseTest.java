// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.jdbcclient;

import mockit.Mock;
import mockit.MockUp;
import org.apache.gobblin.configuration.State;
import com.linkedin.cdi.util.Database;
import org.apache.gobblin.password.PasswordManager;
import org.mockito.Matchers;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


@Test
public class DatabaseTest {
  @Test
  public void testFromUrl() {
    Database mySqlDb = Database.fromUrl("jdbc:mysql://localhost:3036/test");
    Assert.assertEquals(mySqlDb, Database.MYSQL);
  }

  @Test
  public void testGetName() {
    Database mySqlDb = Database.fromUrl("jdbc:mysql://localhost:3036/test");
    Assert.assertEquals(mySqlDb.getName(), "MySql");
  }

  @Test
  public void testGetDbType() {
    Database mySqlDb = Database.fromUrl("jdbc:mysql://localhost:3036/test");
    Assert.assertEquals(mySqlDb.getDbType(), "mysql");
  }

  @Test
  public void getDefaultDriver() {
    Database mySqlDb = Database.fromUrl("jdbc:mysql://localhost:3036/test");
    Assert.assertEquals(mySqlDb.getDefaultDriver(), "com.mysql.cj.jdbc.Driver");
  }

  private void mockEncryptionUtils(String expectPassword) {
    new MockUp<PasswordManager>() {
      @Mock
      PasswordManager getInstance(State state) {
        PasswordManager pm = mock(PasswordManager.class);
        when(pm.readPassword(Matchers.<String>any())).thenReturn(expectPassword);
        return pm;
      }
    };
  }
}


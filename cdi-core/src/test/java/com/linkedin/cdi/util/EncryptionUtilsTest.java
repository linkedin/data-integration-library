// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.util;

import com.google.gson.JsonObject;
import gobblin.configuration.SourceState;
import org.apache.gobblin.codec.StreamCodec;
import org.apache.gobblin.password.PasswordManager;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.cdi.configuration.PropertyCollection.*;
import static org.mockito.Mockito.*;


@PrepareForTest({PasswordManager.class})
@PowerMockIgnore("jdk.internal.reflect.*")
public class EncryptionUtilsTest extends PowerMockTestCase {
  private final static String PLAIN_PASSWORD = "password";
  private final static String ENC_PASSWORD = "ENC(M6nV+j0lhqZ36RgvuF5TQMyNvBtXmkPl)";
  private SourceState state;
  @Mock
  private PasswordManager passwordManager;

  @BeforeMethod
  public void setUp() {
    String masterKeyLoc = this.getClass().getResource("/key/master.key").toString();
    state = new SourceState();
    state.setProp(ENCRYPT_KEY_LOC.toString(), masterKeyLoc);
    PowerMockito.mockStatic(PasswordManager.class);
    PowerMockito.when(PasswordManager.getInstance(state)).thenReturn(passwordManager);
  }

  @Test
  void testDecryption() {
    when(passwordManager.readPassword(ENC_PASSWORD)).thenReturn(PLAIN_PASSWORD);
    Assert.assertEquals(SecretManager.getInstance(state).decrypt(ENC_PASSWORD), PLAIN_PASSWORD);
    Assert.assertEquals(SecretManager.getInstance(state).decrypt(PLAIN_PASSWORD), PLAIN_PASSWORD);
  }

  @Test
  void testEncryption() {
    when(passwordManager.encryptPassword(PLAIN_PASSWORD)).thenReturn(ENC_PASSWORD);
    when(passwordManager.readPassword(ENC_PASSWORD)).thenReturn(PLAIN_PASSWORD);
    Assert.assertEquals(SecretManager.getInstance(state).decrypt(SecretManager.getInstance(state).encrypt(PLAIN_PASSWORD)),
        PLAIN_PASSWORD);

    when(passwordManager.encryptPassword(ENC_PASSWORD)).thenReturn(ENC_PASSWORD);
    Assert.assertEquals(SecretManager.getInstance(state).decrypt(SecretManager.getInstance(state).encrypt(ENC_PASSWORD)),
        PLAIN_PASSWORD);
  }

  @Test
  void testGetGpgCodec() {
    JsonObject parameters = new JsonObject();
    parameters.addProperty("cipher", "AES_256");
    parameters.addProperty("keystore_path","/tmp/public.key");
    Assert.assertTrue(EncryptionUtils.getGpgCodec(parameters) instanceof StreamCodec);
  }
}
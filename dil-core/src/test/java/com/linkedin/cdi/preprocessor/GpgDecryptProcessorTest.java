// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.preprocessor;

import com.google.gson.JsonObject;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.gobblin.crypto.EncryptionConfigParser;
import org.apache.gobblin.crypto.GPGCodec;
import org.mockito.Mockito;
import org.powermock.reflect.Whitebox;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class GpgDecryptProcessorTest {

  private final static String KEYSTORE_PASSWORD = "test_keystore_password";
  private final static String KEYSTORE_PATH = "test_keystore_path";
  private final static long KEY_NAME = 2342341L;
  private final static String CIPHER = "test_cipher";
  private static final String KEY_ACTION = "decrypt";
  private static final String UNSUPPORTED_KEY_ACTION = "unsupported_decrypt";
  private JsonObject parameters;
  private GpgDecryptProcessor _gpgDecryptProcessor;

  @BeforeMethod
  public void setUp() {
    parameters = new JsonObject();
  }

  /**
   * Test GpgProcessor Constructor with null parameters
   */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testGpgProcessorConstructorWithEmptyPassword() throws Exception {
    parameters.addProperty(EncryptionConfigParser.ENCRYPTION_KEYSTORE_PASSWORD_KEY, StringUtils.EMPTY);
    Whitebox.invokeMethod(new GpgDecryptProcessor(parameters), "getGpgCodec");
  }

  /**
   * Test getGpgCodec with 3 happy paths
   */
  @Test
  public void testGetGpgCodec() {
    parameters.addProperty(EncryptionConfigParser.ENCRYPTION_KEYSTORE_PASSWORD_KEY, KEYSTORE_PASSWORD);
    _gpgDecryptProcessor = new GpgDecryptProcessor(parameters);
    Assert.assertTrue(EqualsBuilder.reflectionEquals(
        _gpgDecryptProcessor.getCodec(), new GPGCodec(KEYSTORE_PASSWORD, null)));

    parameters.addProperty(EncryptionConfigParser.ENCRYPTION_KEYSTORE_PATH_KEY, KEYSTORE_PATH);
    parameters.addProperty(EncryptionConfigParser.ENCRYPTION_CIPHER_KEY, CIPHER);
    _gpgDecryptProcessor = new GpgDecryptProcessor(parameters);
    Assert.assertNotNull(_gpgDecryptProcessor.getCodec());

    parameters.addProperty(EncryptionConfigParser.ENCRYPTION_KEY_NAME, KEY_NAME);
    _gpgDecryptProcessor = new GpgDecryptProcessor(parameters);
    Assert.assertNotNull(_gpgDecryptProcessor.getCodec());
  }

  /**
   * Test process with supported action
   */
  @Test
  public void testProcessWithSupportedAction() throws IOException {
    parameters.addProperty(EncryptionConfigParser.ENCRYPTION_KEYSTORE_PASSWORD_KEY, KEYSTORE_PASSWORD);
    parameters.addProperty("action", KEY_ACTION);
    InputStream inputStream = Mockito.mock(InputStream.class);
    GPGCodec gPGCodec = Mockito.mock(GPGCodec.class);
    _gpgDecryptProcessor = new GpgDecryptProcessor(parameters);
    _gpgDecryptProcessor.setCodec(gPGCodec);
    when(gPGCodec.decodeInputStream(inputStream)).thenReturn(inputStream);
    Assert.assertEquals(_gpgDecryptProcessor.process(inputStream), inputStream);
  }

  /**
   * Test process with unsupported action
   */
  @Test(enabled = false)
  public void testProcessWithUnsupportedAction() throws Exception {
    parameters.addProperty(EncryptionConfigParser.ENCRYPTION_KEYSTORE_PASSWORD_KEY, KEYSTORE_PASSWORD);
    parameters.addProperty("action", UNSUPPORTED_KEY_ACTION);
    InputStream inputStream = Mockito.mock(InputStream.class);
    _gpgDecryptProcessor = new GpgDecryptProcessor(parameters);
    _gpgDecryptProcessor.process(inputStream);
  }
}
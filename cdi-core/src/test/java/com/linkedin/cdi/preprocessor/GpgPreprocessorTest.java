// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.preprocessor;

import com.google.gson.JsonObject;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import org.apache.gobblin.crypto.GPGCodec;
import org.mockito.internal.util.reflection.Whitebox;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


@Test
public class GpgPreprocessorTest {

  @Test(expectedExceptions = IllegalArgumentException.class)
  void testGpgInitNoParameters() throws IOException {
    GpgDecryptProcessor preprocessor = new GpgDecryptProcessor(null);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  void testGpgInitNoPassword() throws IOException {
    JsonObject params = new JsonObject();
    params.addProperty("action", "decrypt");
    GpgDecryptProcessor preprocessor = new GpgDecryptProcessor(params);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  void testGpgInitEmptyPassword() throws IOException {
    JsonObject params = new JsonObject();
    params.addProperty("action", "decrypt");
    params.addProperty("keystore_password", "");
    GpgDecryptProcessor preprocessor = new GpgDecryptProcessor(params);
  }

  @Test()
  void testGpgInitNoPassWordWithKey() {
    JsonObject params = new JsonObject();
    // Provide key file, but not password
    params.addProperty("action", "decrypt");
    params.addProperty("keystore_path", "some path");
    // No error should be thrown
    GpgDecryptProcessor preprocessor = new GpgDecryptProcessor(params);
    Assert.assertNotNull(preprocessor.getCodec());
  }

  @Test
  void testGpgDecryptWithPassword() throws IOException {
    InputStream inputStream = getClass().getResourceAsStream("/gpg/test.csv.gpg");
    InputStream decodedInputStream = getClass().getResourceAsStream("/gpg/test.csv");

    GPGCodec mockedCodec = mock(GPGCodec.class);
    when(mockedCodec.decodeInputStream(inputStream)).thenReturn(decodedInputStream);
    JsonObject params = new JsonObject();
    params.addProperty("keystore_password", "gpgTest");
    params.addProperty("action", "decrypt");
    GpgDecryptProcessor preprocessor = new GpgDecryptProcessor(params);
    Whitebox.setInternalState(preprocessor, "codec", mockedCodec);
    CSVReader reader = new CSVReader(new InputStreamReader(preprocessor.process(inputStream)),',');

    Assert.assertEquals(2, reader.readAll().size());
  }

  @Test
  void testGpgDefaultDecrypt() throws IOException {
    InputStream inputStream = getClass().getResourceAsStream("/gpg/test.csv.gpg");
    InputStream decodedInputStream = getClass().getResourceAsStream("/gpg/test.csv");

    GPGCodec mockedCodec = mock(GPGCodec.class);
    when(mockedCodec.decodeInputStream(inputStream)).thenReturn(decodedInputStream);
    JsonObject params = new JsonObject();
    params.addProperty("keystore_password", "gpgTest");
    GpgDecryptProcessor preprocessor = new GpgDecryptProcessor(params);
    Whitebox.setInternalState(preprocessor, "codec", mockedCodec);
    CSVReader reader = new CSVReader(new InputStreamReader(preprocessor.process(inputStream)), ',');

    Assert.assertEquals(2, reader.readAll().size());
  }

  @Test(enabled = false)
  void testGpgEncrypt() throws UnsupportedOperationException, IOException {
    InputStream inputStream = getClass().getResourceAsStream("/gpg/test.csv.gpg");
    JsonObject params = new JsonObject();
    params.addProperty("keystore_password", "gpgTest");
    params.addProperty("action", "encrypt");
    params.addProperty("cipher", "AES256");
    params.addProperty("keystore_path", "some path");
    params.addProperty("key_name", 0);

    GpgDecryptProcessor preprocessor = new GpgDecryptProcessor(params);
    // Should not support
    preprocessor.process(inputStream);
  }

  @Test
  void testGpgFileNameConversion() {
    String fileName = "test.gpg";
    String expectedFilename = "test";
    InputStream inputStream = getClass().getResourceAsStream("/gpg/test.csv.gpg");
    JsonObject params = new JsonObject();
    params.addProperty("keystore_password", "gpgTest");
    GpgDecryptProcessor preprocessor = new GpgDecryptProcessor(params);
    Assert.assertEquals(expectedFilename, preprocessor.convertFileName(fileName));
  }
}

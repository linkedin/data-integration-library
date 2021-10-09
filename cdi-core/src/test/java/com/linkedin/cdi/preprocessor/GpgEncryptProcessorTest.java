// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.preprocessor;

import com.google.gson.JsonObject;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test
public class GpgEncryptProcessorTest {
  @Test
  public void testConvertFileName() {
    String fileName = "abc.zip";

    JsonObject parameters = new JsonObject();
    parameters.addProperty("cipher", "AES_256");
    parameters.addProperty("keystore_path","/tmp/public.key");

    OutputStreamProcessor processor = new GpgEncryptProcessor(parameters);
    Assert.assertEquals(processor.convertFileName(fileName), "abc.zip.gpg");
  }
}

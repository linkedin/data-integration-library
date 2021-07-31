// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.preprocessor;

import com.google.gson.JsonObject;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class GunzipProcessorTest {

  private JsonObject parameters;
  private String fileName;

  @BeforeMethod
  public void setUp(){
    parameters = new JsonObject();
  }

  /**
   * Test convertFileName with .gz file
   */
  @Test
  public void testConvertFileNameWithgz() {
    fileName = "testFileName.gz";
    GunzipProcessor processor = new GunzipProcessor(parameters);
    Assert.assertEquals(processor.convertFileName(fileName), "testFileName");
  }

  /**
   * Test convertFileName with non .gz file
   */
  @Test
  public void testConvertFileName() {
    fileName = "testFileName.nongz";
    GunzipProcessor processor = new GunzipProcessor(parameters);
    Assert.assertEquals(processor.convertFileName(fileName), fileName);
  }
}
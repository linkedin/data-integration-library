// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.keys;

import org.testng.Assert;
import org.testng.annotations.Test;


public class FileDumpExtractorKeysTest {

  @Test
  public void testIncrCurrentFileNumber() {
    FileDumpExtractorKeys key = new FileDumpExtractorKeys();
    key.incrCurrentFileNumber();
    Assert.assertEquals(key.getCurrentFileNumber(), 1);
  }
}
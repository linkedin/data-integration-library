// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test
public class InputStreamUtilsTest {
  @Test
  public void testConvertListToInputStream() throws IOException {
    InputStream stream = InputStreamUtils.convertListToInputStream(Arrays.asList("a", "b", "c"));
    String data = InputStreamUtils.extractText(stream);
    Assert.assertEquals(data, "a\nb\nc");

    Assert.assertNull(InputStreamUtils.convertListToInputStream(null));
    Assert.assertNull(InputStreamUtils.convertListToInputStream(new ArrayList<>()));
  }
}

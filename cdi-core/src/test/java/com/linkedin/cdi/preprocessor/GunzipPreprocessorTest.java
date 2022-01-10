// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.preprocessor;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test
public class GunzipPreprocessorTest {
  @Test
  void testGunzip() throws IOException {
    InputStream inputStream = getClass().getResourceAsStream("/gzip/cc-index.paths.gz");
    GunzipProcessor preprocessor = new GunzipProcessor(null);
    CSVReader reader = new CSVReader(new InputStreamReader(preprocessor.process(inputStream)),',');
    Assert.assertEquals(302, reader.readAll().size());
  }

  @Test
  void testGunzipFileNameConversion() {
    String filename = "test.gz";
    GunzipProcessor preprocessor = new GunzipProcessor(null);
    Assert.assertEquals("test", preprocessor.convertFileName(filename));
  }
}

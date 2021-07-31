// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.preprocessor;

import com.google.gson.JsonObject;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import org.apache.commons.io.FilenameUtils;


/**
 * a preprocessor that transforms a Gzipped InputStream to unzipped format
 */
public class GunzipProcessor extends InputStreamProcessor {

  private static final String FILE_EXT = "gz";

  public GunzipProcessor(JsonObject params) {
    super(params);
  }

  @Override
  public InputStream process(InputStream input) throws IOException {
    return new GZIPInputStream(input);
  }

  @Override
  public String convertFileName(String fileName) {
    String extension = FilenameUtils.getExtension(fileName);
    return FILE_EXT.equals(extension) ? FilenameUtils.removeExtension(fileName) : fileName;
  }
}

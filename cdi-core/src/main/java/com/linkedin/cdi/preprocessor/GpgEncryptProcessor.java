// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.preprocessor;

import com.google.gson.JsonObject;
import com.linkedin.cdi.util.EncryptionUtils;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.commons.io.FilenameUtils;
import org.apache.gobblin.codec.StreamCodec;


/**
 * Preprocessor to encrypted OutputStream using GPG codec
 *
 * This is backwards compatible with PGP algorithms
 *
 */
public class GpgEncryptProcessor extends OutputStreamProcessor {
  private static final String FILE_EXT = "gpg";

  public StreamCodec getCodec() {
    return codec;
  }

  public void setCodec(StreamCodec codec) {
    this.codec = codec;
  }

  private StreamCodec codec;

  public GpgEncryptProcessor(JsonObject params) {
    super(params);
    this.codec = EncryptionUtils.getGpgCodec(parameters);
  }

  @Override
  public OutputStream process(OutputStream origStream) throws IOException {
      return codec.encodeOutputStream(origStream);
  }

  @Override
  public String convertFileName(String fileName) {
    if (!FilenameUtils.getExtension(fileName).equals(FILE_EXT)) {
      return fileName + ".gpg";
    }
    return fileName;
  }
}

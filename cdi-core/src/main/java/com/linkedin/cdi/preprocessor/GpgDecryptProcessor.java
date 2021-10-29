// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.preprocessor;

import com.google.gson.JsonObject;
import com.linkedin.cdi.configuration.PropertyCollection;
import com.linkedin.cdi.util.EncryptionUtils;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.io.FilenameUtils;
import org.apache.gobblin.annotation.Alias;
import org.apache.gobblin.codec.StreamCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Preprocessor to handle InputStream that is encrypted with
 * GPG compatible algorithm and needs decryption
 *
 * This is backwards compatible with PGP algorithms
 */
@Alias("GpgProcessor")
public class GpgDecryptProcessor extends InputStreamProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(GpgDecryptProcessor.class);

  public StreamCodec getCodec() {
    return codec;
  }

  public void setCodec(StreamCodec codec) {
    this.codec = codec;
  }

  private StreamCodec codec;

  /**
   * @param params See {@link PropertyCollection}
   */
  public GpgDecryptProcessor(JsonObject params) {
    super(params);
    this.codec = EncryptionUtils.getGpgCodec(parameters);
  }

  @Override
  public InputStream process(InputStream inputStream) throws IOException {
    return this.codec.decodeInputStream(inputStream);
  }

  /**
   * TODO: Allow appending an optional file extension
   * @param fileName
   * @return transformed file name
   */
  @Override
  public String convertFileName(String fileName) {
    return FilenameUtils.removeExtension(fileName);
  }
}

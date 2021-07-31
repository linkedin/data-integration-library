// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.preprocessor;

import com.google.gson.JsonObject;
import java.io.IOException;
import java.io.InputStream;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.apache.gobblin.annotation.Alias;
import org.apache.gobblin.codec.StreamCodec;
import com.linkedin.cdi.configuration.MultistageProperties;
import com.linkedin.cdi.util.EncryptionUtils;


/**
 * Preprocessor to handle InputStream that is encrypted with
 * GPG compatible algorithm and needs decryption
 *
 * This is backwards compatible with PGP algorithms
 */
@Slf4j
@Alias("GpgProcessor")
public class GpgDecryptProcessor extends InputStreamProcessor {
  @Getter
  @Setter
  private StreamCodec codec;

  /**
   * @param params See {@link MultistageProperties}
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

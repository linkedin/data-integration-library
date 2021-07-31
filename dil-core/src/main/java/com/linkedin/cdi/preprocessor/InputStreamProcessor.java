// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.preprocessor;

import com.google.gson.JsonObject;
import java.io.IOException;
import java.io.InputStream;


/**
 * a base class for dynamic InputStream preprocessor
 */
abstract public class InputStreamProcessor implements StreamProcessor<InputStream> {
  protected JsonObject parameters;

  public InputStreamProcessor(JsonObject params) {
    this.parameters = params;
  }
  abstract public InputStream process(InputStream input) throws IOException;

  abstract public String convertFileName(String fileName);
}

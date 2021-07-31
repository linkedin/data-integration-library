// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.preprocessor;

import com.google.gson.JsonObject;
import java.io.IOException;
import java.io.OutputStream;


/**
 * a base class for dynamic OutputStream preprocessor
 */
abstract public class OutputStreamProcessor implements StreamProcessor<OutputStream> {
  protected JsonObject parameters;

  public OutputStreamProcessor(JsonObject params) {
    this.parameters = params;
  }
  abstract public OutputStream process(OutputStream origStream) throws IOException;

  abstract public String convertFileName(String fileName);
}

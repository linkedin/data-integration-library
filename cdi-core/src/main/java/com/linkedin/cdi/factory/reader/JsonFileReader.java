// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.factory.reader;

import com.google.gson.JsonElement;
import org.apache.gobblin.configuration.State;


/**
 * TODO
 * a utility class that implement a schema reader from a Json file on HDFS
 */
public class JsonFileReader implements SchemaReader {
  /**
   * @param state a Gobbline State object with needed properties
   * @param urn the HDFS file path
   * @return a JsonSchema object
   */
  @Override
  public JsonElement read(final State state, final String urn) {
    return null;
  }

  @Override
  public void close() {
    // do nothing
  }
}

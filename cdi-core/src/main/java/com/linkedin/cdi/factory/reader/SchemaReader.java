// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.factory.reader;

import com.google.gson.JsonElement;
import org.apache.gobblin.configuration.State;


/**
 * The base class for dynamic schema reader based on environment.
 */
public interface SchemaReader {
  JsonElement read(final State state, final String urn);
  void close();
}

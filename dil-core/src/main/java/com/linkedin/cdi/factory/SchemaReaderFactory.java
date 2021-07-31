// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.factory;

import com.google.common.annotations.VisibleForTesting;
import org.apache.gobblin.configuration.State;
import com.linkedin.cdi.configuration.MultistageProperties;
import com.linkedin.cdi.factory.reader.SchemaReader;


/**
 * The factory to create SchemaReader
 */
public interface SchemaReaderFactory {
  /**
   * Creating a schema reader, default reads from TMS
   * @param state Gobblin configuration
   * @return the reader factory
   */
  @VisibleForTesting
  static SchemaReader create(State state) {
    try {
      Class<?> readerClass = Class.forName(
          MultistageProperties.MSTAGE_SOURCE_SCHEMA_READER_FACTORY.getValidNonblankWithDefault(state));
      return (SchemaReader) readerClass.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}

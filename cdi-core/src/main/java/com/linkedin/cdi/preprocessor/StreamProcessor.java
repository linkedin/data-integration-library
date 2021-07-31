// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.preprocessor;

import java.io.Closeable;
import java.io.IOException;


/**
 * A common interface for all preprocessors
 *
 * @param <T> can be either InputStream or OutputStream
 */
public interface StreamProcessor<T extends Closeable> {
  T process(T origin) throws IOException;
}

// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.filter;

import org.apache.gobblin.converter.avro.UnsupportedDateTypeException;


/**
 * Base filter interface
 *
 */
public interface SchemaBasedFilter<T> {
  T filter(T input) throws UnsupportedDateTypeException;
}

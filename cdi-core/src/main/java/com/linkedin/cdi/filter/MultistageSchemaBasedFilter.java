// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.filter;

import com.linkedin.cdi.util.JsonIntermediateSchema;


/**
 * Base filter class
 *
 * Each extractor shall call a derived filter of this class to process its data
 */
public class MultistageSchemaBasedFilter<T> implements SchemaBasedFilter<T> {
  protected JsonIntermediateSchema schema;

  public MultistageSchemaBasedFilter(JsonIntermediateSchema schema) {
    this.schema = schema;
  }

  @Override
  public T filter(T input) {
    return null;
  }
}

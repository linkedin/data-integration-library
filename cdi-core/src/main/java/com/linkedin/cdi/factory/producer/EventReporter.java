// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.
package com.linkedin.cdi.factory.producer;

/**
 * An interface to implement producer for events and metrics
 */
public interface EventReporter<T> {
  void send(T obj);
  void close();
}

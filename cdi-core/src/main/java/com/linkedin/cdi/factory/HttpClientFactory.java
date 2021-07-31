// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.factory;

import org.apache.gobblin.configuration.State;
import org.apache.http.client.HttpClient;


/**
 * The interface for dynamic HttpClient creation based on environment
 */
public interface HttpClientFactory {
  HttpClient get(State state);
}

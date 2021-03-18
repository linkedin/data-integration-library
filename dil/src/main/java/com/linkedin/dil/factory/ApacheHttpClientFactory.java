// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.dil.factory;

import org.apache.gobblin.configuration.State;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClientBuilder;


/**
 * A vehicle to produce an Apache HttpClient
 */
public class ApacheHttpClientFactory implements HttpClientFactory {
  public HttpClient get(State state) {
    return HttpClientBuilder.create().build();
  }
}

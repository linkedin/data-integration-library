// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.dil.factory;

import org.apache.gobblin.configuration.State;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.utils.AttributeMap;

import static software.amazon.awssdk.http.SdkHttpConfigurationOption.*;


/**
 * An implementation to produce an Apache HttpClient
 */
public class DefaultS3ClientFactory implements S3ClientFactory {
  public SdkHttpClient getHttpClient(State state, AttributeMap config) {
    return ApacheHttpClient.builder()
        .connectionTimeout(config.get(CONNECTION_TIMEOUT))
        .build();
  }
}

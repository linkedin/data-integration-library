// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.dil.factory;

import org.apache.gobblin.configuration.State;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.utils.AttributeMap;


/**
 * The interface for dynamic S3Client creation based on environment
 */
public interface S3ClientFactory {
  SdkHttpClient getHttpClient(State state, AttributeMap config);
}

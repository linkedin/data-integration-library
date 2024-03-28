// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.factory;

import org.apache.gobblin.configuration.State;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


@PrepareForTest({HttpClientBuilder.class})
@PowerMockIgnore("jdk.internal.reflect.*")
public class DefaultConnectionClientFactoryTest extends PowerMockTestCase {
  @Mock
  private HttpClientBuilder httpClientBuilder;

  @Mock
  private CloseableHttpClient closeableHttpClient;

  /**
   * Test whether an Apache HttpClient is produced as expected
   */
  @Test
  public void testGet() {
    DefaultConnectionClientFactory factory = new DefaultConnectionClientFactory();
    PowerMockito.mockStatic(HttpClientBuilder.class);
    PowerMockito.when(HttpClientBuilder.create()).thenReturn(httpClientBuilder);
    when(httpClientBuilder.build()).thenReturn(closeableHttpClient);
    Assert.assertEquals(factory.getHttpClient(new State()), closeableHttpClient);
  }
}
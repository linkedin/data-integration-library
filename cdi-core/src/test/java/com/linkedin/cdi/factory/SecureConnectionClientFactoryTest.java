// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.factory;

import com.linkedin.cdi.configuration.PropertyCollection;
import com.linkedin.cdi.factory.sftp.SftpClient;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import org.apache.gobblin.configuration.SourceState;
import org.apache.http.client.HttpClient;
import org.testng.annotations.Test;

public class SecureConnectionClientFactoryTest {

  @Test (enabled = false)
  public void testHttpConnection() throws IOException {
    SourceState state = new SourceState();
    state.setProp(PropertyCollection.MSTAGE_SSL.getConfig(), "REPLACEME");
    HttpClient httpClient = new SecureConnectionClientFactory().getHttpClient(state);
    ((Closeable) httpClient).close();
  }

  @Test (enabled = false)
  public void testSftpConnection() {
    SourceState state = new SourceState();
    state.setProp(PropertyCollection.SOURCE_CONN_HOST.getConfig(), "REPLACEME");
    state.setProp(PropertyCollection.SOURCE_CONN_USERNAME.getConfig(), "REPLACEME");
    state.setProp(PropertyCollection.SOURCE_CONN_PORT.getConfig(), "22");
    state.setProp(PropertyCollection.SOURCE_CONN_PRIVATE_KEY.getConfig(), "REPLACEME");
    state.setProp(PropertyCollection.MSTAGE_SFTP_CONN_TIMEOUT_MILLIS.getConfig(), "360000");
    ConnectionClientFactory factory = new SecureConnectionClientFactory();
    SftpClient client = factory.getSftpChannelClient(state);
    try {
      client.getSftpChannel();
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
    List<String> files = client.ls("REPLACEME");
    client.close();
  }
}

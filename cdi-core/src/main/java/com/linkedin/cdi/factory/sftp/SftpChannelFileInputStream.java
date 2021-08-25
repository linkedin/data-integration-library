// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.factory.sftp;

import com.jcraft.jsch.Channel;
import java.io.IOException;
import java.io.InputStream;
import org.apache.gobblin.util.io.SeekableFSInputStream;

/**
 * A {@link SeekableFSInputStream} that holds a handle on the Sftp {@link Channel} used to open the
 * {@link InputStream}. The {@link Channel} is disconnected when {@link InputStream#close()} is called.
 */
public class SftpChannelFileInputStream extends SeekableFSInputStream {
  private final Channel channel;

  public SftpChannelFileInputStream(InputStream in, Channel channel) {
    super(in);
    this.channel = channel;
  }

  @Override
  public void close() throws IOException {
    super.close();
    this.channel.disconnect();
  }
}
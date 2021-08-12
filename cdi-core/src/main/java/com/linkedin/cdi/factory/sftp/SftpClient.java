// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.factory.sftp;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpException;
import org.apache.gobblin.configuration.State;


/**
 * The base class for dynamic schema reader based on environment.
 */
public interface SftpClient {
  ChannelSftp getSftpChannel(State state) throws SftpException;
}

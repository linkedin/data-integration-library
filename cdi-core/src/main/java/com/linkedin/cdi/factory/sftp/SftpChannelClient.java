// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.factory.sftp;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.linkedin.cdi.util.Credentials;
import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.source.extractor.extract.sftp.SftpFsHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SftpChannelClient implements SftpClient {
  private static final Logger LOG = LoggerFactory.getLogger(SftpChannelClient.class);
  private static final String SFTP_CONNECTION_TIMEOUT_KEY = "sftpConn.timeout";
  private static final int DEFAULT_SFTP_CONNECTION_TIMEOUT_IN_MS = 3000; //in milliseconds

  private Session session = null;
  private JSch jsch = new JSch();

  public SftpChannelClient(State state) {
    initialize(state);
  }

  private void initialize(State state) {
    JSch.setLogger(new SftpFsHelper.JSchLogger());
    try {
      if (StringUtils.isBlank(Credentials.getProxyHost(state))
          && Credentials.getProxyPort(state) == -1) {
        session = jsch.getSession(Credentials.getUserName(state),
            Credentials.getHostName(state), Credentials.getPort(state));
        session.setPassword(Credentials.getPassword(state));
        if (!session.isConnected()) {
          this.session.connect();
        }
        LOG.info("Finished connecting to source");
      }
      // TODO with proxy
    } catch (JSchException e) {
      if (session != null) {
        session.disconnect();
      }
      LOG.error("Cannot connect to SFTP source", e);
    }
  }

  /**
   * Create new channel every time a command needs to be executed. This is required to support execution of multiple
   * commands in parallel. All created channels are cleaned up when the session is closed.
   *
   * @return a new {@link ChannelSftp}
   * @throws SftpException
   */
  @Override
  public ChannelSftp getSftpChannel(State state) throws SftpException {
    try {
      ChannelSftp channelSftp = (ChannelSftp) this.session.openChannel("sftp");
      // In millsec
      int connTimeout = state.getPropAsInt(SFTP_CONNECTION_TIMEOUT_KEY, DEFAULT_SFTP_CONNECTION_TIMEOUT_IN_MS);
      channelSftp.connect(connTimeout);
      return channelSftp;
    } catch (JSchException e) {
      throw new SftpException(0, "Cannot open a channel to SFTP server", e);
    }
  }
}

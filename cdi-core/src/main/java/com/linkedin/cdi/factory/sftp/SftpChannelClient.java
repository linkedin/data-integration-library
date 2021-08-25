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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.source.extractor.extract.sftp.SftpFsHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SftpChannelClient implements SftpClient {
  private static final Logger LOG = LoggerFactory.getLogger(SftpChannelClient.class);
  private static final String SFTP_CONNECTION_TIMEOUT_KEY = "sftpConn.timeout";
  private static final int DEFAULT_SFTP_CONNECTION_TIMEOUT_IN_MS = 3000; //in milliseconds

  protected State state;
  protected Session session = null;
  protected JSch jsch = new JSch();

  public SftpChannelClient(State state) {
    this.state = state;
    initializeConnection(state);
  }

  protected void initializeConnection(State state) {
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
   * @throws SftpException  An SftpException
   */
  @Override
  public ChannelSftp getSftpChannel() throws SftpException {
    try {
      ChannelSftp channelSftp = (ChannelSftp) this.session.openChannel("sftp");
      // In milliseconds
      int connTimeout = state.getPropAsInt(SFTP_CONNECTION_TIMEOUT_KEY, DEFAULT_SFTP_CONNECTION_TIMEOUT_IN_MS);
      channelSftp.connect(connTimeout);
      return channelSftp;
    } catch (JSchException e) {
      throw new SftpException(0, "Cannot open a channel to SFTP server", e);
    }
  }

  /**
   * Close the session and therefore its channels
   */
  @Override
  public void close() {
    if (this.session != null) {
      this.session.disconnect();
    }
  }

  /**
   * Executes a get SftpCommand and returns an input stream to the file
   */
  @Override
  public InputStream getFileStream(String file) {
    SftpMonitor monitor = new SftpMonitor();
    try {
      ChannelSftp channel = getSftpChannel();
      return new SftpChannelFileInputStream(channel.get(file, monitor), channel);
    } catch (SftpException e) {
      throw new RuntimeException("Cannot download file " + file + " due to " + e.getMessage(), e);
    }
  }

  /**
   * Execute an FTP ls command
   * @param path the target path to list content
   * @return the list of files and directories
   */
  @Override
  public List<String> ls(String path) {
    try {
      List<String> list = new ArrayList<>();
      ChannelSftp channel = getSftpChannel();
      Vector<ChannelSftp.LsEntry> vector = channel.ls(path);
      for (ChannelSftp.LsEntry entry : vector) {
        list.add(entry.getFilename());
      }
      channel.disconnect();
      return list;
    } catch (SftpException e) {
      throw new RuntimeException("Cannot execute ls command on sftp connection", e);
    }
  }

  /**
   * Get file modification time
   * @param path file path on target to be checked
   * @return the modification time in long format
   */
  @Override
  public long getFileMTime(String path) {
    ChannelSftp channelSftp = null;
    try {
      channelSftp = getSftpChannel();
      return channelSftp.lstat(path).getMTime();
    } catch (SftpException e) {
      throw new RuntimeException(
          String.format("Failed to get modified timestamp for file at path %s due to error %s", path,
              e.getMessage()), e);
    } finally {
      if (channelSftp != null) {
        channelSftp.disconnect();
      }
    }
  }

  /**
   * Get file size
   * @param path file path on target to be checked
   * @return the file size
   */
  @Override
  public long getFileSize(String path) {
    try {
      ChannelSftp channelSftp = getSftpChannel();
      long fileSize = channelSftp.lstat(path).getSize();
      channelSftp.disconnect();
      return fileSize;
    } catch (SftpException e) {
      throw new RuntimeException(
          String.format("Failed to get size for file at path %s due to error %s", path, e.getMessage()), e);
    }
  }
}

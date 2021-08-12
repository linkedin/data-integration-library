// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.factory.sftp;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpException;
import java.io.InputStream;
import java.util.List;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.source.extractor.filebased.FileBasedHelperException;


/**
 * The base class for dynamic schema reader based on environment.
 */
public interface SftpClient {
  /**
   * Establish a secure channel
   * @return a new secure channel
   * @throws SftpException
   */
  ChannelSftp getSftpChannel() throws SftpException;

  /**
   * Close the session and therefore its channels
   */
  void close();

  /**
   * Executes a get SftpCommand and returns an input stream to the file
   * @throws SftpException
   */
  InputStream getFileStream(String file) throws FileBasedHelperException;

  /**
   * Exceute an FTP ls command
   * @param path
   * @return the list of files and directories
   * @throws FileBasedHelperException
   */
  List<String> ls(String path) throws FileBasedHelperException;
}

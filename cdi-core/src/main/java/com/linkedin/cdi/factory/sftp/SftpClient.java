// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.factory.sftp;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpException;
import java.io.InputStream;
import java.util.List;


/**
 * The base class for dynamic schema reader based on environment.
 */
public interface SftpClient {
  /**
   * Establish a secure channel
   * @return a new secure channel
   * @throws SftpException An SftpException
   */
  ChannelSftp getSftpChannel() throws SftpException;

  /**
   * Close the session and therefore its channels
   */
  void close();

  /**
   * Executes a get SftpCommand and returns an input stream to the file
   */
  InputStream getFileStream(String file);

  /**
   * Execute an FTP ls command
   * @param path path on target host to be listed
   * @return the list of files and directories
   */
  List<String> ls(String path);

  /**
   * Execute an FTP ls command with retries
   * @param path path on target host to be listed
   * @return the list of files and directories
   */
  List<String> ls(String path, final int retries);


  /**
   * Get file modification time
   * @param path file path on target to be checked
   * @return the modification time in long format
   */
  long getFileMTime(String path);

  /**
   * Get file size
   * @param path file path on target to be checked
   * @return the file size
   */
  long getFileSize(String path);
}

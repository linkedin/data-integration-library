// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.factory.sftp;

import com.google.common.base.Preconditions;
import com.jcraft.jsch.JSch;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Sets identity using a file on HDFS
 */
public class HdfsIdentityStrategy implements IdentityStrategy {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsIdentityStrategy.class);

  @Override
  public boolean setIdentity(String privateKey, JSch jsch) {

    FileSystem fs;
    try {
      fs = FileSystem.get(new Configuration());
    } catch (Exception e) {
      LOG.warn("Failed to set identity using HDFS file. Will attempt next strategy. " + e.getMessage());
      return false;
    }

    Preconditions.checkNotNull(fs, "FileSystem cannot be null");
    try (FSDataInputStream privateKeyStream = fs.open(new Path(privateKey))) {
      byte[] bytes = IOUtils.toByteArray(privateKeyStream);
      jsch.addIdentity("sftpIdentityKey", bytes, (byte[]) null, (byte[]) null);
      LOG.info("Successfully set identity using HDFS file");
      return true;
    } catch (Exception e) {
      LOG.warn("Failed to set identity using HDFS file. Will attempt next strategy. " + e.getMessage());
      return false;
    }
  }
}

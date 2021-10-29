// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.factory.sftp;

import com.jcraft.jsch.JSch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Sets identity using a local file
 */
public class LocalFileIdentityStrategy implements IdentityStrategy {
  private static final Logger LOG = LoggerFactory.getLogger(LocalFileIdentityStrategy.class);
  @Override
  public boolean setIdentity(String privateKey, JSch jsch) {
    try {
      jsch.addIdentity(privateKey);
      LOG.info("Successfully set identity using local file " + privateKey);
      return true;
    } catch (Exception e) {
      LOG.warn("Failed to set identity using local file. Will attempt next strategy. " + e.getMessage());
    }
    return false;
  }
}

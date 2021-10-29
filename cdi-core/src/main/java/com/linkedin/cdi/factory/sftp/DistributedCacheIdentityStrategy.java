// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.factory.sftp;

import com.jcraft.jsch.JSch;
import java.io.File;


/**
 * Sets identity using a file on distributed cache
 */
public class DistributedCacheIdentityStrategy extends LocalFileIdentityStrategy {
  @Override
  public boolean setIdentity(String privateKey, JSch jsch) {
    return super.setIdentity(new File(privateKey).getName(), jsch);
  }
}
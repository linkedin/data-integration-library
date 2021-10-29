// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.factory.sftp;

import com.jcraft.jsch.JSch;


/**
 * Interface for multiple identity setter strategies
 */
interface IdentityStrategy {
  public boolean setIdentity(String privateKey, JSch jsch);
}

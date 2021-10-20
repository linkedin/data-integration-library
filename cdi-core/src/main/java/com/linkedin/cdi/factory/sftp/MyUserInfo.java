// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.factory.sftp;

import com.jcraft.jsch.UserInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of UserInfo class for JSch which allows for password-less login via keys
 * @author stakiar
 */
public class MyUserInfo implements UserInfo {
  private static final Logger LOG = LoggerFactory.getLogger(MyUserInfo.class);

  // The passphrase used to access the private key
  @Override
  public String getPassphrase() {
    return null;
  }

  // The password to login to the client server
  @Override
  public String getPassword() {
    return null;
  }

  @Override
  public boolean promptPassword(String message) {
    return true;
  }

  @Override
  public boolean promptPassphrase(String message) {
    return true;
  }

  @Override
  public boolean promptYesNo(String message) {
    return true;
  }

  @Override
  public void showMessage(String message) {
    LOG.info(message);
  }
}
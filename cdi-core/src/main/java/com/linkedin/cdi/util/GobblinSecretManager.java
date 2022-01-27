// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.util;

import org.apache.gobblin.configuration.State;


/**
 * Interface for secret encryption and decryption
 */
public class GobblinSecretManager extends SecretManager {
  public GobblinSecretManager(State state) {
    super(state);
  }

  /**
   * Decrypt the encrypted string
   * @param input the encrypted string
   * @return decrypted string
   */
  @Override
  public String decrypt(String input) {
    return EncryptionUtils.decryptGobblin(input, state);
  }

  /**
   * Encrypt the decrypted string
   * @param input the unencrypted string
   * @return encrypted string
   */
  @Override
  public String encrypt(String input) {
    return EncryptionUtils.encryptGobblin(input, state);
  }
}

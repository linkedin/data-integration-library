// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.util;

import org.apache.gobblin.configuration.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.cdi.configuration.PropertyCollection.*;


/**
 * Interface for secret encryption and decryption
 */
public abstract class SecretManager {
  final private static Logger LOG = LoggerFactory.getLogger(SecretManager.class);
  private static SecretManager manager = null;
  protected State state;

  public SecretManager(State state) {
    this.state = state;
  }
  /**
   * Decrypt the encrypted string
   * @param input the encrypted string
   * @return decrypted string
   */
  abstract public String decrypt(String input);

  /**
   * Encrypt the decrypted string
   * @param input the unencrypted string
   * @return encrypted string
   */
  abstract public String encrypt(String input);

  /**
   * Close out any resources allocated. The default does nothing.
   */
  public void close() { };

  /**
   * The singleton instantiation method
   * @param state configuration state object
   * @return the singleton SecretManager instance
   */
  static public SecretManager getInstance(State state) {
    if (SecretManager.manager != null) {
      return SecretManager.manager;
    }

    try {
      Class<?> clazz = Class.forName(MSTAGE_SECRET_MANAGER_CLASS.get(state));
      Object manager = clazz.getConstructor(State.class).newInstance(state);
      if (manager instanceof SecretManager) {
        SecretManager.manager = (SecretManager) manager;
      }
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception e) {
      LOG.error("Error creating required secret manager: {}", MSTAGE_SECRET_MANAGER_CLASS.get(state));
      LOG.info("Returning default GobblinSecretManager.");
      SecretManager.manager = new GobblinSecretManager(state);
    }
    return SecretManager.manager;
  }
}

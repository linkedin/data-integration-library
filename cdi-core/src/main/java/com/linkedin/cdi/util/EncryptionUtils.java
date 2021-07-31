// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.util;

import com.google.gson.JsonObject;
import org.apache.gobblin.codec.StreamCodec;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.crypto.EncryptionConfigParser;
import org.apache.gobblin.crypto.GPGCodec;
import org.apache.gobblin.password.PasswordManager;
import org.apache.hadoop.fs.Path;


/**
 * String encryption and decryption utilities
 */
public interface EncryptionUtils {
  String PATTERN = "^ENC\\(.*\\)$";
  /**
   * Decrypt the encrypted string using Gobblin utility
   * @param input the encrypted string
   * @param state Gobblin state object contains the master key location
   * @return decrypted string if the input string is enclosed inside ENC()
   */
  static String decryptGobblin(String input, State state) {
    if (input.matches(PATTERN)) {
      return PasswordManager.getInstance(state).readPassword(input);
    }
    return input;
  }

  /**
   * Encrypt the decrypted string using Gobblin utility
   * @param input the deccrypted string
   * @param state Gobblin state object contains the master key location
   * @return encrypted string which is enclosed within ENC() - as Gobblin utility doesn't do that explicitly
   */
  static String encryptGobblin(String input, State state) {
    String encryptedString = PasswordManager.getInstance(state).encryptPassword(input);
    if (encryptedString.matches(PATTERN)) {
      return encryptedString;
    }
    return "ENC(" + encryptedString + ")";
  }

  /**
   * Create a Gpg Codec per given parameters
   *
   * @param parameters the GPG decryption or encryption parameters
   * @return A StreamCodec object, in this case, returns a @GPGCodec object
   */
  static StreamCodec getGpgCodec(JsonObject parameters) {

    if (parameters == null) {
      throw new IllegalArgumentException("Expect parameters to not be empty.");
    }
    if (!parameters.has(EncryptionConfigParser.ENCRYPTION_KEYSTORE_PASSWORD_KEY)
        && !parameters.has(EncryptionConfigParser.ENCRYPTION_KEYSTORE_PATH_KEY)) {
      throw new IllegalArgumentException("Expect either password or key file in the parameters.");
    }

    // keystore_password, optional if keystore_path is present
    // default to empty string as this is what GpgCodec expects
    String password = "";
    if (parameters.has(EncryptionConfigParser.ENCRYPTION_KEYSTORE_PASSWORD_KEY)) {
      password = parameters.get(EncryptionConfigParser.ENCRYPTION_KEYSTORE_PASSWORD_KEY).getAsString();
    }

    // keystore_path, optional, needed for secret keyring based decryption
    String keystorePathStr = null;
    if (parameters.has(EncryptionConfigParser.ENCRYPTION_KEYSTORE_PATH_KEY)) {
      keystorePathStr = parameters.get(EncryptionConfigParser.ENCRYPTION_KEYSTORE_PATH_KEY).getAsString();
    }
    // If key file not present, then password must be provided. Otherwise, password is optional
    if ((password == null || password.isEmpty()) && (keystorePathStr == null || keystorePathStr.isEmpty())) {
      throw new IllegalArgumentException("Both key and password cannot be empty.");
    }

    // key_name, optional, needed for encryption
    String keyName = null;
    if (parameters.has(EncryptionConfigParser.ENCRYPTION_KEY_NAME)) {
      keyName = parameters.get(EncryptionConfigParser.ENCRYPTION_KEY_NAME).getAsString();
    }

    // cipher, null to default to CAST5 (128 bit key, as per RFC 2144)
    String cipherName = null;
    if (parameters.has(EncryptionConfigParser.ENCRYPTION_CIPHER_KEY)) {
      cipherName = parameters.get(EncryptionConfigParser.ENCRYPTION_CIPHER_KEY).getAsString();
    }

    // if not using a keystore then use password based encryption
    if (keystorePathStr == null) {
      return new GPGCodec(password, cipherName);
    }
    // if a key name is not present then use a key id of 0. A GPGCodec may be configured without a key name
    // when used only for decryption where the key name is retrieved from the encrypted file
    return new GPGCodec(new Path(keystorePathStr), password,
        keyName == null ? 0 : Long.parseUnsignedLong(keyName, 16), cipherName);
  }
}

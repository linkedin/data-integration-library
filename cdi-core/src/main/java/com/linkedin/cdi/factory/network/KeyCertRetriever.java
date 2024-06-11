// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.factory.network;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableEntryException;
import java.security.cert.CertificateException;
import java.util.Base64;
import org.apache.gobblin.configuration.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.cdi.configuration.PropertyCollection.*;


/**
 * This is replicated from li-gobblin for cleaner dependency
 *
 * A {@link KeyCertRetriever} provides private key and cert that can be found from job configurations
 *
 * <p> It supports following scenarios:
 *  <li> Find key and cert loaded by Azkaban. The key and cert are extracted from the key store file
 */
public class KeyCertRetriever {
  private static final Logger LOG = LoggerFactory.getLogger(KeyCertRetriever.class);
  private String keyAlias;
  private String privateKey;
  private String cert;
  private String keyStoreFilePath;
  private String trustStoreFilePath;
  private String trustStorePassword;
  private String keyStorePassword;
  private String keyStoreType;
  private String keyPassword;

  public KeyCertRetriever(State state) {
    try {
      keyStoreFilePath = MSTAGE_SSL.getKeyStorePath(state);
      keyStorePassword = MSTAGE_SSL.getKeyStorePassword(state);
      keyStoreType = MSTAGE_SSL.getKeyStoreType(state);
      keyPassword = MSTAGE_SSL.getKeyPassword(state);
      trustStoreFilePath = MSTAGE_SSL.getTrustStorePath(state);
      trustStorePassword = MSTAGE_SSL.getTrustStorePassword(state);

      KeyStore keyStore = loadKeyStore(keyStoreFilePath, keyStoreType, keyStorePassword.toCharArray());
      keyAlias = keyStore.aliases().nextElement();
      LOG.info("Loading key and cert of alias {}", keyAlias);

      KeyStore.ProtectionParameter protectionParameter = new KeyStore.PasswordProtection(keyStorePassword.toCharArray());
      KeyStore.PrivateKeyEntry entry =
          (KeyStore.PrivateKeyEntry) keyStore.getEntry(keyAlias, protectionParameter);
      privateKey = Base64.getEncoder().encodeToString(entry.getPrivateKey().getEncoded());
      cert = Base64.getEncoder().encodeToString(entry.getCertificate().getEncoded());
    } catch (KeyStoreException | NoSuchAlgorithmException | CertificateException | UnrecoverableEntryException e) {
      LOG.error("Error while loading key and cert for Alias : {}", keyAlias);
      LOG.error("Error", e);
      throw new RuntimeException(e);
    }
  }

  private static KeyStore loadKeyStore(String keyStoreFile, String keyStoreType, char[] keyStorePassword) {
    try (FileInputStream inputStream = new FileInputStream(keyStoreFile)) {
      KeyStore keyStore = KeyStore.getInstance(keyStoreType);
      keyStore.load(inputStream, keyStorePassword);
      return keyStore;
    } catch (KeyStoreException | NoSuchAlgorithmException | CertificateException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  public String getKeyAlias() {
    return keyAlias;
  }

  public KeyCertRetriever setKeyAlias(String keyAlias) {
    this.keyAlias = keyAlias;
    return this;
  }

  public String getPrivateKey() {
    return privateKey;
  }

  public KeyCertRetriever setPrivateKey(String privateKey) {
    this.privateKey = privateKey;
    return this;
  }

  public String getCert() {
    return cert;
  }

  public KeyCertRetriever setCert(String cert) {
    this.cert = cert;
    return this;
  }

  public String getKeyStoreFilePath() {
    return keyStoreFilePath;
  }

  public KeyCertRetriever setKeyStoreFilePath(String keyStoreFilePath) {
    this.keyStoreFilePath = keyStoreFilePath;
    return this;
  }

  public String getTrustStoreFilePath() {
    return trustStoreFilePath;
  }

  public KeyCertRetriever setTrustStoreFilePath(String trustStoreFilePath) {
    this.trustStoreFilePath = trustStoreFilePath;
    return this;
  }

  public String getTrustStorePassword() {
    return trustStorePassword;
  }

  public KeyCertRetriever setTrustStorePassword(String trustStorePassword) {
    this.trustStorePassword = trustStorePassword;
    return this;
  }

  public String getKeyStorePassword() {
    return keyStorePassword;
  }

  public KeyCertRetriever setKeyStorePassword(String keyStorePassword) {
    this.keyStorePassword = keyStorePassword;
    return this;
  }

  public String getKeyStoreType() {
    return keyStoreType;
  }

  public KeyCertRetriever setKeyStoreType(String keyStoreType) {
    this.keyStoreType = keyStoreType;
    return this;
  }

  public String getKeyPassword() {
    return keyPassword;
  }

  public KeyCertRetriever setKeyPassword(String keyPassword) {
    this.keyPassword = keyPassword;
    return this;
  }
}

// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.keys;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;

public class S3Keys extends JobKeys {
  private static final Logger LOG = LoggerFactory.getLogger(S3Keys.class);
  private String bucket = "";
  private String endpoint = "";
  private String prefix = "";
  private String filesPattern = ".*";
  private Region region = Region.AWS_GLOBAL;
  private Integer maxKeys = 0;
  private String accessKey;
  private String secretId;
  private Integer connectionTimeout;
  String targetFilePattern;

  @Override
  public void logDebugAll() {
    LOG.debug("These are values in S3SourceV2:");
    LOG.debug("S3 Bucket: {}", bucket);
    LOG.debug("S3 endpoint: {}", endpoint);
    LOG.debug("S3 prefix: {}", prefix);
    LOG.debug("S3 files pattern: {}", filesPattern);
  }

  public String getBucket() {
    return bucket;
  }

  public void setBucket(String bucket) {
    this.bucket = bucket;
  }

  public String getEndpoint() {
    return endpoint;
  }

  public void setEndpoint(String endpoint) {
    this.endpoint = endpoint;
  }

  public String getPrefix() {
    return prefix;
  }

  public void setPrefix(String prefix) {
    this.prefix = prefix;
  }

  public String getFilesPattern() {
    return filesPattern;
  }

  public void setFilesPattern(String filesPattern) {
    this.filesPattern = filesPattern;
  }

  public Region getRegion() {
    return region;
  }

  public void setRegion(Region region) {
    this.region = region;
  }

  public Integer getMaxKeys() {
    return maxKeys;
  }

  public void setMaxKeys(Integer maxKeys) {
    this.maxKeys = maxKeys;
  }

  public String getAccessKey() {
    return accessKey;
  }

  public void setAccessKey(String accessKey) {
    this.accessKey = accessKey;
  }

  public String getSecretId() {
    return secretId;
  }

  public void setSecretId(String secretId) {
    this.secretId = secretId;
  }

  public Integer getConnectionTimeout() {
    return connectionTimeout;
  }

  public void setConnectionTimeout(Integer connectionTimeout) {
    this.connectionTimeout = connectionTimeout;
  }

  public String getTargetFilePattern() {
    return targetFilePattern;
  }

  public void setTargetFilePattern(String targetFilePattern) {
    this.targetFilePattern = targetFilePattern;
  }
}

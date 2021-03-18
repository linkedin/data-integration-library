// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.dil.keys;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.regions.Region;


@Getter (AccessLevel.PUBLIC)
@Setter (AccessLevel.PUBLIC)
@Slf4j
public class S3Keys extends JobKeys {
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
    super.logDebugAll();
    log.debug("These are values in S3SourceV2:");
    log.debug("S3 Bucket: {}", bucket);
    log.debug("S3 endpoint: {}", endpoint);
    log.debug("S3 prefix: {}", prefix);
    log.debug("S3 files pattern: {}", filesPattern);
  }
}

// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.keys;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;


@Getter (AccessLevel.PUBLIC)
@Setter (AccessLevel.PUBLIC)
@Slf4j
public class SftpKeys extends JobKeys {
  private String filesPattern = ".*";
  private String splitPattern = ":::";
  private String filesPath = "";
  private String baseDirectory = "";
  private String pathSeparator = "/";
  private String targetFilePattern;

  @Override
  public void logDebugAll() {
    super.logDebugAll();
    log.debug("These are values in SftpSource:");
    log.debug("sftp source path: {}", filesPath);
    log.debug("path separator: {}", pathSeparator);
    log.debug("split pattern: {}", splitPattern);
    log.debug("files pattern: {}", filesPattern);
    log.debug("Base directory: {}", baseDirectory);
  }
}

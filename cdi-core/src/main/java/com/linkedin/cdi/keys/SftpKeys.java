// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.keys;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SftpKeys extends JobKeys {
  private static final Logger LOG = LoggerFactory.getLogger(SftpKeys.class);
  private String filesPattern = ".*";
  private String splitPattern = ":::";
  private String filesPath = "";
  private String baseDirectory = "";
  private String pathSeparator = "/";
  private String targetFilePattern;

  @Override
  public void logDebugAll() {
    LOG.debug("These are values in SftpSource:");
    LOG.debug("sftp source path: {}", filesPath);
    LOG.debug("path separator: {}", pathSeparator);
    LOG.debug("split pattern: {}", splitPattern);
    LOG.debug("files pattern: {}", filesPattern);
    LOG.debug("Base directory: {}", baseDirectory);
  }

  public String getFilesPattern() {
    return filesPattern;
  }

  public void setFilesPattern(String filesPattern) {
    this.filesPattern = filesPattern;
  }

  public String getSplitPattern() {
    return splitPattern;
  }

  public void setSplitPattern(String splitPattern) {
    this.splitPattern = splitPattern;
  }

  public String getFilesPath() {
    return filesPath;
  }

  public void setFilesPath(String filesPath) {
    this.filesPath = filesPath;
  }

  public String getBaseDirectory() {
    return baseDirectory;
  }

  public void setBaseDirectory(String baseDirectory) {
    this.baseDirectory = baseDirectory;
  }

  public String getPathSeparator() {
    return pathSeparator;
  }

  public void setPathSeparator(String pathSeparator) {
    this.pathSeparator = pathSeparator;
  }

  public String getTargetFilePattern() {
    return targetFilePattern;
  }

  public void setTargetFilePattern(String targetFilePattern) {
    this.targetFilePattern = targetFilePattern;
  }
}

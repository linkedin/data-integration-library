// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.keys;

import org.apache.gobblin.source.workunit.WorkUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileDumpExtractorKeys extends ExtractorKeys {
  private static final Logger LOG = LoggerFactory.getLogger(FileDumpExtractorKeys.class);
  String fileName;
  String fileWritePermissions;
  String fileDumpLocation;
  private long currentFileNumber = 0;

  public String getFileName() {
    return fileName;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public String getFileWritePermissions() {
    return fileWritePermissions;
  }

  public void setFileWritePermissions(String fileWritePermissions) {
    this.fileWritePermissions = fileWritePermissions;
  }

  public String getFileDumpLocation() {
    return fileDumpLocation;
  }

  public void setFileDumpLocation(String fileDumpLocation) {
    this.fileDumpLocation = fileDumpLocation;
  }

  public long getCurrentFileNumber() {
    return currentFileNumber;
  }

  public void setCurrentFileNumber(long currentFileNumber) {
    this.currentFileNumber = currentFileNumber;
  }

  public long incrCurrentFileNumber() {
    return currentFileNumber++;
  }

  @Override
  public void logDebugAll(WorkUnit workUnit) {
    super.logDebugAll(workUnit);
    LOG.debug("These are values in FileDumpExtractor:");
    LOG.debug("Dumping data with file name - " + fileName);
    LOG.debug("Dumping data with permissions - " + fileWritePermissions);
    LOG.debug("Dumping data at location - " + fileDumpLocation);
    LOG.debug("Current file number - {}", currentFileNumber);
  }
}

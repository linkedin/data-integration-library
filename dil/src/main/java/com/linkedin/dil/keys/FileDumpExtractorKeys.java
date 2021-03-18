// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.dil.keys;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.source.workunit.WorkUnit;


@Slf4j
@Getter(AccessLevel.PUBLIC)
@Setter
public class FileDumpExtractorKeys extends ExtractorKeys {
  String fileName;
  String fileWritePermissions;
  String fileDumpLocation;
  @Getter
  private long currentFileNumber = 0;

  public long incrCurrentFileNumber() {
    return currentFileNumber++;
  }

  @Override
  public void logDebugAll(WorkUnit workUnit) {
    super.logDebugAll(workUnit);
    log.debug("These are values in FileDumpExtractor:");
    log.debug("Dumping data with file name - " + fileName);
    log.debug("Dumping data with permissions - " + fileWritePermissions);
    log.debug("Dumping data at location - " + fileDumpLocation);
    log.debug("Current file number - {}", currentFileNumber);
  }
}

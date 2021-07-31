// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.connection;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.State;
import com.linkedin.cdi.exception.RetriableAuthenticationException;
import com.linkedin.cdi.keys.ExtractorKeys;
import com.linkedin.cdi.keys.HdfsKeys;
import com.linkedin.cdi.keys.JobKeys;
import com.linkedin.cdi.util.InputStreamUtils;
import com.linkedin.cdi.util.WorkUnitStatus;
import org.apache.gobblin.source.extractor.filebased.FileBasedHelperException;
import org.apache.gobblin.source.extractor.filebased.TimestampAwareFileBasedHelper;
import org.apache.gobblin.source.extractor.hadoop.HadoopFsHelper;


/**
 * HdfsConnection creates transmission channel with HDFS data provider or HDFS data receiver,
 * and it executes commands per Extractor calls.
 *
 * @author Chris Li
 */
@Slf4j
public class HdfsConnection extends MultistageConnection {
  private final static String URI_REGEXP_PATTERN = "RE=";
  @Getter
  final private HdfsKeys hdfsKeys;
  @Setter (AccessLevel.PACKAGE)
  private TimestampAwareFileBasedHelper fsHelper;

  public HdfsConnection(State state, JobKeys jobKeys, ExtractorKeys extractorKeys) {
    super(state, jobKeys, extractorKeys);
    assert jobKeys instanceof HdfsKeys;
    hdfsKeys = (HdfsKeys) jobKeys;
  }

  /**
   * Get a list of files if the URI has pattern match, else read the file at the URI.
   *
   * In order to perform a list operation and output the list of files, the
   * ms.source.uri need to coded like /directory?RE=file-name-pattern. If the purpose
   * is to list all files, the file name pattern can be just ".*".
   *
   * In order to perform a read of a file and output the content of the file as
   * InputStream. ms.source.uri need to be full path without RE expression. If a
   * partial path is given, then only a single file will be picked because there
   * will be a list command performed before the read. A partial path could result
   * in multiple files being listed, but then only the first file will be used.
   *
   * So if the intention is to read a single file, support the full path to ms.source.uri.
   *
   * @param status prior work unit status
   * @return new work unit status
   */
  @Override
  public WorkUnitStatus execute(final WorkUnitStatus status) {
    assert hdfsKeys.getSourceUri() != null;
    URI uri = URI.create(getWorkUnitSpecificString(hdfsKeys.getSourceUri(),
        getExtractorKeys().getDynamicParameters()));

    if (uri.getPath() == null) {
      return status;
    }

    if (uri.getQuery() != null && uri.getQuery().matches(URI_REGEXP_PATTERN + ".*")) {
      status.setBuffer(InputStreamUtils.convertListToInputStream(
          readFileList(uri.getPath(), uri.getQuery().substring(URI_REGEXP_PATTERN.length()))));
    } else {
      List<String> files = readFileList(uri.getPath(), ".*");
      if (files.size() > 0) {
        status.setBuffer(readSingleFile(files.get(0)));
      }
    }
    return status;
  }

  /**
   * Close the connection to HDFS
   * @param message the message to send to the other end of connection upon closing
   * @return true if closed successfully, or false
   */
  @Override
  public boolean closeAll(String message) {
    try {
      fsHelper.close();
      fsHelper = null;
      return true;
    } catch (Exception e) {
      log.error("Error closing file system connection", e);
      return false;
    }
  }

  /**
   * execute the HDFS read command (ls or getFileStream)
   * @param workUnitStatus prior work unit status
   * @return the updated work unit status
   * @throws RetriableAuthenticationException if retry is needed
   */
  @Override
  public WorkUnitStatus executeFirst(final WorkUnitStatus workUnitStatus) throws RetriableAuthenticationException {
    WorkUnitStatus status = super.executeFirst(workUnitStatus);
    if (fsHelper == null) {
      fsHelper = getHdfsClient();
    }
    return execute(status);
  }

  /**
   * Read a list of files based on the given pattern
   * @param path base path of files
   * @param pattern file name pattern
   * @return a list of paths
   */
  private List<String> readFileList(final String path, final String pattern) {
    try {
      return this.fsHelper.ls(path)
          .stream()
          .filter(fileName -> fileName.matches(pattern))
          .collect(Collectors.toList());
    } catch (FileBasedHelperException e) {
      log.error("Not able to run ls command due to " + e.getMessage(), e);
    }
    return Lists.newArrayList();
  }

  /**
   * Read a single file from HDFS
   * @param path full path of the file
   * @return the file content in an InputStream
   */
  private InputStream readSingleFile(final String path) {
    try {
      return fsHelper.getFileStream(path);
    } catch (FileBasedHelperException e) {
      log.error("Not able to run getFileStream command due to " + e.getMessage(), e);
      return null;
    }
  }

  @VisibleForTesting
  TimestampAwareFileBasedHelper getHdfsClient() {
    TimestampAwareFileBasedHelper fsHelper = new HadoopFsHelper(this.getState());
    try {
      fsHelper.connect();
      return fsHelper;
    } catch (Exception e) {
      log.error("Failed to initialize HdfsSource", e);
      return null;
    }
  }
}

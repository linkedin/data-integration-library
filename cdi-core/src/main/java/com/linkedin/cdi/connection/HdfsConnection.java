// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.connection;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.linkedin.cdi.exception.RetriableAuthenticationException;
import com.linkedin.cdi.keys.ExtractorKeys;
import com.linkedin.cdi.keys.HdfsKeys;
import com.linkedin.cdi.keys.JobKeys;
import com.linkedin.cdi.util.InputStreamUtils;
import com.linkedin.cdi.util.WorkUnitStatus;
import java.io.InputStream;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.source.extractor.filebased.FileBasedHelperException;
import org.apache.gobblin.source.extractor.filebased.TimestampAwareFileBasedHelper;
import org.apache.gobblin.source.extractor.hadoop.HadoopFsHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * HdfsConnection creates transmission channel with HDFS data provider or HDFS data receiver,
 * and it executes commands per Extractor calls.
 *
 * @author Chris Li
 */
public class HdfsConnection extends MultistageConnection {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsConnection.class);
  private Iterator<String> fileListIterator = null;

  public HdfsKeys getHdfsKeys() {
    return hdfsKeys;
  }

  public TimestampAwareFileBasedHelper getFsHelper() {
    return fsHelper;
  }

  public void setFsHelper(TimestampAwareFileBasedHelper fsHelper) {
    this.fsHelper = fsHelper;
  }

  private final static String URI_REGEXP_PATTERN = "RE=";
  final private HdfsKeys hdfsKeys;
  private TimestampAwareFileBasedHelper fsHelper;

  public HdfsConnection(State state, JobKeys jobKeys, ExtractorKeys extractorKeys) {
    super(state, jobKeys, extractorKeys);
    assert jobKeys instanceof HdfsKeys;
    hdfsKeys = (HdfsKeys) jobKeys;
  }

  /**
   * Get a list of files if the URI has pattern match, else read the files at the URI.
   *
   * In order to perform a list operation and output the list of files, the
   * ms.source.uri need to coded like /directory?RE=file-name-pattern. If the purpose
   * is to list all files, the file name pattern can be just ".*".
   *
   * In order to perform a read of files and output the content of the file as
   * InputStream, ms.source.uri need to be a path without RE expression.
   *
   * If a partial path is given without RE expression, there will be a list command performed before the read.
   * The list of files will be processed through the pagination process, i.e., each
   * file is read as a page. In such case ms.pagination has to be set so that pagination
   * can take effect. If pagination is not enabled, then only the first file will
   * be read.
   *
   * If the intention is to read a single file, support the full path to ms.source.uri without
   * the RE expression.
   *
   * @param status prior work unit status
   * @return new work unit status
   */
  @Override
  public WorkUnitStatus execute(final WorkUnitStatus status) {
    Preconditions.checkNotNull(hdfsKeys.getSourceUri(), "ms.source.uri is missing or of wrong format");

    // If pagination has started, paginating through the files.
    // If no more files to process, then return directly. That should stop the
    // pagination process.
    if (fileListIterator != null) {
      if (fileListIterator.hasNext()) {
        status.setBuffer(readSingleFile(fileListIterator.next()));
      } else {
        status.setBuffer(null);
      }
      return status;
    }

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
      fileListIterator = files.iterator();
      if (fileListIterator.hasNext()) {
        status.setBuffer(readSingleFile(fileListIterator.next()));
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
    fileListIterator = null;
    try {
      fsHelper.close();
      fsHelper = null;
      return true;
    } catch (Exception e) {
      LOG.error("Error closing file system connection", e);
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
   * execute the HDFS read command (getFileStream)
   * @param workUnitStatus prior work unit status
   * @return the updated work unit status
   * @throws RetriableAuthenticationException if retry is needed
   */
  @Override
  public WorkUnitStatus executeNext(final WorkUnitStatus workUnitStatus) throws RetriableAuthenticationException {
    WorkUnitStatus status = super.executeNext(workUnitStatus);

    // If pagination has started already, but there is no more files to process,
    // then return directly to stop the process
    if (fileListIterator != null && !fileListIterator.hasNext()) {
      workUnitStatus.setBuffer(null);
      return workUnitStatus;
    }

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
      LOG.error("Not able to run ls command due to " + e.getMessage(), e);
    }
    return Lists.newArrayList();
  }

  /**
   * Read a single file from HDFS
   * @param path full path of the file
   * @return the file content in an InputStream
   */
  private InputStream readSingleFile(final String path) {
    LOG.info("Processing file: {}", path);
    try {
      return fsHelper.getFileStream(path);
    } catch (FileBasedHelperException e) {
      LOG.error("Not able to run getFileStream command due to " + e.getMessage(), e);
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
      LOG.error("Failed to initialize HdfsSource", e);
      return null;
    }
  }
}

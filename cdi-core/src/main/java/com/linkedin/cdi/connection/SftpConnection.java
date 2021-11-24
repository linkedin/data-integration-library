// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.connection;

import com.linkedin.cdi.exception.RetriableAuthenticationException;
import com.linkedin.cdi.factory.ConnectionClientFactory;
import com.linkedin.cdi.factory.sftp.SftpClient;
import com.linkedin.cdi.keys.ExtractorKeys;
import com.linkedin.cdi.keys.JobKeys;
import com.linkedin.cdi.keys.SftpKeys;
import com.linkedin.cdi.util.WorkUnitStatus;
import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.apache.gobblin.configuration.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.cdi.configuration.PropertyCollection.*;


public class SftpConnection extends MultistageConnection {
  private static final Logger LOG = LoggerFactory.getLogger(SftpConnection.class);

  final private SftpKeys sftpSourceKeys;
  SftpClient fsClient;

  public SftpConnection(State state, JobKeys jobKeys, ExtractorKeys extractorKeys) {
    super(state, jobKeys, extractorKeys);
    assert jobKeys instanceof SftpKeys;
    sftpSourceKeys = (SftpKeys) jobKeys;
  }

  @Override
  public boolean closeAll(String message) {
    if (this.fsClient != null) {
      LOG.info("Shutting down FileSystem connection");
      this.fsClient.close();
      fsClient = null;
    }
    return true;
  }

  @Override
  public WorkUnitStatus execute(WorkUnitStatus status) {
    String path = getPath();
    String finalPrefix = getWorkUnitSpecificString(path, getExtractorKeys().getDynamicParameters());
    LOG.info("File path found is: " + finalPrefix);
    try {
      if (getFsClient() == null) {
        LOG.error("Error initializing SFTP connection");
        return null;
      }
    } catch (Exception e) {
      LOG.error("Error initializing SFTP connection", e);
      return null;
    }

    //get List of files matching the pattern
    List<String> files;
    try {
      files = getFiles(finalPrefix).stream()
          .filter(objectKey -> objectKey.matches(sftpSourceKeys.getFilesPattern()))
          .collect(Collectors.toList());
    } catch (Exception e) {
      LOG.error("Error reading file list", e);
      return null;
    }

    LOG.info("No Of Files to be processed matching the pattern: {}", files.size());

    if (StringUtils.isBlank(sftpSourceKeys.getTargetFilePattern())) {
      status.setBuffer(wrap(files));
    } else {
      String fileToDownload = files.size() == 0 ? StringUtils.EMPTY : files.get(0);
      if (StringUtils.isNotBlank(fileToDownload)) {
        LOG.info("Downloading file: {}", fileToDownload);
        try {
          status.setBuffer(this.fsClient.getFileStream(fileToDownload));
        } catch (Exception e) {
          LOG.error("Error downloading file {}", fileToDownload, e);
          return null;
        }
      } else {
        LOG.warn("Invalid set of parameters. Please make sure to set source directory, entity and file pattern");
      }
    }
    return status;
  }

  /**
   * @param workUnitStatus prior work unit status
   * @return new work unit status
   * @throws RetriableAuthenticationException
   */
  @Override
  public WorkUnitStatus executeFirst(WorkUnitStatus workUnitStatus) throws RetriableAuthenticationException {
    WorkUnitStatus status = super.executeFirst(workUnitStatus);
    return this.execute(status);
  }

  /**
   * @param workUnitStatus prior work unit status
   * @return new work unit status
   * @throws RetriableAuthenticationException
   */
  @Override
  public WorkUnitStatus executeNext(WorkUnitStatus workUnitStatus) throws RetriableAuthenticationException {
    WorkUnitStatus status = super.executeNext(workUnitStatus);
    return this.execute(status);
  }

  private SftpClient getFsClient() {
    if (this.fsClient == null) {
      try {
        Class<?> factoryClass = Class.forName(MSTAGE_CONNECTION_CLIENT_FACTORY.get(this.getState()));
        ConnectionClientFactory factory = (ConnectionClientFactory) factoryClass.getDeclaredConstructor().newInstance();
        this.fsClient = factory.getSftpChannelClient(this.getState());
      } catch (Exception e) {
        LOG.error("Error initiating SFTP client", e);
      }
    }
    return this.fsClient;
  }

  /**
   * //TODO: List files based on pattern on parent nodes as well.
   * The current version supports pattern only on leaf node.
   * Ex: file path supported "/a/b/*c*"
   * file path not supported "/a/*b/*c*
   * Get files list based on pattern
   * @param filesPattern pattern of content to list
   * @return list of content
   */
  private List<String> getFiles(String filesPattern) {
    LOG.info("Files to be processed from input " + filesPattern);
    try {
      List<String> files = fsClient.ls(filesPattern, 2);
      int i = 0;
      for (String file : files) {
        URI uri = new URI(file);
        String filepath = uri.toString();
        if (!uri.isAbsolute()) {
          File f = new File(getBaseDir(filesPattern), filepath);
          filepath = f.getAbsolutePath();
        }
        files.set(i, filepath);
        i++;
      }
      return files;
    } catch (Exception e) {
      LOG.error("Unable to list files after 2 tries. {}", e.getMessage());
      throw new RuntimeException(e);
    }
  }
  private String getPath() {
    return sftpSourceKeys.getFilesPath();
  }

  private List<String> getFilteredFiles(List<String> files) {
    return files.stream().filter(file -> file.matches(sftpSourceKeys.getFilesPattern())).collect(Collectors.toList());
  }

  private String getBaseDir(String uri) {
    File file = new File(uri);
    return file.getParentFile().getAbsolutePath() + sftpSourceKeys.getPathSeparator();
  }

}

// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.connection;

import com.google.common.collect.Lists;
import com.linkedin.cdi.exception.RetriableAuthenticationException;
import com.linkedin.cdi.factory.ConnectionClientFactory;
import com.linkedin.cdi.keys.ExtractorKeys;
import com.linkedin.cdi.keys.JobKeys;
import com.linkedin.cdi.keys.S3Keys;
import com.linkedin.cdi.util.EncryptionUtils;
import com.linkedin.cdi.util.InputStreamUtils;
import com.linkedin.cdi.util.WorkUnitStatus;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.apache.gobblin.configuration.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.utils.AttributeMap;

import static com.linkedin.cdi.configuration.PropertyCollection.*;
import static software.amazon.awssdk.http.SdkHttpConfigurationOption.*;

/**
 * S3Connection creates transmission channel with AWS S3 data provider or AWS S3 data receiver,
 * and it executes commands per Extractor calls.
 *
 * @author Chris Li
 */
public class S3Connection extends MultistageConnection {
  private static final Logger LOG = LoggerFactory.getLogger(S3Connection.class);
  final private S3Keys s3SourceV2Keys;
  private S3Client s3Client = null;

  public S3Keys getS3SourceV2Keys() {
    return s3SourceV2Keys;
  }

  public S3Client getS3Client() {
    return s3Client;
  }

  public void setS3Client(S3Client s3Client) {
    this.s3Client = s3Client;
  }

  public S3Connection(State state, JobKeys jobKeys, ExtractorKeys extractorKeys) {
    super(state, jobKeys, extractorKeys);
    assert jobKeys instanceof S3Keys;
    s3SourceV2Keys = (S3Keys) jobKeys;
  }

  @Override
  public WorkUnitStatus execute(WorkUnitStatus status) {
    return null;
  }

  @Override
  public boolean closeAll(String message) {
    return true;
  }

  /*
  Below is the logic of when to download a file and when to list similar files based on the uri and pattern
  ms.source.files.pattern
      if Is not blank:
            List the S3 keys and output as CSV

      if Is blank:
            ms.extractor.target.file.name?
                  If is blank:
                        List the S3 keys and output as CSV
                  If is not blank:
                        If ms.source.uri prefix produces only 1 file:
                              dump the S3 object into the given output file name
                        If ms.source.uir prefix produces more than 1 file:
                              dump only the file where prefix = object key, and ignore all other objects
 */
  @Override
  public WorkUnitStatus executeFirst(WorkUnitStatus workUnitStatus) throws RetriableAuthenticationException {
    WorkUnitStatus status = super.executeFirst(workUnitStatus);
    s3Client = getS3HttpClient(getState());

    String finalPrefix = getWorkUnitSpecificString(s3SourceV2Keys.getPrefix(), getExtractorKeys().getDynamicParameters());
    LOG.debug("Final Prefix to get files list: {}", finalPrefix);
    try {
      List<String> files = getFilesList(finalPrefix);
      boolean isObjectWithPrefixExist = files.stream().anyMatch(objectKey -> objectKey.equals(finalPrefix));
      LOG.debug("Number of files identified: {}", files.size());

      if (StringUtils.isNotBlank(s3SourceV2Keys.getFilesPattern())) {
        List<String> filteredFiles = files.stream()
            .filter(fileName -> fileName.matches(s3SourceV2Keys.getFilesPattern()))
            .collect(Collectors.toList());
        status.setBuffer(InputStreamUtils.convertListToInputStream(filteredFiles));
      } else {
        if (StringUtils.isBlank(s3SourceV2Keys.getTargetFilePattern())) {
          status.setBuffer(InputStreamUtils.convertListToInputStream(files));
        } else {
          String fileToDownload = "";
          if (files.size() == 1) {
            fileToDownload = files.get(0);
          } else if (isObjectWithPrefixExist) {
            fileToDownload = finalPrefix;
          }
          if (StringUtils.isNotBlank(fileToDownload)) {
            LOG.debug("Downloading file: {}", files.get(0));
            GetObjectRequest getObjectRequest =
                GetObjectRequest.builder().bucket(s3SourceV2Keys.getBucket()).key(files.get(0)).build();
            ResponseInputStream<GetObjectResponse> response =
                s3Client.getObject(getObjectRequest, ResponseTransformer.toInputStream());
            status.setBuffer(response);
          } else {
            LOG.warn("Invalid set of parameters. To list down files from a bucket, pattern "
                + "parameter is needed and to get object from s3 source target file name is needed.");
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Unexpected Exception", e);
      return null;
    }
    return status;
  }

  /**
   * Thread-safely create S3Client as needed
   */
  synchronized S3Client getS3HttpClient(State state) {
    if (s3Client == null) {
      try {
        Class<?> factoryClass = Class.forName(MSTAGE_CONNECTION_CLIENT_FACTORY.getValidNonblankWithDefault(state));
        ConnectionClientFactory factory = (ConnectionClientFactory) factoryClass.getDeclaredConstructor().newInstance();

        Integer connectionTimeout = s3SourceV2Keys.getConnectionTimeout();
        AttributeMap config = connectionTimeout == null ? GLOBAL_HTTP_DEFAULTS
            : GLOBAL_HTTP_DEFAULTS.toBuilder()
                .put(CONNECTION_TIMEOUT, Duration.ofSeconds(connectionTimeout))
                .build();

        s3Client = S3Client.builder()
            .region(this.s3SourceV2Keys.getRegion())
            .endpointOverride(URI.create(s3SourceV2Keys.getEndpoint()))
            .httpClient(factory.getS3Client(state, config))
            .credentialsProvider(getCredentialsProvider(state))
            .build();
      } catch (Exception e) {
        LOG.error("Error creating S3 Client: {}", e.getMessage());
      }
    }
    return s3Client;
  }

  /**
   * retrieve a list of objects given a bucket name and a prefix
   * @return list of object keys
   */
  private List<String> getFilesList(String finalPrefix) {
    List<String> files = Lists.newArrayList();
    ListObjectsV2Request.Builder builder =
        ListObjectsV2Request.builder().bucket(s3SourceV2Keys.getBucket()).maxKeys(s3SourceV2Keys.getMaxKeys());

    if (!finalPrefix.isEmpty()) {
      builder.prefix(finalPrefix);
    }
    ListObjectsV2Request request = builder.build();
    ListObjectsV2Response listObjectsV2Response = null;

    LOG.debug("Listing object by prefix: {}", finalPrefix);
    do {
      if (listObjectsV2Response != null) {
        request = builder.continuationToken(listObjectsV2Response.continuationToken()).build();
      }
      listObjectsV2Response = s3Client.listObjectsV2(request);
      listObjectsV2Response.contents().forEach(f -> {
        files.add(f.key());
      });
    } while (listObjectsV2Response.isTruncated());
    return files;
  }

  public AwsCredentialsProvider getCredentialsProvider(State state) {
    AwsCredentialsProvider credentialsProvider = AnonymousCredentialsProvider.create();
    if (StringUtils.isNotBlank(s3SourceV2Keys.getAccessKey()) || StringUtils.isNotEmpty(s3SourceV2Keys.getSecretId())) {
      AwsCredentials credentials =
          AwsBasicCredentials.create(EncryptionUtils.decryptGobblin(s3SourceV2Keys.getAccessKey(), state),
              EncryptionUtils.decryptGobblin(s3SourceV2Keys.getSecretId(), state));
      credentialsProvider = StaticCredentialsProvider.create(credentials);
    }
    return credentialsProvider;
  }
}

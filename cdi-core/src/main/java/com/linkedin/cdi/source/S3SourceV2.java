// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.source;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import com.linkedin.cdi.connection.S3Connection;
import com.linkedin.cdi.extractor.MultistageExtractor;
import com.linkedin.cdi.keys.S3Keys;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.Extractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;

import static com.linkedin.cdi.configuration.PropertyCollection.*;


public class S3SourceV2 extends MultistageSource<Schema, GenericRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(S3SourceV2.class);
  private static final String KEY_REGION = "region";
  private static final String KEY_CONNECTION_TIMEOUT = "connection_timeout";
  private static final HashSet<String> S3_REGIONS_SET =
      Region.regions().stream().map(region -> region.toString()).collect(Collectors.toCollection(HashSet::new));

  private static final String KEY_BUCKET_NAME = "bucket_name";

  private S3Keys s3SourceV2Keys = new S3Keys();

  public S3Keys getS3SourceV2Keys() {
    return s3SourceV2Keys;
  }

  public void setS3SourceV2Keys(S3Keys s3SourceV2Keys) {
    this.s3SourceV2Keys = s3SourceV2Keys;
  }

  public S3SourceV2() {
    s3SourceV2Keys = new S3Keys();
    jobKeys = s3SourceV2Keys;
  }
  protected void initialize(State state) {
    super.initialize(state);

    URL url = null;
    try {
      String sourceUri = MSTAGE_SOURCE_URI.get(state);
      url = new URL(sourceUri.replaceAll("(s3|S3)://", "https://"));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    if (url == null || url.getHost().isEmpty()) {
      throw new RuntimeException("Incorrect configuration in " +
          MSTAGE_SOURCE_URI.toString());
    }

    // set region, note that aws SDK won't raise an error here if region is invalid,
    // later on, an exception will be raised when the actual request is issued
    JsonObject parameters = MSTAGE_SOURCE_S3_PARAMETERS.get(state);
    if (parameters.has(KEY_REGION)) {
      String region = parameters.get(KEY_REGION).getAsString();
      if (!S3_REGIONS_SET.contains(region)) {
        throw new IllegalArgumentException(region + " is not a valid S3 region.");
      }
      s3SourceV2Keys.setRegion(Region.of(region));
    } else {
      // Default to us-west-2
      s3SourceV2Keys.setRegion(Region.US_WEST_2);
    }

    // set S3 connection timeout, non-positive integers are rejected
    if (parameters.has(KEY_CONNECTION_TIMEOUT)) {
      int connectionTimeout = parameters.get(KEY_CONNECTION_TIMEOUT).getAsInt();
      if (connectionTimeout <= 0) {
        throw new IllegalArgumentException(connectionTimeout + " is not a valid timeout value.");
      }
      s3SourceV2Keys.setConnectionTimeout(connectionTimeout);
    }

    // separate the endpoint, which should be a URL without bucket name, from the domain name
    s3SourceV2Keys.setEndpoint("https://" + getEndpoint(parameters, url.getHost()));
    s3SourceV2Keys.setPrefix(url.getPath().substring(1));

    // separate the bucket name from URI domain name
    s3SourceV2Keys.setBucket(getBucketName(parameters, url.getHost()));

    s3SourceV2Keys.setFilesPattern(MSTAGE_SOURCE_FILES_PATTERN.get(state));
    s3SourceV2Keys.setMaxKeys(MSTAGE_S3_LIST_MAX_KEYS.get(state));
    s3SourceV2Keys.setAccessKey(SOURCE_CONN_USERNAME.get(state));
    s3SourceV2Keys.setSecretId(SOURCE_CONN_PASSWORD.get(state));
    s3SourceV2Keys.setTargetFilePattern(
        MSTAGE_EXTRACTOR_TARGET_FILE_NAME.get(state));
    s3SourceV2Keys.logDebugAll();
  }

  /**
   * Create extractor based on the input WorkUnitState, the extractor.class
   * configuration, and a new S3Connection
   *
   * @param state WorkUnitState passed in from Gobblin framework
   * @return the MultistageExtractor object
   */

  @Override
  public Extractor<Schema, GenericRecord> getExtractor(WorkUnitState state) {
    initialize(state);
    MultistageExtractor<Schema, GenericRecord> extractor =
        (MultistageExtractor<Schema, GenericRecord>) super.getExtractor(state);
    extractor.setConnection(new S3Connection(state, this.s3SourceV2Keys, extractor.getExtractorKeys()));
    return extractor;
  }

  /**
   * split the host name, and remove the bucket name from the beginning, and return the rest
   * @param host hostname with bucket name in the beginning
   * @return the endpoint name without the bucket name
   */
  private String getEndpointFromHost(String host) {
    List<String> segments = Lists.newArrayList(host.split("\\."));
    Preconditions.checkArgument(segments.size() > 1, "Host name format is incorrect");
    segments.remove(0);
    return Joiner.on(".").join(segments);
  }

  /**
   *
   * @param parameters JsonObject containing ms.source.s3.parameters
   * @param host hostname with bucket name in the beginning
   * @return the bucket name
   */
  @VisibleForTesting
  protected String getBucketName(JsonObject parameters, String host) {
    if (parameters.has(KEY_BUCKET_NAME)) {
      return parameters.get(KEY_BUCKET_NAME).getAsString();
    }
    return host.split("\\.")[0];
  }

  /**
   *
   * @param parameters JsonObject containing ms.source.s3.parameters
   * @param host hostname with bucket name in the beginning
   * @return the endpoint name if bucket name is present in the parameters then removes the bucket name from host and
   * calls the getEndpointFromHost method to get the endpoint.
   */
  @VisibleForTesting
  protected String getEndpoint(JsonObject parameters, String host) {
    if (parameters.has(KEY_BUCKET_NAME)) {
      String bucketName = parameters.get(KEY_BUCKET_NAME).getAsString().toLowerCase();
      host = host.toLowerCase(Locale.ROOT).replace(bucketName, "");
    }
    return getEndpointFromHost(host);
  }
}

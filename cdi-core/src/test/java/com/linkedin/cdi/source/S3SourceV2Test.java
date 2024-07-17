// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.source;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.UnsupportedEncodingException;
import org.apache.gobblin.configuration.WorkUnitState;
import org.testng.Assert;
import org.testng.annotations.Test;
import software.amazon.awssdk.regions.Region;

@Test
public class S3SourceV2Test {
  /**
   * This test depends on a publicly available common crawl file. That's why it is disabled by default.
   *
   * @throws UnsupportedEncodingException
   */
  @Test (enabled = false)
  public void testInitialization() throws UnsupportedEncodingException {
    S3SourceV2 source = new S3SourceV2();
    WorkUnitState state = new WorkUnitState();
    state.setProp("ms.source.uri", "https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2019-43/cc-index.paths.gz");
    state.setProp("ms.extractor.class", "com.linkedin.cdi.extractor.CsvExtractor");
    JsonObject params = new JsonObject();
    params.addProperty("region", "us-east-1");
    params.addProperty("connection_timeout", 30);
    state.setProp("ms.source.s3.parameters", params);
    source.getExtractor(state);
    Assert.assertEquals(source.getS3SourceV2Keys().getBucket(), "commoncrawl");
    Assert.assertEquals(source.getS3SourceV2Keys().getRegion().id(), Region.US_EAST_1.id());
    Assert.assertEquals(source.getS3SourceV2Keys().getEndpoint(), "https://s3.amazonaws.com");
    Assert.assertEquals(source.getS3SourceV2Keys().getPrefix(), "crawl-data/CC-MAIN-2019-43/cc-index.paths.gz");
    Assert.assertEquals(source.getS3SourceV2Keys().getMaxKeys(), new Integer(1000));
    Assert.assertEquals(source.getS3SourceV2Keys().getConnectionTimeout(), new Integer(30));
  }

  @Test
  public void testBucketName() {
    S3SourceV2 s3SourceV2 = new S3SourceV2();
    JsonObject parameteres =
        new Gson().fromJson("{\"region\" : \"us-east-2\", \"bucket_name\" : \"collect-us-west-2.tealium.com\"}",
            JsonObject.class);
    String host = "collect-us-west-2.tealium.com.s3.amazonaws.com";
    String bucketName = s3SourceV2.getBucketName(parameteres, host);
    Assert.assertEquals(bucketName, "collect-us-west-2.tealium.com");
  }

  @Test
  public void testBucketNameWithoutBucketParameter() {
    S3SourceV2 s3SourceV2 = new S3SourceV2();
    JsonObject parameteres = new Gson().fromJson("{\"region\" : \"us-east-2\"}", JsonObject.class);
    String host = "collect-us-west-2.s3.amazonaws.com";
    String bucketName = s3SourceV2.getBucketName(parameteres, host);
    Assert.assertEquals(bucketName, "collect-us-west-2");
  }

  @Test
  public void testEndpoint() {
    S3SourceV2 s3SourceV2 = new S3SourceV2();
    JsonObject parameteres =
        new Gson().fromJson("{\"region\" : \"us-east-2\", \"bucket_name\" : \"colleCt-us-west-2.tealium.com\"}",
            JsonObject.class);
    String host = "collect-us-west-2.tealium.com.s3.amazonaws.com";
    String endpoint = s3SourceV2.getEndpoint(parameteres, host);
    Assert.assertEquals(endpoint, "s3.amazonaws.com");
  }

  @Test
  public void testEndpointWithoutPeriod() {
    S3SourceV2 s3SourceV2 = new S3SourceV2();
    JsonObject parameters =
        new Gson().fromJson("{\"region\" : \"us-east-2\", \"bucket_name\" : \"collect-us-west-2\"}", JsonObject.class);
    String host = "collect-us-west-2.s3.amazonaws.com";
    String endpoint = s3SourceV2.getEndpoint(parameters, host);
    Assert.assertEquals(endpoint, "s3.amazonaws.com");
  }
}

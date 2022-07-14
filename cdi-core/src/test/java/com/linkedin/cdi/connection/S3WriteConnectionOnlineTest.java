// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.connection;

import org.apache.gobblin.runtime.embedded.EmbeddedGobblin;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * These online tests are true Gobblin jobs. Their execution depends on:
 * 1. a complete pull file
 * 2. the online resource that is being pulled
 *
 * To execute these tests or debug with them, please enable the test and ensure
 * above conditions are met.
 */
public class S3WriteConnectionOnlineTest {
  @Test(enabled = false)
  void testS3ClientWithApacheHttpClient() throws Exception {
    EmbeddedGobblin job = new EmbeddedGobblin("test");
    Assert.assertTrue(job.jobFile(getClass().getResource("/pull/s3-upload.pull").getPath()).run().isSuccessful());
  }
}

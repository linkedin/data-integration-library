// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.
package com.linkedin.cdi.source;

import org.apache.gobblin.runtime.embedded.EmbeddedGobblin;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SimpleIntegrationTest {
  @Test(enabled = false)
  void fileDumpJobTask() throws Exception {
    Logger.getRootLogger().setLevel(Level.INFO);
    EmbeddedGobblin job = new EmbeddedGobblin("SimpleIntegrationTestJob");
    Assert.assertTrue(job.jobFile(getClass().getResource("/pull/metrics_integration_test.pull").getPath())
        .run()
        .isSuccessful());
  }

  @Test(enabled = false)
  void httpIntegrationTask() throws Exception {
    Logger.getRootLogger().setLevel(Level.INFO);
    EmbeddedGobblin job = new EmbeddedGobblin("IMDB_Ratings_ingestion");
    Assert.assertTrue(job.jobFile(getClass().getResource("/pull/imdb-example.pull").getPath())
        .run()
        .isSuccessful());
  }
}

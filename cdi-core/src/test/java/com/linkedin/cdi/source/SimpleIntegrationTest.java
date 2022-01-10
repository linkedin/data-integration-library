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
    EmbeddedGobblin job = new EmbeddedGobblin("SimpleIntegrationTest Job");
    Assert.assertTrue(job.jobFile(getClass().getResource("/pull/metrics_integration_test.pull").getPath())
        .run()
        .isSuccessful());
  }
}

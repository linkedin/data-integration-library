// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.dil.connection;

import gobblin.runtime.JobState;
import java.util.List;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import com.linkedin.dil.configuration.MultistageProperties;
import com.linkedin.dil.keys.ExtractorKeys;
import com.linkedin.dil.source.MultistageSource;
import com.linkedin.dil.source.S3SourceV2;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test
public class S3ReadConnectionTest {
  @Test
  public void testGetS3HttpClient() {
    List<WorkUnit> wus = new MultistageSource().getWorkunits(new SourceState());
    WorkUnitState wuState = new WorkUnitState(wus.get(0), new JobState());
    wuState.setProp(MultistageProperties.MSTAGE_HTTP_CLIENT_FACTORY.getConfig(), "com.linkedin.dil.factory.ApacheHttpClientFactory");

    S3SourceV2 source = new S3SourceV2();
    SourceState sourceState = new SourceState();
    sourceState.setProp(MultistageProperties.MSTAGE_SOURCE_URI.getConfig(), "https://nonexist.s3.amazonaws.com/data");
    source.getWorkunits(sourceState);

    S3Connection conn = new S3Connection(wuState, source.getS3SourceV2Keys(), new ExtractorKeys());
    Assert.assertNotNull(conn.getS3HttpClient(wuState));

    conn = new S3Connection(wuState, source.getS3SourceV2Keys(), new ExtractorKeys());
    conn.getS3SourceV2Keys().setConnectionTimeout(10);
    Assert.assertNotNull(conn.getS3HttpClient(wuState));
  }
}

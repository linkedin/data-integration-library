// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.connection;

import com.linkedin.cdi.keys.ExtractorKeys;
import com.linkedin.cdi.source.MultistageSource;
import com.linkedin.cdi.source.S3SourceV2;
import gobblin.runtime.JobState;
import java.util.List;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.cdi.configuration.PropertyCollection.*;


@Test
public class S3ReadConnectionTest {
  @Test
  public void testGetS3HttpClient() {
    SourceState sourceState = new SourceState();
    sourceState.setProp("extract.table.name", "xxx");
    List<WorkUnit> wus = new MultistageSource().getWorkunits(sourceState);
    WorkUnitState wuState = new WorkUnitState(wus.get(0), new JobState());
    wuState.setProp(MSTAGE_CONNECTION_CLIENT_FACTORY.getConfig(), "com.linkedin.cdi.factory.DefaultConnectionClientFactory");

    S3SourceV2 source = new S3SourceV2();
    sourceState = new SourceState();
    sourceState.setProp("extract.table.name", "xxx");
    sourceState.setProp(MSTAGE_SOURCE_URI.getConfig(), "https://nonexist.s3.amazonaws.com/data");
    source.getWorkunits(sourceState);

    S3Connection conn = new S3Connection(wuState, source.getS3SourceV2Keys(), new ExtractorKeys());
    Assert.assertNotNull(conn.getS3HttpClient(wuState));

    conn = new S3Connection(wuState, source.getS3SourceV2Keys(), new ExtractorKeys());
    conn.getS3SourceV2Keys().setConnectionTimeout(10);
    Assert.assertNotNull(conn.getS3HttpClient(wuState));
  }
}

// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.configuration;

import com.google.gson.JsonArray;
import com.linkedin.cdi.extractor.MultistageExtractor;
import com.linkedin.cdi.keys.JobKeys;
import com.linkedin.cdi.source.HdfsSource;
import gobblin.runtime.JobState;
import java.util.List;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.cdi.configuration.PropertyCollection.*;
import static com.linkedin.cdi.configuration.StaticConstants.*;


public class SecondaryInputPropertiesTest {
  @Test
  public void testVariableInPayloadPath() {
    SourceState sourceState = new SourceState();
    sourceState.setProp("extract.table.name", "xxx");
    sourceState.setProp("ms.secondary.input", "[{\"path\": \"{{customerId}}\", \"fields\": [\"dummy\"], \"category\": \"payload\"}]");
    sourceState.setProp("ms.source.uri", "/data/test?RE=.*");
    sourceState.setProp("ms.watermark", "[{\"name\":\"customerId\",\"type\":\"unit\",\"units\":\"dir1, dir2, dir3\"}]");
    sourceState.setProp(MSTAGE_CONNECTION_CLIENT_FACTORY.getConfig(),
        "com.linkedin.cdi.factory.DefaultConnectionClientFactory");

    Assert.assertTrue(new JobKeys().validate(sourceState));

    HdfsSource source = new HdfsSource();
    List<WorkUnit> wus = source.getWorkunits(sourceState);

    // check 1st work unit
    WorkUnitState wuState = new WorkUnitState(wus.get(0), new JobState());
    wuState.setProp("ms.extractor.class", "com.linkedin.cdi.extractor.CsvExtractor");
    MultistageExtractor extractor = (MultistageExtractor) source.getExtractor(wuState);
    Assert.assertNotNull(extractor);
    JsonArray payloads = extractor.getExtractorKeys().getPayloads();
    Assert.assertEquals(payloads.size(), 1);
    Assert.assertEquals(payloads.get(0).getAsJsonObject().get(KEY_WORD_PATH).getAsString(), "dir1");

    // check 2nd work unit
    wuState = new WorkUnitState(wus.get(1), new JobState());
    wuState.setProp("ms.extractor.class", "com.linkedin.cdi.extractor.CsvExtractor");
    extractor = (MultistageExtractor) source.getExtractor(wuState);
    Assert.assertNotNull(extractor);
    payloads = extractor.getExtractorKeys().getPayloads();
    Assert.assertEquals(payloads.size(), 1);
    Assert.assertEquals(payloads.get(0).getAsJsonObject().get(KEY_WORD_PATH).getAsString(), "dir2");

  }
}

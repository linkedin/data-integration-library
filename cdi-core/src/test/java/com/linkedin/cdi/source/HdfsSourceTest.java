// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.source;

import gobblin.configuration.SourceState;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import org.testng.Assert;
import org.testng.annotations.Test;


public class HdfsSourceTest {
  @Test
  public void testInitialize() {
    HdfsSource hdfsSource = new HdfsSource();
    SourceState state = new SourceState();
    state.setProp("extract.table.name", "xxx");
    Assert.assertTrue(hdfsSource.getWorkunits(state).size() > 0);
  }

  @Test
  public void testGetExtractor() {
    HdfsSource hdfsSource = new HdfsSource();
    SourceState sourceState = new SourceState();
    sourceState.setProp("extract.table.name", "xxx");
    WorkUnitState state = new WorkUnitState(hdfsSource.getWorkunits(sourceState).get(0), new State());
    state.setProp("ms.extractor.class", "com.linkedin.cdi.extractor.CsvExtractor");
    hdfsSource.getExtractor(state);
    Assert.assertNotNull(hdfsSource.getExtractor(state));
  }
}

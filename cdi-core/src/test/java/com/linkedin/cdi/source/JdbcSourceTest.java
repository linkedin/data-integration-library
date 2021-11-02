// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.source;

import gobblin.configuration.SourceState;
import org.testng.Assert;
import org.testng.annotations.Test;


public class JdbcSourceTest {
  @Test
  public void testInitialize() {
    JdbcSource jdbcSource = new JdbcSource();
    SourceState state = new SourceState();
    state.setProp("extract.table.name", "xxx");
    Assert.assertNotNull(jdbcSource.getWorkunits(state));
  }
}

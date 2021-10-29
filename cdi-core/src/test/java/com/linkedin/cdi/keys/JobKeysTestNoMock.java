// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.keys;

import gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.State;
import org.junit.Assert;
import org.testng.annotations.Test;


public class JobKeysTestNoMock {
  @Test
  public void testValidate() {
    State state = new SourceState();
    state.setProp("csv.max.failures", "10");
    Assert.assertFalse(new JobKeys().validate(state));
  }
}

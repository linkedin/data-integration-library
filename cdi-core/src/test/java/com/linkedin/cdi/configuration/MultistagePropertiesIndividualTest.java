// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.configuration;

import com.google.gson.JsonObject;
import org.apache.gobblin.configuration.SourceState;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.cdi.configuration.PropertyCollection.*;


public class MultistagePropertiesIndividualTest {
  @Test
  public void testMsAuthentication() {
    SourceState state = new SourceState();
    Assert.assertEquals(MSTAGE_AUTHENTICATION.getValidNonblankWithDefault(state), new JsonObject());

    state.setProp("ms.authentication", "[0, 1, 2]");
    Assert.assertFalse(MSTAGE_AUTHENTICATION.isValid(state));
    Assert.assertEquals(MSTAGE_AUTHENTICATION.getValidNonblankWithDefault(state), new JsonObject());

    state.setProp("ms.authentication", "{\"name\": \"value\"");
    Assert.assertFalse(MSTAGE_AUTHENTICATION.isValid(state));
    Assert.assertFalse(MSTAGE_AUTHENTICATION.validateNonblank(state));
    Assert.assertEquals(MSTAGE_AUTHENTICATION.getValidNonblankWithDefault(state), new JsonObject());

    state.setProp("ms.authentication", "{\"method\": \"bearer\"}");
    Assert.assertFalse(MSTAGE_AUTHENTICATION.isValid(state));
    Assert.assertFalse(MSTAGE_AUTHENTICATION.validateNonblank(state));
    Assert.assertEquals(MSTAGE_AUTHENTICATION.getValidNonblankWithDefault(state), new JsonObject());

    state.setProp("ms.authentication", "{\"method\": \"bearer\", \"encryption\": \"base64\", \"header\": \"Authorization\"}");
    Assert.assertTrue(MSTAGE_AUTHENTICATION.isValid(state));
  }
}

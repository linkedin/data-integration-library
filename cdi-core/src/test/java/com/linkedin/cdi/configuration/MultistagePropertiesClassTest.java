// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.configuration;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.gobblin.configuration.SourceState;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.cdi.configuration.PropertyCollection.*;

public class MultistagePropertiesClassTest {
  @Test
  public void testBaseClass() {
    SourceState state = new SourceState();
    Assert.assertEquals(MSTAGE_CALL_INTERVAL_MILLIS.getDocUrl(),
        "https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.call.interval.millis.md");
  }

  @Test
  public void testJsonArrayProperties() {
    SourceState state = new SourceState();
    Assert.assertEquals(MSTAGE_DERIVED_FIELDS.get(state), new JsonArray());

    state.setProp("ms.derived.fields", "[0, 1, 2]");
    Assert.assertFalse(MSTAGE_DERIVED_FIELDS.isValid(state));
    Assert.assertEquals(MSTAGE_DERIVED_FIELDS.get(state), new JsonArray());

    state.setProp("ms.derived.fields", "[{}]");
    Assert.assertFalse(MSTAGE_DERIVED_FIELDS.isValid(state));
    Assert.assertEquals(MSTAGE_DERIVED_FIELDS.get(state), new JsonArray());

    state.setProp("ms.derived.fields", "[{\"name\": \"dummy\"}]");
    Assert.assertFalse(MSTAGE_DERIVED_FIELDS.isValid(state));
    Assert.assertFalse(MSTAGE_DERIVED_FIELDS.isValidNonblank(state));
    Assert.assertEquals(MSTAGE_DERIVED_FIELDS.get(state), new JsonArray());

    state.setProp("ms.derived.fields", "[{\"name\": \"dummy\", \"formula\": \"dummy\"}]");
    Assert.assertFalse(MSTAGE_DERIVED_FIELDS.isValid(state));
  }

  @Test
  public void testJsonObjectProperties() {
    SourceState state = new SourceState();
    Assert.assertEquals(MSTAGE_ACTIVATION_PROPERTY.get(state), new JsonObject());

    state.setProp("ms.activation.property", "[0, 1, 2]");
    Assert.assertFalse(MSTAGE_ACTIVATION_PROPERTY.isValid(state));
    Assert.assertEquals(MSTAGE_ACTIVATION_PROPERTY.get(state), new JsonObject());

    state.setProp("ms.activation.property", "{\"name\": \"value\"");
    Assert.assertFalse(MSTAGE_ACTIVATION_PROPERTY.isValid(state));
    Assert.assertEquals(MSTAGE_ACTIVATION_PROPERTY.get(state), new JsonObject());

    state.setProp("ms.activation.property", "{\"name\": \"value\"}");
    Assert.assertTrue(MSTAGE_ACTIVATION_PROPERTY.isValid(state));
  }

  @Test
  public void testIntegerProperties() {
    SourceState state = new SourceState();
    Assert.assertEquals(MSTAGE_ABSTINENT_PERIOD_DAYS.get(state), (Integer) 0);

    state.setProp("ms.abstinent.period.days", "abc");
    Assert.assertFalse(MSTAGE_ABSTINENT_PERIOD_DAYS.isValid(state));
    Assert.assertEquals(MSTAGE_ABSTINENT_PERIOD_DAYS.get(state), (Integer) 0);

    state.setProp("ms.abstinent.period.days", "99");
    Assert.assertTrue(MSTAGE_ABSTINENT_PERIOD_DAYS.isValid(state));
    Assert.assertEquals(MSTAGE_ABSTINENT_PERIOD_DAYS.get(state), (Integer) 99);
  }

}

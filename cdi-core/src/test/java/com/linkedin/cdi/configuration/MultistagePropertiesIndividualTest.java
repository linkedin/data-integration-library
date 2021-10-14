// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.configuration;

import com.google.gson.JsonObject;
import org.apache.gobblin.configuration.SourceState;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.cdi.configuration.PropertyCollection.*;
import static com.linkedin.cdi.configuration.StaticConstants.*;


public class MultistagePropertiesIndividualTest {
  @Test
  public void testMsAuthentication() {
    SourceState state = new SourceState();
    Assert.assertEquals(MSTAGE_AUTHENTICATION.getProp(state), new JsonObject());

    state.setProp("ms.authentication", "[0, 1, 2]");
    Assert.assertFalse(MSTAGE_AUTHENTICATION.isValid(state));
    Assert.assertEquals(MSTAGE_AUTHENTICATION.getProp(state), new JsonObject());

    state.setProp("ms.authentication", "{\"name\": \"value\"");
    Assert.assertFalse(MSTAGE_AUTHENTICATION.isValid(state));
    Assert.assertFalse(MSTAGE_AUTHENTICATION.validateNonblank(state));
    Assert.assertEquals(MSTAGE_AUTHENTICATION.getProp(state), new JsonObject());

    state.setProp("ms.authentication", "{\"method\": \"bearer\"}");
    Assert.assertFalse(MSTAGE_AUTHENTICATION.isValid(state));
    Assert.assertFalse(MSTAGE_AUTHENTICATION.validateNonblank(state));
    Assert.assertEquals(MSTAGE_AUTHENTICATION.getProp(state), new JsonObject());

    state.setProp("ms.authentication", "{\"method\": \"bearer\", \"encryption\": \"base64\", \"header\": \"Authorization\"}");
    Assert.assertTrue(MSTAGE_AUTHENTICATION.isValid(state));
  }

  @Test
  public void testDefaultValues() {
    SourceState state = new SourceState();
    Assert.assertEquals(EXTRACT_IS_FULL.getProp(state), Boolean.FALSE);
    Assert.assertEquals(MSTAGE_BACKFILL.getProp(state), Boolean.FALSE);
    Assert.assertEquals(MSTAGE_CONVERTER_KEEP_NULL_STRINGS.getProp(state), Boolean.FALSE);
    Assert.assertEquals(MSTAGE_CSV_COLUMN_HEADER .getProp(state), Boolean.FALSE);
    Assert.assertEquals(MSTAGE_DATA_EXPLICIT_EOF.getProp(state), Boolean.FALSE);
    Assert.assertEquals(MSTAGE_ENABLE_CLEANSING.getProp(state), Boolean.TRUE);
    Assert.assertEquals(MSTAGE_ENABLE_DYNAMIC_FULL_LOAD.getProp(state), Boolean.TRUE);
    Assert.assertEquals(MSTAGE_ENABLE_SCHEMA_BASED_FILTERING.getProp(state), Boolean.TRUE);
    Assert.assertEquals(MSTAGE_WORK_UNIT_PARTIAL_PARTITION.getProp(state), Boolean.TRUE);
    Assert.assertEquals(STATE_STORE_ENABLED.getProp(state), Boolean.TRUE);
    Assert.assertEquals(MSTAGE_ABSTINENT_PERIOD_DAYS.getProp(state).longValue(), 0L);
    Assert.assertEquals(MSTAGE_S3_LIST_MAX_KEYS.getProp(state).intValue(), 1000);
    Assert.assertEquals(MSTAGE_NORMALIZER_BATCH_SIZE.getProp(state).longValue(), 500L);
    Assert.assertEquals(MSTAGE_WAIT_TIMEOUT_SECONDS.getProp(state).longValue(), 600L);
    Assert.assertEquals(MSTAGE_CSV_ESCAPE_CHARACTER.getProp(state), "u005C");
    Assert.assertEquals(MSTAGE_CSV_QUOTE_CHARACTER.getProp(state), "\"");
    Assert.assertEquals(MSTAGE_CSV_SEPARATOR.getProp(state), KEY_WORD_COMMA);
    Assert.assertEquals(MSTAGE_JDBC_SCHEMA_REFACTOR.getProp(state), "none");
    Assert.assertEquals(MSTAGE_SOURCE_DATA_CHARACTER_SET.getProp(state), "UTF-8");
    Assert.assertEquals(MSTAGE_SOURCE_FILES_PATTERN.getProp(state), REGEXP_DEFAULT_PATTERN);
    Assert.assertEquals(MSTAGE_WORK_UNIT_PARTITION.getProp(state), "none");
  }
}

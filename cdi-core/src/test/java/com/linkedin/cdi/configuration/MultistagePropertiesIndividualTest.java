// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.configuration;

import com.google.gson.JsonObject;
import com.linkedin.cdi.keys.JobKeys;
import org.apache.gobblin.configuration.SourceState;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.cdi.configuration.PropertyCollection.*;
import static com.linkedin.cdi.configuration.StaticConstants.*;


public class MultistagePropertiesIndividualTest {
  @Test
  public void testAllKeys() {
    SourceState state = new SourceState();
    Assert.assertTrue(new JobKeys().validate(state));

    state.setProp("ms.csv.column.header", "xxx");
    Assert.assertFalse(new JobKeys().validate(state));
  }

  @Test
  public void testMsAuthentication() {
    SourceState state = new SourceState();
    Assert.assertEquals(MSTAGE_AUTHENTICATION.get(state), new JsonObject());

    state.setProp("ms.authentication", "[0, 1, 2]");
    Assert.assertFalse(MSTAGE_AUTHENTICATION.isValid(state));
    Assert.assertEquals(MSTAGE_AUTHENTICATION.get(state), new JsonObject());

    state.setProp("ms.authentication", "{\"name\": \"value\"");
    Assert.assertFalse(MSTAGE_AUTHENTICATION.isValid(state));
    Assert.assertFalse(MSTAGE_AUTHENTICATION.isValidNonblank(state));
    Assert.assertEquals(MSTAGE_AUTHENTICATION.get(state), new JsonObject());

    state.setProp("ms.authentication", "{\"method\": \"bearer\"}");
    Assert.assertFalse(MSTAGE_AUTHENTICATION.isValid(state));
    Assert.assertFalse(MSTAGE_AUTHENTICATION.isValidNonblank(state));
    Assert.assertEquals(MSTAGE_AUTHENTICATION.get(state), new JsonObject());

    state.setProp("ms.authentication", "{\"method\": \"bearer\", \"encryption\": \"base64\", \"header\": \"Authorization\"}");
    Assert.assertTrue(MSTAGE_AUTHENTICATION.isValid(state));
  }

  @Test
  public void testDefaultValues() {
    SourceState state = new SourceState();
    Assert.assertEquals(EXTRACT_IS_FULL.get(state), Boolean.FALSE);
    Assert.assertEquals(MSTAGE_BACKFILL.get(state), Boolean.FALSE);
    Assert.assertEquals(MSTAGE_CONVERTER_KEEP_NULL_STRINGS.get(state), Boolean.FALSE);
    Assert.assertEquals(MSTAGE_CSV_COLUMN_HEADER .get(state), Boolean.FALSE);
    Assert.assertEquals(MSTAGE_DATA_EXPLICIT_EOF.get(state), Boolean.FALSE);
    Assert.assertEquals(MSTAGE_ENABLE_CLEANSING.get(state), Boolean.TRUE);
    Assert.assertEquals(MSTAGE_ENABLE_DYNAMIC_FULL_LOAD.get(state), Boolean.TRUE);
    Assert.assertEquals(MSTAGE_ENABLE_SCHEMA_BASED_FILTERING.get(state), Boolean.TRUE);
    Assert.assertEquals(MSTAGE_WORK_UNIT_PARTIAL_PARTITION.get(state), Boolean.TRUE);
    Assert.assertEquals(STATE_STORE_ENABLED.get(state), Boolean.TRUE);
    Assert.assertEquals(MSTAGE_ABSTINENT_PERIOD_DAYS.get(state).longValue(), 0L);
    Assert.assertEquals(MSTAGE_S3_LIST_MAX_KEYS.get(state).intValue(), 1000);
    Assert.assertEquals(MSTAGE_NORMALIZER_BATCH_SIZE.get(state).longValue(), 500L);
    Assert.assertEquals(MSTAGE_WAIT_TIMEOUT_SECONDS.get(state).longValue(), 600L);
    Assert.assertEquals(MSTAGE_CSV_ESCAPE_CHARACTER.get(state), "u005C");
    Assert.assertEquals(MSTAGE_CSV_QUOTE_CHARACTER.get(state), "\"");
    Assert.assertEquals(MSTAGE_CSV_SEPARATOR.get(state), KEY_WORD_COMMA);
    Assert.assertEquals(MSTAGE_JDBC_SCHEMA_REFACTOR.get(state), "none");
    Assert.assertEquals(MSTAGE_SOURCE_DATA_CHARACTER_SET.get(state), "UTF-8");
    Assert.assertEquals(MSTAGE_SOURCE_FILES_PATTERN.get(state), REGEXP_DEFAULT_PATTERN);
    Assert.assertEquals(MSTAGE_WORK_UNIT_PARTITION.get(state), "none");
  }


  @Test
  public void testCsvColumnHeader() {
    SourceState state = new SourceState();
    state.setProp("ms.csv.column.header", "xxx");
    Assert.assertFalse(MSTAGE_CSV_COLUMN_HEADER.isValid(state));

    state.setProp("ms.csv.column.header", "true");
    Assert.assertTrue(MSTAGE_CSV_COLUMN_HEADER.isValid(state));

    state.setProp("ms.csv.column.header", "false");
    Assert.assertTrue(MSTAGE_CSV_COLUMN_HEADER.isValid(state));
  }
}

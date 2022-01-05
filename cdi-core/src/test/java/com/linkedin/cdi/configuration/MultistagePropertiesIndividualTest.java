// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.configuration;

import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
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
    Assert.assertFalse(new JobKeys().validate(state));

    // required parameters
    state.setProp("extract.table.name", "xxx");
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

    // SAML 2.0 is not supported yet
    state.setProp("ms.authentication", "{\"method\": \"saml\", \"encryption\": \"base64\", \"header\": \"Authorization\"}");
    Assert.assertFalse(MSTAGE_AUTHENTICATION.isValid(state));
  }

  @Test
  public void testDefaultValues() {
    SourceState state = new SourceState();
    Assert.assertEquals(EXTRACT_IS_FULL.get(state), Boolean.FALSE);
    Assert.assertEquals(MSTAGE_BACKFILL.get(state), Boolean.FALSE);
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
    Assert.assertEquals(MSTAGE_JDBC_SCHEMA_REFACTOR.get(state), "none");
    Assert.assertEquals(MSTAGE_SOURCE_DATA_CHARACTER_SET.get(state), "UTF-8");
    Assert.assertEquals(MSTAGE_SOURCE_FILES_PATTERN.get(state), REGEXP_DEFAULT_PATTERN);
    Assert.assertEquals(MSTAGE_WORK_UNIT_PARTITION.get(state), "none");
  }


  @Test
  public void testCsv() {
    SourceState state = new SourceState();
    JsonObject csv;

    Assert.assertTrue(MSTAGE_CSV.isValid(state));
    Assert.assertEquals(MSTAGE_CSV.getEscapeCharacter(state), "\\");
    Assert.assertEquals(MSTAGE_CSV.getQuoteCharacter(state), "\"");
    Assert.assertEquals(MSTAGE_CSV.getFieldSeparator(state), ",");
    Assert.assertEquals(MSTAGE_CSV.getRecordSeparator(state), System.lineSeparator());

    csv = new JsonObject();
    csv.addProperty("columnHeaderIndex", -1);
    csv.addProperty("linesToSkip", 0);
    csv.addProperty("escapeCharacter", "u0003");
    csv.addProperty("quoteCharacter", "u0003");
    csv.addProperty("defaultFieldType", "xxx");
    csv.addProperty("fieldSeparator", "u0003");
    csv.addProperty("recordSeparator", "u0003");
    csv.addProperty("columnProjection", "1,2,3");
    csv.addProperty("maxFailures", 1);
    csv.addProperty("keepNullString", true);
    state.setProp("ms.csv", csv.toString());
    Assert.assertTrue(MSTAGE_CSV.isValid(state));
    Assert.assertEquals(MSTAGE_CSV.getEscapeCharacter(state), "\u0003");
    Assert.assertEquals(MSTAGE_CSV.getQuoteCharacter(state), "\u0003");
    Assert.assertEquals(MSTAGE_CSV.getFieldSeparator(state), "\u0003");
    Assert.assertEquals(MSTAGE_CSV.getRecordSeparator(state), "\u0003");

    csv = new JsonObject();
    csv.addProperty("columnHeaderIndex", -1);
    state.setProp("ms.csv", csv.toString());
    Assert.assertTrue(MSTAGE_CSV.isValid(state));

    csv = new JsonObject();
    csv.addProperty("columnHeaderIndex", 0);
    state.setProp("ms.csv", csv.toString());
    Assert.assertTrue(MSTAGE_CSV.isValid(state));

    csv = new JsonObject();
    csv.addProperty("columnHeaderIndex", 0);
    csv.addProperty("linesToSkip", 0);
    state.setProp("ms.csv", csv.toString());
    Assert.assertFalse(MSTAGE_CSV.isValid(state));

    // column projection has to be numbers or ranges of numbers
    csv = new JsonObject();
    csv.addProperty("columnProjection", "x,y,z");
    state.setProp("ms.csv", csv.toString());
    Assert.assertFalse(MSTAGE_CSV.isValid(state));
  }

  @Test
  public void testEncryptionFields() {
    SourceState state = new SourceState();
    Assert.assertTrue(MSTAGE_ENCRYPTION_FIELDS.isValid(state));

    JsonArray fields = new JsonArray();
    fields.add("access_token");
    state.setProp(MSTAGE_ENCRYPTION_FIELDS.getConfig(), fields.toString());

    JsonObject schemaColumn = new JsonObject();
    schemaColumn.addProperty("columnName", "access_token");
    schemaColumn.addProperty("isNullable", "true");
    JsonArray schema = new JsonArray();
    schema.add(schemaColumn);
    state.setProp(MSTAGE_OUTPUT_SCHEMA.getConfig(), schema.toString());

    Assert.assertFalse(MSTAGE_ENCRYPTION_FIELDS.isValid(state));

    schemaColumn = new JsonObject();
    schemaColumn.addProperty("columnName", "access_token");
    schema = new JsonArray();
    schema.add(schemaColumn);
    state.setProp(MSTAGE_OUTPUT_SCHEMA.getConfig(), schema.toString());

    Assert.assertFalse(MSTAGE_ENCRYPTION_FIELDS.isValid(state));

    schemaColumn = new JsonObject();
    schemaColumn.addProperty("columnName", "access_token");
    schemaColumn.addProperty("isNullable", "false");
    schema = new JsonArray();
    schema.add(schemaColumn);
    state.setProp(MSTAGE_OUTPUT_SCHEMA.getConfig(), schema.toString());

    Assert.assertTrue(MSTAGE_ENCRYPTION_FIELDS.isValid(state));


  }


  @Test
  public void testSSL() {
    SourceState state = new SourceState();
    Assert.assertTrue(MSTAGE_SSL.isValid(state));

    JsonObject ssl = new JsonObject();
    ssl.addProperty("keyStoreType", "xxx");
    ssl.addProperty("keyStorePath", "xxx");
    ssl.addProperty("keyStorePassword", "xxx");
    ssl.addProperty("keyPassword", "xxx");
    ssl.addProperty("trustStorePath", "xxx");
    ssl.addProperty("trustStorePassword", "xxx");
    ssl.addProperty("connectionTimeoutSeconds", "1");
    ssl.addProperty("socketTimeoutSeconds", "1");
    ssl.addProperty("version", "xxx");
    state.setProp(MSTAGE_SSL.getConfig(), ssl.toString());
    Assert.assertTrue(MSTAGE_SSL.isValid(state));

    ssl.addProperty("keystorePassword", "xxx");
    state.setProp(MSTAGE_SSL.getConfig(), ssl.toString());
    Assert.assertFalse(MSTAGE_SSL.isValid(state));
  }

  @Test
  public void testWatermark() {
    SourceState state = new SourceState();
    Assert.assertTrue(MSTAGE_WATERMARK.isValid(state));

    // not a JsonArray
    state.setProp("ms.watermark", "string");
    Assert.assertFalse(MSTAGE_WATERMARK.isValid(state));

    // array item is not a JsonObject
    state.setProp("ms.watermark", "[\"string\"]");
    Assert.assertFalse(MSTAGE_WATERMARK.isValid(state));

    // no "name"
    state.setProp("ms.watermark", "[{\"type\": \"datetime\",\"range\": {\"from\": \"2019-01-01\", \"to\": \"-\"}}]");
    Assert.assertFalse(MSTAGE_WATERMARK.isValid(state));

    // unknown type
    state.setProp("ms.watermark", "[{\"name\": \"system\",\"type\": \"unknown\"}]");
    Assert.assertFalse(MSTAGE_WATERMARK.isValid(state));

    // no "range"
    state.setProp("ms.watermark", "[{\"name\": \"system\",\"type\": \"datetime\"}]");
    Assert.assertFalse(MSTAGE_WATERMARK.isValid(state));

    // no "units"
    state.setProp("ms.watermark", "[{\"name\": \"system\",\"type\": \"unit\"}]");
    Assert.assertFalse(MSTAGE_WATERMARK.isValid(state));

    // normal datetime watermark
    state.setProp("ms.watermark", "[{\"name\": \"system\",\"type\": \"datetime\",\"range\": {\"from\": \"2019-01-01\", \"to\": \"-\"}}]");
    Assert.assertTrue(MSTAGE_WATERMARK.isValid(state));
    Assert.assertEquals(MSTAGE_WATERMARK.getRange(state).getRight(), "-");

    // normal datetime watermark and normal unit watermark
    state.setProp("ms.watermark", "[{\"name\": \"system\",\"type\": \"datetime\", \"range\": {\"from\": \"2021-08-21\", \"to\": \"-\"}}, {\"name\": \"bucketId\", \"type\": \"unit\", \"units\": \"null,0,1,2,3,4,5,6,7,8,9\"}]");
    Assert.assertTrue(MSTAGE_WATERMARK.isValid(state));
    Assert.assertEquals(MSTAGE_WATERMARK.getRange(state).getLeft(), "2021-08-21");
    Assert.assertEquals(MSTAGE_WATERMARK.getUnits(state), Lists.newArrayList("null,0,1,2,3,4,5,6,7,8,9".split(",")));

    state.setProp("ms.watermark", "[{\"name\": \"system\",\"type\": \"datetime\", \"range\": {\"from\": \"2020-01-01\", \"to\": \"2020-01-31\"}}]");
    Assert.assertTrue(MSTAGE_WATERMARK.isValid(state));

    state.setProp("ms.watermark", "[{\"name\":\"system\",\"type\":\"datetime\",\"range\":{\"from\":\"2021-01-01\",\"to\":\"P0D\"}}]");
    Assert.assertTrue(MSTAGE_WATERMARK.isValid(state));

    state.setProp("ms.watermark", "[{\"name\": \"table\", \"type\": \"unit\", \"units\": \"ac_audit_log,ac_tables,accounts\"}]");
    Assert.assertTrue(MSTAGE_WATERMARK.isValid(state));

    // P0D0H0M - minute offset not supported yet
    state.setProp("ms.watermark", "[{\"name\":\"system\",\"type\":\"datetime\",\"range\":{\"from\":\"2021-01-01\",\"to\":\"P0D0H0M\"}}]");
    Assert.assertFalse(MSTAGE_WATERMARK.isValid(state));

    // YYYYMMDD format is not supported for now
    state.setProp("ms.watermark", "[{\"name\": \"system\",\"type\": \"datetime\", \"range\": {\"from\": \"20091201\", \"to\": \"-\"}}]");
    Assert.assertFalse(MSTAGE_WATERMARK.isValid(state));

    // P0D is valid
    state.setProp("ms.watermark", "[{\"name\": \"system\",\"type\": \"datetime\", \"range\": {\"from\": \"P0D\", \"to\": \"P0D\"}}]");
    Assert.assertTrue(MSTAGE_WATERMARK.isValid(state));

    // P0DT0H is valid
    state.setProp("ms.watermark", "[{\"name\": \"system\",\"type\": \"datetime\", \"range\": {\"from\": \"P0DT0H\", \"to\": \"P0DT0H\"}}]");
    Assert.assertTrue(MSTAGE_WATERMARK.isValid(state));

    // P0DT0H0M is valid
    state.setProp("ms.watermark", "[{\"name\": \"system\",\"type\": \"datetime\", \"range\": {\"from\": \"P0DT0H0M\", \"to\": \"P0DT0H0M\"}}]");
    Assert.assertTrue(MSTAGE_WATERMARK.isValid(state));

    // P0DT0H0M.UTC is valid
    state.setProp("ms.watermark", "[{\"name\": \"system\",\"type\": \"datetime\", \"range\": {\"from\": \"P0DT0H0M.UTC\", \"to\": \"P0DT0H0M.UTC\"}}]");
    Assert.assertTrue(MSTAGE_WATERMARK.isValid(state));

    // P0DT0H0M.America/Los_Angeles is valid
    state.setProp("ms.watermark", "[{\"name\": \"system\",\"type\": \"datetime\", \"range\": {\"from\": \"P0DT0H0M.America/Los_Angeles\", \"to\": \"P0DT0H0M.America/Los_Angeles\"}}]");
    Assert.assertTrue(MSTAGE_WATERMARK.isValid(state));
  }

  @Test
  public void testWorkUnitParallelismMax() {
    SourceState state = new SourceState();
    Assert.assertTrue(MSTAGE_WORK_UNIT_PARALLELISM_MAX.isValid(state));

    state.setProp("ms.work.unit.parallelism.max", "0");
    Assert.assertTrue(MSTAGE_WORK_UNIT_PARALLELISM_MAX.isValid(state));
    Assert.assertEquals(MSTAGE_WORK_UNIT_PARALLELISM_MAX.get(state).intValue(), 500);

    state.setProp("ms.work.unit.parallelism.max", "0L");
    Assert.assertFalse(MSTAGE_WORK_UNIT_PARALLELISM_MAX.isValid(state));

    state.setProp("ms.work.unit.parallelism.max", "20000");
    Assert.assertFalse(MSTAGE_WORK_UNIT_PARALLELISM_MAX.isValid(state));
  }

  @Test
  public void testSecondaryInput() throws Exception {
    SourceState state = new SourceState();
    Assert.assertTrue(MSTAGE_SECONDARY_INPUT.isValid(state));
    state.setProp("ms.secondary.input", "[{\"path\": \"dummy\", \"fields\": [\"access_token\"], \"category\": \"authentication\",\"retry\": {\"threadpool\": 5}}]");
    Assert.assertEquals((long) MSTAGE_SECONDARY_INPUT.getAuthenticationRetry(state).get("delayInSec"), 300L);
    Assert.assertEquals((long) MSTAGE_SECONDARY_INPUT.getAuthenticationRetry(state).get("retryCount"), 3);
  }

  @Test
  public void testDerivedFieldsProperty() {
    SourceState state = new SourceState();
    state.setProp("ms.derived.fields", "[{\"name\": \"dummy\", \"formula\": {\"type\": \"epoc\", \"source\": \"CURRENTDATE\"}}]");
    Assert.assertTrue(MSTAGE_DERIVED_FIELDS.isValid(state));

    state.setProp("ms.derived.fields", "[{\"name\": \"dummy\", \"formula\": {\"type\": \"epoc\", \"source\": \"CURRENTDATE\", \"timezone\": \"UTC\"}}]");
    Assert.assertTrue(MSTAGE_DERIVED_FIELDS.isValid(state));

    state.setProp("ms.derived.fields", "[{\"name\": \"dummy\", \"formula\": {\"type\": \"epoc\", \"source\": \"CURRENTDATE\", \"timezone\": \"nozone\"}}]");
    Assert.assertFalse(MSTAGE_DERIVED_FIELDS.isValid(state));
  }
}

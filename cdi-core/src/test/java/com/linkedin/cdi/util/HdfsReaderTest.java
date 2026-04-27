// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.util;

import com.google.gson.JsonObject;
import java.util.Collections;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.gobblin.configuration.SourceState;
import org.powermock.reflect.Whitebox;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.cdi.configuration.PropertyCollection.*;


public class HdfsReaderTest {
  private static final String FIELD = "data";
  private static final List<String> FIELDS = Collections.singletonList(FIELD);

  private SourceState state;
  private HdfsReader reader;

  @BeforeMethod
  public void setUp() {
    state = new SourceState();
    reader = new HdfsReader(state);
  }

  @Test
  public void testStringField_whenFlagOn_andValueIsJsonObject_inlinesAsJsonObject() throws Exception {
    state.setProp(MSTAGE_HDFS_READER_PARSE_JSON_STRINGS.getConfig(), "true");
    GenericRecord record = recordWithStringField(FIELD, "{\"key\":\"value\"}");

    JsonObject result = Whitebox.invokeMethod(reader, "selectFieldsFromGenericRecord", record, FIELDS);

    Assert.assertTrue(result.get(FIELD).isJsonObject());
    Assert.assertEquals(result.get(FIELD).getAsJsonObject().get("key").getAsString(), "value");
  }

  @Test
  public void testStringField_whenFlagOn_andValueIsJsonArray_inlinesAsJsonArray() throws Exception {
    state.setProp(MSTAGE_HDFS_READER_PARSE_JSON_STRINGS.getConfig(), "true");
    GenericRecord record = recordWithStringField(FIELD, "[{\"@context\":\"schema.org\"}]");

    JsonObject result = Whitebox.invokeMethod(reader, "selectFieldsFromGenericRecord", record, FIELDS);

    Assert.assertTrue(result.get(FIELD).isJsonArray());
    Assert.assertEquals(
        result.get(FIELD).getAsJsonArray().get(0).getAsJsonObject().get("@context").getAsString(),
        "schema.org");
  }

  @Test
  public void testStringField_whenFlagOn_andValueIsPlainText_keepsAsJsonPrimitive() throws Exception {
    state.setProp(MSTAGE_HDFS_READER_PARSE_JSON_STRINGS.getConfig(), "true");
    GenericRecord record = recordWithStringField(FIELD, "hello world");

    JsonObject result = Whitebox.invokeMethod(reader, "selectFieldsFromGenericRecord", record, FIELDS);

    Assert.assertTrue(result.get(FIELD).isJsonPrimitive());
    Assert.assertEquals(result.get(FIELD).getAsString(), "hello world");
  }

  @Test
  public void testStringField_whenFlagOn_andValueIsMalformedJson_fallsBackToJsonPrimitive() throws Exception {
    state.setProp(MSTAGE_HDFS_READER_PARSE_JSON_STRINGS.getConfig(), "true");
    GenericRecord record = recordWithStringField(FIELD, "[{incomplete]");

    JsonObject result = Whitebox.invokeMethod(reader, "selectFieldsFromGenericRecord", record, FIELDS);

    Assert.assertTrue(result.get(FIELD).isJsonPrimitive());
    Assert.assertEquals(result.get(FIELD).getAsString(), "[{incomplete]");
  }

  @Test
  public void testStringField_whenFlagOff_preservesLegacyBehaviorForJsonContent() throws Exception {
    GenericRecord record = recordWithStringField(FIELD, "{\"key\":\"value\"}");

    JsonObject result = Whitebox.invokeMethod(reader, "selectFieldsFromGenericRecord", record, FIELDS);

    Assert.assertTrue(result.get(FIELD).isJsonPrimitive());
    Assert.assertEquals(result.get(FIELD).getAsString(), "{\"key\":\"value\"}");
  }

  @Test
  public void testStringField_whenFlagOff_preservesLegacyBehaviorForPlainText() throws Exception {
    GenericRecord record = recordWithStringField(FIELD, "hello");

    JsonObject result = Whitebox.invokeMethod(reader, "selectFieldsFromGenericRecord", record, FIELDS);

    Assert.assertEquals(result.get(FIELD).getAsString(), "hello");
  }

  @Test
  public void testStringField_whenFlagOn_andValueIsNull_returnsJsonNull() throws Exception {
    state.setProp(MSTAGE_HDFS_READER_PARSE_JSON_STRINGS.getConfig(), "true");
    GenericRecord record = recordWithStringField(FIELD, null);

    JsonObject result = Whitebox.invokeMethod(reader, "selectFieldsFromGenericRecord", record, FIELDS);

    Assert.assertTrue(result.get(FIELD).isJsonNull());
  }

  @Test
  public void testStringField_whenFlagOn_andValueIsEmptyString_keepsAsJsonPrimitive() throws Exception {
    state.setProp(MSTAGE_HDFS_READER_PARSE_JSON_STRINGS.getConfig(), "true");
    GenericRecord record = recordWithStringField(FIELD, "");

    JsonObject result = Whitebox.invokeMethod(reader, "selectFieldsFromGenericRecord", record, FIELDS);

    Assert.assertTrue(result.get(FIELD).isJsonPrimitive());
    Assert.assertEquals(result.get(FIELD).getAsString(), "");
  }

  @Test
  public void testNullableStringField_whenFlagOn_andValueIsJsonArray_inlinesAsJsonArray() throws Exception {
    state.setProp(MSTAGE_HDFS_READER_PARSE_JSON_STRINGS.getConfig(), "true");
    GenericRecord record = recordWithNullableStringField(FIELD, "[{\"@context\":\"schema.org\"}]");

    JsonObject result = Whitebox.invokeMethod(reader, "selectFieldsFromGenericRecord", record, FIELDS);

    Assert.assertTrue(result.get(FIELD).isJsonArray());
    Assert.assertEquals(
        result.get(FIELD).getAsJsonArray().get(0).getAsJsonObject().get("@context").getAsString(),
        "schema.org");
  }

  private GenericRecord recordWithStringField(String key, String val) {
    Schema schema = SchemaBuilder.record("Test").namespace("com.linkedin.test")
        .doc("Test record").fields()
        .name(key).doc("test").type().stringType()
        .noDefault().endRecord();
    GenericRecord record = new GenericData.Record(schema);
    record.put(key, val);
    return record;
  }

  private GenericRecord recordWithNullableStringField(String key, String val) {
    Schema schema = SchemaBuilder.record("Test").namespace("com.linkedin.test")
        .doc("Test record").fields()
        .name(key).doc("test").type().nullable().stringType()
        .noDefault().endRecord();
    GenericRecord record = new GenericData.Record(schema);
    record.put(key, val);
    return record;
  }
}

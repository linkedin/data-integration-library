// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.filter;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.gobblin.configuration.WorkUnitState;
import com.linkedin.cdi.exception.RetriableAuthenticationException;
import com.linkedin.cdi.keys.AvroExtractorKeys;
import com.linkedin.cdi.util.AvroSchemaUtils;
import com.linkedin.cdi.util.JsonIntermediateSchema;
import org.apache.gobblin.converter.avro.UnsupportedDateTypeException;
import org.apache.gobblin.source.workunit.Extract;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class AvroSchemaBasedFilterTest {
  private GenericRecord inputRecord;
  private Gson GSON;
  private AvroExtractorKeys _avroExtractorKeys;
  private WorkUnitState state;

  @BeforeMethod
  public void setUp() throws RetriableAuthenticationException {
    Schema schema = Schema.createRecord("test", "test", "test", false);
    List<Schema.Field> fieldList = new ArrayList<>();
    fieldList.add(new Schema.Field("id0", Schema.create(Schema.Type.STRING), "id0", null));
    fieldList.add(new Schema.Field("id1", Schema.create(Schema.Type.STRING), "id0", null));
    schema.setFields(fieldList);
    inputRecord = new GenericData.Record(schema);
    inputRecord.put("id0", "0");
    inputRecord.put("id1", "1");

    GSON = new Gson();
    _avroExtractorKeys = new AvroExtractorKeys();
    _avroExtractorKeys.setIsValidOutputSchema(true);
    _avroExtractorKeys.setAvroOutputSchema(schema);

    state = mock(WorkUnitState.class);
    Extract extract = new Extract(Extract.TableType.SNAPSHOT_ONLY, "com.linkedin.test", "test");
    when(state.getExtract()).thenReturn(extract);
  }

  @Test
  public void testFilter() throws UnsupportedDateTypeException {
    // The case where one column is filtered out
    JsonArray rawSchemaArray = GSON.fromJson(
        "[{\"columnName\":\"id0\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}}]", JsonArray.class);
    AvroSchemaBasedFilter avroSchemaBasedFilter =
        new AvroSchemaBasedFilter(new JsonIntermediateSchema(rawSchemaArray), _avroExtractorKeys, state);
    GenericRecord record = avroSchemaBasedFilter.filter(inputRecord);
    // id0 remains
    Assert.assertEquals(record.get("id0"), "0");
    // id1 is filtered out
    Assert.assertFalse(AvroSchemaUtils.getSchemaFieldNames(record.getSchema()).contains("id1"));
    Assert.assertNull(record.get("id1"));

    // The case where output schema contains an extra column not in the original record
    rawSchemaArray = GSON.fromJson(
        "[{\"columnName\":\"id0\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}}, "
            + "{\"columnName\":\"id1\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}}, "
            + "{\"columnName\":\"id2\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}}]", JsonArray.class);
    avroSchemaBasedFilter =
        new AvroSchemaBasedFilter(new JsonIntermediateSchema(rawSchemaArray), _avroExtractorKeys, state);
    record = avroSchemaBasedFilter.filter(inputRecord);
    // id0 remains
    Assert.assertEquals(record.get("id0"), "0");
    // id1 remains
    Assert.assertEquals(record.get("id1"), "1");
    // id2 is padded with null
    Assert.assertTrue(AvroSchemaUtils.getSchemaFieldNames(record.getSchema()).contains("id2"));
    Assert.assertNull(record.get("id2"));
  }
}
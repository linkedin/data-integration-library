// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.dil.extractor;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import com.linkedin.dil.configuration.MultistageProperties;
import com.linkedin.dil.connection.MultistageConnection;
import com.linkedin.dil.exception.RetriableAuthenticationException;
import com.linkedin.dil.keys.AvroExtractorKeys;
import com.linkedin.dil.keys.JobKeys;
import com.linkedin.dil.source.HttpSource;
import com.linkedin.dil.source.MultistageSource;
import com.linkedin.dil.util.JsonUtils;
import com.linkedin.dil.util.WorkUnitStatus;
import org.apache.gobblin.source.workunit.Extract;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.util.AvroUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.mockito.Mockito;
import org.powermock.reflect.Whitebox;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static com.linkedin.dil.configuration.MultistageProperties.*;
import static com.linkedin.dil.configuration.StaticConstants.*;
import static org.mockito.Mockito.*;


@Test
@Slf4j
public class AvroExtractorTest {

  private final static String DATA_SET_URN_KEY = "com.linkedin.somecase.SeriesCollection";
  private final static String ACTIVATION_PROP = "{\"name\": \"survey\", \"type\": \"unit\", \"units\": \"id1,id2\"}";
  private final static String DATA_FINAL_DIR = "/jobs/testUser/gobblin/useCaseRoot";
  private final static String FILE_PERMISSION = "775";
  private final static long ONE_HOUR_IN_MILLS = 3600000L;
  private final static long WORK_UNIT_START_TIME_KEY = 1590994800000L;
  JsonArray outputJsonSchema;
  private WorkUnitState state;
  private SourceState sourceState;
  private MultistageSource multiStageSource;
  private HttpSource httpSource;
  private HttpSource realHttpSource;
  private WorkUnit workUnit;
  private JobKeys jobKeys;
  private AvroExtractor avroExtractor;
  private WorkUnitStatus workUnitStatus;
  private AvroExtractorKeys avroExtractorKeys;
  private MultistageConnection multistageConnection;

  @BeforeMethod
  public void setUp() throws RetriableAuthenticationException {
    state = mock(WorkUnitState.class);
    sourceState = mock(SourceState.class);
    multiStageSource = mock(MultistageSource.class);
    httpSource = mock(HttpSource.class);
    realHttpSource = new HttpSource();

    List<WorkUnit> wus = new MultistageSource().getWorkunits(new SourceState());
    workUnit = wus.get(0);
    workUnit.setProp(MultistageProperties.DATASET_URN_KEY.getConfig(), DATA_SET_URN_KEY);

    jobKeys = mock(JobKeys.class);
    workUnitStatus = mock(WorkUnitStatus.class);

    avroExtractorKeys = mock(AvroExtractorKeys.class);
    when(avroExtractorKeys.getActivationParameters()).thenReturn(new JsonObject());

    outputJsonSchema = new JsonArray();

    // mock for state
    when(state.getWorkunit()).thenReturn(workUnit);
    when(state.getProp(MSTAGE_ACTIVATION_PROPERTY.getConfig(), new JsonObject().toString())).thenReturn(ACTIVATION_PROP);
    when(state.getPropAsLong(MSTAGE_WORKUNIT_STARTTIME_KEY.getConfig(), 0L)).thenReturn(WORK_UNIT_START_TIME_KEY);
    when(state.getProp(DATA_PUBLISHER_FINAL_DIR.getConfig(), StringUtils.EMPTY)).thenReturn(DATA_FINAL_DIR);
    when(state.getProp(MSTAGE_EXTRACTOR_TARGET_FILE_PERMISSION.getConfig(), StringUtils.EMPTY)).thenReturn(FILE_PERMISSION);
    Extract extract = new Extract(Extract.TableType.SNAPSHOT_ONLY, "com.linkedin.test", "test");
    when(state.getExtract()).thenReturn(extract);
    // mock for source state
    when(sourceState.getProp("extract.table.type", "SNAPSHOT_ONLY")).thenReturn("SNAPSHOT_ONLY");
    when(sourceState.contains("source.conn.use.proxy.url")).thenReturn(true);


    // mock for source
    when(multiStageSource.getJobKeys()).thenReturn(jobKeys);

    // mock for source keys
    when(jobKeys.getOutputSchema()).thenReturn(outputJsonSchema);
    when(jobKeys.getDerivedFields()).thenReturn(new HashMap<>());

    avroExtractor = new AvroExtractor(state, multiStageSource.getJobKeys());
    avroExtractor.setAvroExtractorKeys(avroExtractorKeys);
    avroExtractor.jobKeys = jobKeys;

    multistageConnection = Mockito.mock(MultistageConnection.class);
    when(multistageConnection.executeFirst(workUnitStatus)).thenReturn(workUnitStatus);
    when(multistageConnection.executeNext(workUnitStatus)).thenReturn(workUnitStatus);
    avroExtractor.setConnection(multistageConnection);  }

  @BeforeTest
  public void setup() {
    if (System.getProperty("hadoop.home.dir") == null) {
      System.setProperty("hadoop.home.dir", "/tmp");
    }
  }

  /**
   * testing vanilla Avro Extractor with a blank file
   */
  @Test
  public void testExtractAvroWithEmptyFile() throws RetriableAuthenticationException {
    InputStream inputStream = getClass().getResourceAsStream("/avro/empty_file.avro");
    WorkUnitStatus status = WorkUnitStatus.builder().buffer(inputStream).build();

    when(sourceState.getProp("ms.output.schema", "" )).thenReturn("");

    // replace mocked keys with default keys
    realHttpSource.getWorkunits(sourceState);
    avroExtractor.jobKeys = realHttpSource.getJobKeys();
    avroExtractor.setAvroExtractorKeys(new AvroExtractorKeys());

    when(multistageConnection.executeFirst(avroExtractor.workUnitStatus)).thenReturn(status);

    // schema should be a minimum schema with no field
    Schema schema = avroExtractor.getSchema();
    Assert.assertEquals(0, schema.getFields().size());

    // there should be 0 records processed
    GenericRecord rst = avroExtractor.readRecord(null);
    Assert.assertNull(rst);
    while (avroExtractor.hasNext()) {
      avroExtractor.readRecord(null);
    }
    Assert.assertEquals(0, avroExtractor.getAvroExtractorKeys().getProcessedCount());
  }

  private GenericRecord createSingletonRecordWithString(String val) {
    return createSingletonRecordWithString("test", val);
  }

  private GenericRecord createSingletonRecordWithString(String key, String val) {
    Schema schema = SchemaBuilder.record("Test").namespace("com.linkedin.test")
        .doc("Test record").fields()
        .name(key).doc("test").type().stringType()
        .noDefault().endRecord();
    GenericRecord record = new GenericData.Record(schema);
    record.put(key, val);
    return record;
  }

  @Test
  public void testAddDerivedFields() throws Exception {
    avroExtractor.setTimezone("America/Los_Angeles");

    // derived field is in unsupported type and the source is non-existent
    Map<String, Map<String, String>> derivedFields = ImmutableMap.of("formula",
        ImmutableMap.of("type", "non-epoc", "source", "start_time", "format", "yyyy-MM-dd"));
    when(jobKeys.getDerivedFields()).thenReturn(derivedFields);
    GenericRecord row = createSingletonRecordWithString("testVal");
    GenericRecord res = Whitebox.invokeMethod(avroExtractor, "addDerivedFields", row);
    Assert.assertEquals(res.getSchema().getFields().size(), 1);
    Optional<Object> fieldValue = AvroUtils.getFieldValue(res, "test");
    Assert.assertTrue(fieldValue.isPresent());
    Assert.assertEquals(fieldValue.get().toString(), "testVal");

    // derived field is empty early exit
    derivedFields = ImmutableMap.of();
    when(jobKeys.getDerivedFields()).thenReturn(derivedFields);
    row = createSingletonRecordWithString("testVal");
    res = Whitebox.invokeMethod(avroExtractor, "addDerivedFields", row);
    Assert.assertEquals(res.getSchema().getFields().size(), 1);
    fieldValue = AvroUtils.getFieldValue(res, "test");
    Assert.assertTrue(fieldValue.isPresent());
    Assert.assertEquals(fieldValue.get().toString(), "testVal");

    // derived field is currentdate
    derivedFields = ImmutableMap.of("current_date",
        ImmutableMap.of("type", "epoc", "source", "currentdate"));
    when(jobKeys.getDerivedFields()).thenReturn(derivedFields);
    row = createSingletonRecordWithString("testVal");
    res = Whitebox.invokeMethod(avroExtractor, "addDerivedFields", row);
    Assert.assertEquals(res.getSchema().getFields().size(), 2);
    fieldValue = AvroUtils.getFieldValue(res, "test");
    Assert.assertTrue(fieldValue.isPresent());
    Assert.assertEquals(fieldValue.get().toString(), "testVal");
    fieldValue = AvroUtils.getFieldValue(res, "current_date");
    Assert.assertTrue(fieldValue.isPresent());
    Assert.assertTrue(Math.abs((Long)fieldValue.get() - DateTime.now().getMillis()) < ONE_HOUR_IN_MILLS);

    // derived field is P1D
    derivedFields = ImmutableMap.of("current_date",
        ImmutableMap.of("type", "epoc", "source", "P1D"));
    when(jobKeys.getDerivedFields()).thenReturn(derivedFields);
    row = createSingletonRecordWithString("testVal");
    res = Whitebox.invokeMethod(avroExtractor, "addDerivedFields", row);
    Assert.assertEquals(res.getSchema().getFields().size(), 2);
    fieldValue = AvroUtils.getFieldValue(res, "test");
    Assert.assertTrue(fieldValue.isPresent());
    Assert.assertEquals(fieldValue.get().toString(), "testVal");
    DateTimeZone timeZone = DateTimeZone.forID("America/Los_Angeles");
    Period period = Period.parse("P1D");
    long p1d = DateTime.now().withZone(timeZone).minus(period).dayOfMonth().roundFloorCopy().getMillis();
    fieldValue = AvroUtils.getFieldValue(res, "current_date");
    Assert.assertTrue(fieldValue.isPresent());
    Assert.assertTrue(Math.abs((Long)fieldValue.get() - p1d) < ONE_HOUR_IN_MILLS);

    // derived field is in the specified format
    derivedFields = ImmutableMap.of("current_date",
        ImmutableMap.of("type", "epoc", "source", "start_time", "format", "yyyy-MM-dd"));
    when(jobKeys.getDerivedFields()).thenReturn(derivedFields);
    row = createSingletonRecordWithString("start_time", "2020-06-01");
    res = Whitebox.invokeMethod(avroExtractor, "addDerivedFields", row);
    Assert.assertEquals(res.getSchema().getFields().size(), 2);
    fieldValue = AvroUtils.getFieldValue(res, "start_time");
    Assert.assertTrue(fieldValue.isPresent());
    Assert.assertEquals(fieldValue.get().toString(), "2020-06-01");
    DateTimeFormatter datetimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd");
    fieldValue = AvroUtils.getFieldValue(res, "current_date");
    Assert.assertTrue(fieldValue.isPresent());
    Assert.assertEquals((long)fieldValue.get(), datetimeFormatter.parseDateTime("2020-06-01").getMillis());

    // derived field is NOT in the specified format
    derivedFields = ImmutableMap.of("current_date",
        ImmutableMap.of("type", "epoc", "source", "start_time", "format", "yyyy-MM-dd"));
    when(jobKeys.getDerivedFields()).thenReturn(derivedFields);
    row = createSingletonRecordWithString("start_time", "notdatetime");
    res = Whitebox.invokeMethod(avroExtractor, "addDerivedFields", row);
    // Since the type is supported, we created a new record with new columns.
    // In reality, the work unit will fail when processing the derived field's value.
    Assert.assertEquals(res.getSchema().getFields().size(), 2);
    fieldValue = AvroUtils.getFieldValue(res, "start_time");
    Assert.assertTrue(fieldValue.isPresent());
    Assert.assertEquals(fieldValue.get().toString(), "notdatetime");

    // derived field is boolean
    derivedFields = ImmutableMap.of("partial_failure",
        ImmutableMap.of("type", "boolean", "value", "true"));
    when(jobKeys.getDerivedFields()).thenReturn(derivedFields);
    row = createSingletonRecordWithString("testVal");
    res = Whitebox.invokeMethod(avroExtractor, "addDerivedFields", row);
    Assert.assertEquals(res.getSchema().getFields().size(), 2);
    fieldValue = AvroUtils.getFieldValue(res, "test");
    Assert.assertTrue(fieldValue.isPresent());
    Assert.assertEquals(fieldValue.get().toString(), "testVal");
    fieldValue = AvroUtils.getFieldValue(res, "partial_failure");
    Assert.assertTrue(fieldValue.isPresent());
    Assert.assertTrue((boolean) fieldValue.get());

    // derived fields are from variables
    JsonObject parameters = new JsonObject();
    parameters.addProperty("dateString", "2019-11-01 12:00:00");
    parameters.addProperty("someInteger", 123456);
    parameters.addProperty("someNumber", 123.456);
    parameters.addProperty("someEpoc", 1601038688000L);
    parameters.addProperty("someBoolean", true);
    avroExtractor.currentParameters = parameters;

    derivedFields = ImmutableMap.of("dateString",
        ImmutableMap.of("type", "string", "source", "{{dateString}}"),
        "someInteger",
        ImmutableMap.of("type", "integer", "source", "{{someInteger}}"),
        "someEpoc",
        ImmutableMap.of("type", "epoc", "source", "{{someEpoc}}"),
        "someNumber",
        ImmutableMap.of("type", "number", "source", "{{someNumber}}"),
        "someBoolean",
        ImmutableMap.of("type", "boolean", "source", "{{someBoolean}}"));
    when(jobKeys.getDerivedFields()).thenReturn(derivedFields);
    row = createSingletonRecordWithString("start_time", "2020-06-01");
    res = Whitebox.invokeMethod(avroExtractor, "addDerivedFields", row);
    Assert.assertEquals(res.getSchema().getFields().size(), 6);
    fieldValue = AvroUtils.getFieldValue(res, "start_time");
    Assert.assertTrue(fieldValue.isPresent());
    Assert.assertEquals((String)fieldValue.get(), "2020-06-01");
    fieldValue = AvroUtils.getFieldValue(res, "dateString");
    Assert.assertTrue(fieldValue.isPresent());
    Assert.assertEquals((String)fieldValue.get(), "2019-11-01 12:00:00");
    fieldValue = AvroUtils.getFieldValue(res, "someInteger");
    Assert.assertTrue(fieldValue.isPresent());
    Assert.assertEquals((int)fieldValue.get(), 123456);
    fieldValue = AvroUtils.getFieldValue(res, "someNumber");
    Assert.assertTrue(fieldValue.isPresent());
    Assert.assertEquals((double)fieldValue.get(), 123.456);
    fieldValue = AvroUtils.getFieldValue(res, "someEpoc");
    Assert.assertTrue(fieldValue.isPresent());
    Assert.assertEquals((long)fieldValue.get(), 1601038688000L);
    fieldValue = AvroUtils.getFieldValue(res, "someBoolean");
    Assert.assertTrue(fieldValue.isPresent());
    Assert.assertTrue((boolean) fieldValue.get());
  }

  @Test
  public void testGetSchema() throws Exception {
    Schema avroSchema;
    String schemaString = "[{\"columnName\":\"id0\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}}, "
        + "{\"columnName\":\"id1\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}}, "
        + "{\"columnName\":\"id2\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}}]";
    JsonArray schemaArray = GSON.fromJson(schemaString, JsonArray.class);
    JsonArray schema = JsonUtils.deepCopy(schemaArray).getAsJsonArray();
    when(jobKeys.hasOutputSchema()).thenReturn(true);
    when(jobKeys.getOutputSchema()).thenReturn(schema);
    when(jobKeys.getOutputSchema()).thenReturn(schemaArray);
    avroSchema = Whitebox.invokeMethod(avroExtractor, "getSchema");
    Assert.assertEquals(avroSchema.getFields().size(), 3);
    Assert.assertEquals(avroSchema.getName(), "test");
    Assert.assertEquals(avroSchema.getNamespace(), "com.linkedin.test");
  }
}

// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.extractor;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.linkedin.cdi.connection.MultistageConnection;
import com.linkedin.cdi.exception.RetriableAuthenticationException;
import com.linkedin.cdi.keys.AvroExtractorKeys;
import com.linkedin.cdi.keys.JobKeys;
import com.linkedin.cdi.source.HttpSource;
import com.linkedin.cdi.source.MultistageSource;
import com.linkedin.cdi.util.JsonUtils;
import com.linkedin.cdi.util.WorkUnitStatus;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static com.linkedin.cdi.configuration.PropertyCollection.*;
import static com.linkedin.cdi.configuration.StaticConstants.*;
import static org.mockito.Mockito.*;


@Test
public class AvroExtractorTest {
  private static final Logger LOG = LoggerFactory.getLogger(AvroExtractorTest.class);
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

    SourceState tmpState = new SourceState();
    tmpState.setProp("extract.table.name", "xxx");
    List<WorkUnit> wus = new MultistageSource().getWorkunits(tmpState);
    workUnit = wus.get(0);
    workUnit.setProp(DATASET_URN.getConfig(), DATA_SET_URN_KEY);

    jobKeys = mock(JobKeys.class);
    workUnitStatus = mock(WorkUnitStatus.class);

    avroExtractorKeys = mock(AvroExtractorKeys.class);
    when(avroExtractorKeys.getActivationParameters()).thenReturn(new JsonObject());

    outputJsonSchema = new JsonArray();

    // mock for state
    when(state.getWorkunit()).thenReturn(workUnit);
    when(state.getProp(MSTAGE_ACTIVATION_PROPERTY.getConfig(), new JsonObject().toString())).thenReturn(ACTIVATION_PROP);
    when(state.getPropAsLong(MSTAGE_WORK_UNIT_SCHEDULING_STARTTIME.getConfig(), 0L)).thenReturn(WORK_UNIT_START_TIME_KEY);
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
    when(jobKeys.getSessionInitialValue()).thenReturn(java.util.Optional.empty());

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
    when(sourceState.contains("extract.table.name")).thenReturn(true);
    when(sourceState.getProp("extract.table.name")).thenReturn("xxx");
    when(sourceState.getProp("extract.table.name", "")).thenReturn("xxx");

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
    Assert.assertEquals((long)fieldValue.get(), datetimeFormatter.withZone(DateTimeZone.forID("America/Los_Angeles")).parseDateTime("2020-06-01").getMillis());

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

  /**
   * When ms.data.field is an array
   * data = {
   *   "results": [
   *     {
   *       "field1": "a",
   *       "field2": "aa"
   *     },
   *     {
   *       "field1": "b",
   *       "field2": "bb"
   *     },
   *     {
   *       "field1": "c",
   *       "field2": "cc"
   *     }
   *   ]
   * }
   * ms.data.field = "results"
   * @throws Exception exception
   */
  @Test
  public void testMSDataField1() throws Exception {
    InputStream inputStream = getClass().getResourceAsStream("/avro/ArrayFieldTest.avro");
    WorkUnitStatus status = WorkUnitStatus.builder().buffer(inputStream).build();

    when(sourceState.getProp("ms.output.schema", "" )).thenReturn("");
    when(sourceState.contains("extract.table.name")).thenReturn(true);
    when(sourceState.getProp("extract.table.name")).thenReturn("xxx");
    when(sourceState.getProp("extract.table.name", "")).thenReturn("xxx");

    // replace mocked keys with default keys
    realHttpSource.getWorkunits(sourceState);
    avroExtractor.jobKeys = jobKeys;
    avroExtractor.setAvroExtractorKeys(new AvroExtractorKeys());
    when(jobKeys.getDataField()).thenReturn("results");
    when(multistageConnection.executeFirst(avroExtractor.workUnitStatus)).thenReturn(status);

    // schema should be of type record
    Schema schema = avroExtractor.getSchema();
    Assert.assertEquals(schema.getType(), Schema.Type.RECORD);

    // there should be 1 records processed
    GenericRecord rst = avroExtractor.readRecord(null);
    /* expected data = {
     *   "results": [
     *     {
     *       "field1": "a",
     *       "field2": "aa"
     *     },
     *     {
     *       "field1": "b",
     *       "field2": "bb"
     *     },
     *     {
     *       "field1": "c",
     *       "field2": "cc"
     *     }
     *   ]
     * }
     */
    Assert.assertEquals(AvroUtils.getFieldValue(rst, "results.0.field1").get().toString(), "a");
    Assert.assertEquals(AvroUtils.getFieldValue(rst, "results.0.field2").get().toString(), "aa");
    Assert.assertEquals(AvroUtils.getFieldValue(rst, "results.1.field1").get().toString(), "b");
    Assert.assertEquals(AvroUtils.getFieldValue(rst, "results.1.field2").get().toString(), "bb");
    Assert.assertEquals(AvroUtils.getFieldValue(rst, "results.2.field1").get().toString(), "c");
    Assert.assertEquals(AvroUtils.getFieldValue(rst, "results.2.field2").get().toString(), "cc");
    while (avroExtractor.hasNext()) {
      avroExtractor.readRecord(null);
    }
    Assert.assertEquals(1, avroExtractor.getAvroExtractorKeys().getProcessedCount());
  }

  /**
   * When ms.data.field is a single record
   * data = {
   *   "results": {
   *     "field1": "a",
   *     "field2": "aa"
   *   }
   * }
   * ms.data.field = "results"
   * @throws Exception exception
   */
  @Test
  public void testMSDataField2() throws Exception {
    InputStream inputStream = getClass().getResourceAsStream("/avro/SingleRecordArrayFieldTest.avro");
    WorkUnitStatus status = WorkUnitStatus.builder().buffer(inputStream).build();

    when(sourceState.getProp("ms.output.schema", "" )).thenReturn("");
    when(sourceState.contains("extract.table.name")).thenReturn(true);
    when(sourceState.getProp("extract.table.name")).thenReturn("xxx");
    when(sourceState.getProp("extract.table.name", "")).thenReturn("xxx");

    // replace mocked keys with default keys
    realHttpSource.getWorkunits(sourceState);
    avroExtractor.jobKeys = jobKeys;
    avroExtractor.setAvroExtractorKeys(new AvroExtractorKeys());
    when(jobKeys.getDataField()).thenReturn("results");
    when(multistageConnection.executeFirst(avroExtractor.workUnitStatus)).thenReturn(status);

    // schema should be of type record
    Schema schema = avroExtractor.getSchema();
    Assert.assertEquals(schema.getType(), Schema.Type.RECORD);

    // there should be 1 records processed
    GenericRecord rst = avroExtractor.readRecord(null);
    /*
     * expected data = {
     *   "results": {
     *     "field1": "a",
     *     "field2": "aa"
     *   }
     * }
     */
    Assert.assertEquals(AvroUtils.getFieldValue(rst, "results.field1").get().toString(), "a");
    Assert.assertEquals(AvroUtils.getFieldValue(rst, "results.field2").get().toString(), "aa");
    while (avroExtractor.hasNext()) {
      avroExtractor.readRecord(null);
    }
    Assert.assertEquals(1, avroExtractor.getAvroExtractorKeys().getProcessedCount());
  }

  /**
   * When ms.data.field is deep in a nested structure
   * data
   * Record 1 {
   *     "results": [
   *         {
   *             "wrapper": {
   *                 "field1": [
   *                     {
   *                         "field11": "a11",
   *                         "field12": "a12"
   *                     },
   *                     {
   *                         "field11": "aa11",
   *                         "field12": "aa12"
   *                     }
   *                 ]
   *             },
   *             "field2": "aa"
   *         }
   *     ]
   * }
   * Record 2 {
   *     "results": [
   *         {
   *             "wrapper": {
   *                 "field1": [
   *                     {
   *                         "field11": "b11",
   *                         "field12": "b12"
   *                     },
   *                     {
   *                         "field11": "bb11",
   *                         "field12": "bb12"
   *                     }
   *                 ]
   *             },
   *             "field2": "bb"
   *         }
   *     ]
   * }
   * ms.data.field = "results.0.wrapper.field1"
   * @throws Exception exception
   */
  @Test
  public void testMSDataField3() throws Exception {
    InputStream inputStream = getClass().getResourceAsStream("/avro/NestedDataFieldTest.avro");
    WorkUnitStatus status = WorkUnitStatus.builder().buffer(inputStream).build();

    when(sourceState.getProp("ms.output.schema", "" )).thenReturn("");
    when(sourceState.contains("extract.table.name")).thenReturn(true);
    when(sourceState.getProp("extract.table.name")).thenReturn("xxx");
    when(sourceState.getProp("extract.table.name", "")).thenReturn("xxx");

    // replace mocked keys with default keys
    realHttpSource.getWorkunits(sourceState);
    avroExtractor.jobKeys = jobKeys;
    avroExtractor.setAvroExtractorKeys(new AvroExtractorKeys());
    when(jobKeys.getDataField()).thenReturn("results.0.wrapper.field1");
    when(multistageConnection.executeFirst(avroExtractor.workUnitStatus)).thenReturn(status);

    // schema should be of type record
    Schema schema = avroExtractor.getSchema();
    Assert.assertEquals(schema.getType(), Schema.Type.RECORD);

    // there should be 2 records processed
    GenericRecord rst = avroExtractor.readRecord(null);
    /*
     * expected data = {
     *     "field1": [
     *         {
     *             "field11": "a11",
     *             "field12": "a12"
     *         },
     *         {
     *             "field11": "aa11",
     *             "field12": "aa12"
     *         }
     *     ]
     * }
     */
    Assert.assertEquals(AvroUtils.getFieldValue(rst, "field1.0.field11").get().toString(), "a11");
    Assert.assertEquals(AvroUtils.getFieldValue(rst, "field1.0.field12").get().toString(), "a12");
    Assert.assertEquals(AvroUtils.getFieldValue(rst, "field1.1.field11").get().toString(), "aa11");
    Assert.assertEquals(AvroUtils.getFieldValue(rst, "field1.1.field12").get().toString(), "aa12");

    rst = avroExtractor.readRecord(null);
    /*
     * expected data = {
     *     "field1": [
     *         {
     *             "field11": "b11",
     *             "field12": "b12"
     *         },
     *         {
     *             "field11": "bb11",
     *             "field12": "bb12"
     *         }
     *     ]
     * }
     */
    Assert.assertEquals(AvroUtils.getFieldValue(rst, "field1.0.field11").get().toString(), "b11");
    Assert.assertEquals(AvroUtils.getFieldValue(rst, "field1.0.field12").get().toString(), "b12");
    Assert.assertEquals(AvroUtils.getFieldValue(rst, "field1.1.field11").get().toString(), "bb11");
    Assert.assertEquals(AvroUtils.getFieldValue(rst, "field1.1.field12").get().toString(), "bb12");
    while (avroExtractor.hasNext()) {
      avroExtractor.readRecord(null);
    }
    Assert.assertEquals(2, avroExtractor.getAvroExtractorKeys().getProcessedCount());
  }
}

// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.extractor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.linkedin.cdi.connection.MultistageConnection;
import com.linkedin.cdi.exception.RetriableAuthenticationException;
import com.linkedin.cdi.keys.JobKeys;
import com.linkedin.cdi.keys.JsonExtractorKeys;
import com.linkedin.cdi.source.MultistageSource;
import com.linkedin.cdi.util.JsonUtils;
import com.linkedin.cdi.util.ParameterTypes;
import com.linkedin.cdi.util.SchemaBuilder;
import com.linkedin.cdi.util.WorkUnitStatus;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang.StringUtils;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.mockito.Mockito;
import org.powermock.reflect.Whitebox;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.cdi.configuration.PropertyCollection.*;
import static org.mockito.Mockito.*;


@Test
public class JsonExtractorTest {

  // Matches to the total count field in the response json
  private static final int TOTAL_COUNT = 2741497;
  private final static String DATA_SET_URN_KEY = "com.apache.SeriesCollection";
  private final static String ACTIVATION_PROP = "{\"name\": \"survey\", \"type\": \"unit\", \"units\": \"id1,id2\"}";
  private final static long WORKUNIT_STARTTIME_KEY = 1590994800000L;
  private final static long ONE_HOUR_IN_MILLS = 3600000L;

  private Gson gson;
  private JobKeys jobKeys;
  private WorkUnit workUnit;
  private WorkUnitState state;
  private WorkUnitStatus workUnitStatus;
  private MultistageSource source;
  private JsonExtractorKeys jsonExtractorKeys;
  private JsonExtractor jsonExtractor;
  private MultistageConnection multistageConnection;

  @BeforeMethod
  public void setUp() throws RetriableAuthenticationException {
    gson = new Gson();
    source = Mockito.mock(MultistageSource.class);
    jobKeys = Mockito.mock(JobKeys.class);

    SourceState sourceState = new SourceState();
    sourceState.setProp("extract.table.name", "xxx");
    List<WorkUnit> wus = new MultistageSource().getWorkunits(sourceState);
    workUnit = wus.get(0);

    workUnitStatus = Mockito.mock(WorkUnitStatus.class);
    state = Mockito.mock(WorkUnitState.class);
    when(state.getProp(MSTAGE_ACTIVATION_PROPERTY.getConfig(), new JsonObject().toString())).thenReturn(ACTIVATION_PROP);
    when(state.getPropAsLong(MSTAGE_WORK_UNIT_SCHEDULING_STARTTIME.getConfig(), 0L)).thenReturn(WORKUNIT_STARTTIME_KEY);
    when(state.getWorkunit()).thenReturn(workUnit);
    workUnit.setProp(DATASET_URN.getConfig(), DATA_SET_URN_KEY);
    when(source.getJobKeys()).thenReturn(jobKeys);
    when(jobKeys.getPaginationInitValues()).thenReturn(new HashMap<>());
    when(jobKeys.getSessionInitialValue()).thenReturn(Optional.empty());
    when(jobKeys.getSchemaCleansingPattern()).thenReturn("(\\s|\\$|@)");
    when(jobKeys.getSchemaCleansingReplacement()).thenReturn("_");
    when(jobKeys.getSchemaCleansingNullable()).thenReturn(false);
    jsonExtractorKeys = Mockito.mock(JsonExtractorKeys.class);
    jsonExtractor = new JsonExtractor(state, source.getJobKeys());
    jsonExtractor.setJsonExtractorKeys(jsonExtractorKeys);
    jsonExtractor.jobKeys = jobKeys;

    multistageConnection = Mockito.mock(MultistageConnection.class);
    when(multistageConnection.executeFirst(workUnitStatus)).thenReturn(workUnitStatus);
    when(multistageConnection.executeNext(workUnitStatus)).thenReturn(workUnitStatus);
    jsonExtractor.setConnection(multistageConnection);
  }

  @Test
  public void testReadRecord() throws RetriableAuthenticationException {
    when(jobKeys.getTotalCountField()).thenReturn("totalRecords");

    when(jsonExtractorKeys.getActivationParameters()).thenReturn(new JsonObject());
    when(jsonExtractorKeys.getTotalCount()).thenReturn(Long.valueOf(0));
    when(jsonExtractorKeys.getJsonElementIterator()).thenReturn(null);
    when(jsonExtractorKeys.getPayloads()).thenReturn(new JsonArray());
    Assert.assertNull(jsonExtractor.readRecord(new JsonObject()));

    when(jobKeys.getTotalCountField()).thenReturn(StringUtils.EMPTY);
    when(workUnitStatus.getMessages()).thenReturn(ImmutableMap.of("contentType", "application/json"));
    when(multistageConnection.executeNext(jsonExtractor.workUnitStatus)).thenReturn(workUnitStatus);
    InputStream stream = new ByteArrayInputStream("{\"key\":\"value\"}".getBytes());
    when(workUnitStatus.getBuffer()).thenReturn(stream);
    when(jobKeys.getDataField()).thenReturn(StringUtils.EMPTY);
    when(jobKeys.getSessionKeyField()).thenReturn(new JsonObject());
    JsonArray outputSchema = SchemaBuilder.fromJsonData("{\"key\":\"value\"}").buildAltSchema().getAsJsonArray();
    when(jobKeys.getOutputSchema()).thenReturn(outputSchema);
    when(jsonExtractorKeys.getCurrentPageNumber()).thenReturn(Long.valueOf(0));
    when(jsonExtractorKeys.getSessionKeyValue()).thenReturn("session_key");
    workUnit.setProp(DATASET_URN.getConfig(), "com.linkedin.xxxxx.UserGroups");
    Iterator jsonElementIterator = ImmutableList.of().iterator();
    when(jsonExtractorKeys.getJsonElementIterator()).thenReturn(jsonElementIterator);
    when(jsonExtractorKeys.getProcessedCount()).thenReturn(Long.valueOf(0));
    when(jsonExtractorKeys.getTotalCount()).thenReturn(Long.valueOf(20));
    Assert.assertNull(jsonExtractor.readRecord(new JsonObject()));

    JsonObject item = gson.fromJson("{\"key\":\"value\"}", JsonObject.class);
    jsonElementIterator = ImmutableList.of(item).iterator();
    when(jsonExtractorKeys.getJsonElementIterator()).thenReturn(jsonElementIterator);
    when(jsonExtractorKeys.getProcessedCount()).thenReturn(Long.valueOf(10));
    when(jobKeys.getEncryptionField()).thenReturn(null);
    when(jobKeys.isEnableCleansing()).thenReturn(true);
    when(jobKeys.getSchemaCleansingPattern()).thenReturn("(\\s|\\$|@)");
    when(jobKeys.getSchemaCleansingReplacement()).thenReturn("_");
    when(jobKeys.getSchemaCleansingNullable()).thenReturn(false);
    Assert.assertEquals(jsonExtractor.readRecord(new JsonObject()).toString(), "{\"key\":\"value\"}");

    jsonElementIterator = ImmutableList.of().iterator();
    when(jsonExtractorKeys.getJsonElementIterator()).thenReturn(jsonElementIterator);
    when(jsonExtractorKeys.getProcessedCount()).thenReturn(Long.valueOf(10));
    when(jsonExtractorKeys.getTotalCount()).thenReturn(Long.valueOf(20));
    Assert.assertNull(jsonExtractor.readRecord(new JsonObject()));

    when(jsonExtractorKeys.getTotalCount()).thenReturn(Long.valueOf(10));
    when(jobKeys.isPaginationEnabled()).thenReturn(true);
    when(jobKeys.isSessionStateEnabled()).thenReturn(false);
    Assert.assertNull(jsonExtractor.readRecord(new JsonObject()));

    when(jobKeys.isPaginationEnabled()).thenReturn(false);
    Assert.assertNull(jsonExtractor.readRecord(new JsonObject()));

    when(jobKeys.isPaginationEnabled()).thenReturn(true);
    when(jobKeys.isSessionStateEnabled()).thenReturn(true);
    when(jobKeys.getSessionStateCondition()).thenReturn("success|ready");
    when(jsonExtractorKeys.getSessionKeyValue()).thenReturn("success");
    Assert.assertNull(jsonExtractor.readRecord(new JsonObject()));
  }

  @Test
  public void testProcessInputStream() throws RetriableAuthenticationException {
    // replaced mock'ed work unit status with default work unit status
    jsonExtractor.workUnitStatus = WorkUnitStatus.builder().build();

    when(jsonExtractorKeys.getActivationParameters()).thenReturn(new JsonObject());
    when(jsonExtractorKeys.getPayloads()).thenReturn(new JsonArray());
    when(jobKeys.getTotalCountField()).thenReturn(StringUtils.EMPTY);
    Assert.assertFalse(jsonExtractor.processInputStream(10));

    JsonElement item = new JsonObject();
    Iterator<JsonElement> jsonElementIterator = ImmutableList.of(item).iterator();
    when(jsonExtractorKeys.getJsonElementIterator()).thenReturn(jsonElementIterator);
    when(multistageConnection.executeNext(jsonExtractor.workUnitStatus)).thenReturn(null);
    Assert.assertFalse(jsonExtractor.processInputStream(0));

    when(workUnitStatus.getMessages()).thenReturn(ImmutableMap.of("contentType", "multipart/form-data"));
    when(multistageConnection.executeNext(jsonExtractor.workUnitStatus)).thenReturn(workUnitStatus);
    Assert.assertFalse(jsonExtractor.processInputStream(0));

    when(workUnitStatus.getMessages()).thenReturn(null);
    when(jobKeys.hasSourceSchema()).thenReturn(true);
    Assert.assertFalse(jsonExtractor.processInputStream(0));

    when(jobKeys.hasSourceSchema()).thenReturn(false);
    when(jobKeys.hasOutputSchema()).thenReturn(true);
    Assert.assertFalse(jsonExtractor.processInputStream(0));
  }

  @Test
  public void testProcessInputStream2() {
    jsonExtractor.setJsonExtractorKeys(new JsonExtractorKeys());
    jsonExtractor.setJobKeys(new JobKeys());

    when(workUnitStatus.getMessages()).thenReturn(null);
    when(workUnitStatus.getBuffer()).thenReturn(null);
    Assert.assertFalse(jsonExtractor.processInputStream(0));

    InputStream stream = new ByteArrayInputStream("null".getBytes());
    when(workUnitStatus.getBuffer()).thenReturn(stream);
    Assert.assertFalse(jsonExtractor.processInputStream(0));

    stream = new ByteArrayInputStream("primitive_string".getBytes());
    when(workUnitStatus.getBuffer()).thenReturn(stream);
    Assert.assertFalse(jsonExtractor.processInputStream(0));
  }

   @Test
  public void testGetElementByJsonPathWithEdgeCases() {
    JsonObject row = new JsonObject();
    String jsonPath = StringUtils.EMPTY;
    Assert.assertEquals(JsonUtils.get(row, jsonPath), JsonNull.INSTANCE);

    jsonPath = "key";
    Assert.assertEquals(JsonUtils.get(null, jsonPath), JsonNull.INSTANCE);

    row = gson.fromJson("{\"key\":\"some_primitive_value\"}", JsonObject.class);
    jsonPath = "key.1";
    Assert.assertEquals(JsonUtils.get(row, jsonPath), JsonNull.INSTANCE);

    row = gson.fromJson("{\"key\":[\"some_primitive_value\"]}", JsonObject.class);
    jsonPath = "key.a";
    Assert.assertEquals(JsonUtils.get(row, jsonPath), JsonNull.INSTANCE);

    jsonPath = "key.3";
    Assert.assertEquals(JsonUtils.get(row, jsonPath), JsonNull.INSTANCE);
  }

  @Test
  public void testAddDerivedFields() throws Exception {
    Map<String, Map<String, String>> derivedFields = ImmutableMap.of("formula",
        ImmutableMap.of("type", "non-epoc", "source", "start_time", "format", "yyyy-MM-dd"));
    when(jobKeys.getDerivedFields()).thenReturn(derivedFields);
    JsonObject row = new JsonObject();
    JsonObject pushDowns = new JsonObject();
    JsonObject actual;
    when(jsonExtractorKeys.getPushDowns()).thenReturn(pushDowns);
    Assert.assertEquals(Whitebox.invokeMethod(jsonExtractor, "addDerivedFields", row).toString(), "{}");

    derivedFields = ImmutableMap.of("formula",
        ImmutableMap.of("type", "epoc", "source", "currentdate"));
    pushDowns.addProperty("non-formula", "testValue");
    row.addProperty("start_time", "2020-06-01");
    when(jsonExtractorKeys.getPushDowns()).thenReturn(pushDowns);
    when(jobKeys.getDerivedFields()).thenReturn(derivedFields);
    actual = Whitebox.invokeMethod(jsonExtractor, "addDerivedFields", row);
    Assert.assertEquals(actual.entrySet().size(), 2);
    Assert.assertTrue(actual.has("formula"));
    Assert.assertTrue(
        Math.abs(Long.parseLong(actual.get("formula").toString()) - DateTime.now().getMillis())
            < ONE_HOUR_IN_MILLS);

    derivedFields = ImmutableMap.of("formula",
        ImmutableMap.of("type", "epoc", "source", "start_time", "format", "yyyy-MM-dd"));
    pushDowns.addProperty("non-formula", "testValue");
    row.addProperty("start_time", "2020-06-01");
    when(jsonExtractorKeys.getPushDowns()).thenReturn(pushDowns);
    when(jobKeys.getDerivedFields()).thenReturn(derivedFields);
    actual = Whitebox.invokeMethod(jsonExtractor, "addDerivedFields", row);
    Assert.assertEquals(actual.entrySet().size(), 2);
    Assert.assertTrue(actual.has("formula"));
    Assert.assertEquals(actual.get("start_time").toString(), "\"2020-06-01\"");

    derivedFields = ImmutableMap.of("formula",
        ImmutableMap.of("type", "string", "source", "P0D", "format", "yyyy-MM-dd"));
    when(jobKeys.getDerivedFields()).thenReturn(derivedFields);
    pushDowns.addProperty("non-formula", "testValue");
    Assert.assertEquals(
        Whitebox.invokeMethod(jsonExtractor, "addDerivedFields", row).toString(),
        "{\"start_time\":\"2020-06-01\",\"formula\":\"\"}");

    derivedFields = ImmutableMap.of("formula",
        ImmutableMap.of("type", "epoc", "source", "P0D", "format", "yyyy-MM-dd"));
    when(jobKeys.getDerivedFields()).thenReturn(derivedFields);
    pushDowns.addProperty("non-formula", "testValue");
    actual = Whitebox.invokeMethod(jsonExtractor, "addDerivedFields", row);
    Assert.assertEquals(actual.entrySet().size(), 2);
    Assert.assertTrue(actual.has("formula"));
    Assert.assertEquals(actual.get("start_time").toString(), "\"2020-06-01\"");

    derivedFields = ImmutableMap.of("formula",
        ImmutableMap.of("type", "epoc", "source", "start_time", "format", "yyyy-MM-dd"));
    when(jobKeys.getDerivedFields()).thenReturn(derivedFields);
    pushDowns.addProperty("non-formula", "testValue");
    row.addProperty("start_time", "1592809200000");
    actual = Whitebox.invokeMethod(jsonExtractor, "addDerivedFields", row);
    Assert.assertEquals(actual.entrySet().size(), 2);
    Assert.assertTrue(actual.has("formula"));
    Assert.assertEquals(actual.get("start_time").toString(), "\"1592809200000\"");

    // negative regex case
    derivedFields = ImmutableMap.of("formula",
        ImmutableMap.of("type", "regexp", "source", "uri", "format", "/syncs/([0-9]+)$"));
    when(jobKeys.getDerivedFields()).thenReturn(derivedFields);
    pushDowns.addProperty("non-formula", "testValue");
    row.addProperty("uri", "invalid_uri");
    Assert.assertEquals(
        Whitebox.invokeMethod(jsonExtractor, "addDerivedFields", row).toString(),
        "{\"start_time\":\"1592809200000\",\"formula\":\"no match\",\"uri\":\"invalid_uri\"}");
    actual = Whitebox.invokeMethod(jsonExtractor, "addDerivedFields", row);
    Assert.assertEquals(actual.entrySet().size(), 3);
    Assert.assertEquals(actual.get("start_time").toString(), "\"1592809200000\"");
    Assert.assertEquals(actual.get("formula").toString(), "\"no match\"");
    Assert.assertEquals(actual.get("uri").toString(), "\"invalid_uri\"");

    // positive regex case
    derivedFields = ImmutableMap.of("formula",
        ImmutableMap.of("type", "regexp", "source", "uri", "format", "/syncs/([0-9]+)$"));
    when(jobKeys.getDerivedFields()).thenReturn(derivedFields);
    pushDowns.addProperty("formula", "/syncs/1234");
    row.addProperty("uri", "invalid_uri");
    Assert.assertEquals(
        Whitebox.invokeMethod(jsonExtractor, "addDerivedFields", row).toString(),
        "{\"start_time\":\"1592809200000\",\"formula\":\"1234\",\"uri\":\"invalid_uri\"}");
    actual = Whitebox.invokeMethod(jsonExtractor, "addDerivedFields", row);
    Assert.assertEquals(actual.entrySet().size(), 3);
    Assert.assertEquals(actual.get("start_time").toString(), "\"1592809200000\"");
    Assert.assertEquals(actual.get("formula").getAsString(), "1234");
    Assert.assertEquals(actual.get("uri").toString(), "\"invalid_uri\"");
    pushDowns.remove("formula");

    derivedFields = ImmutableMap.of("formula",
        ImmutableMap.of("type", "boolean", "value", "true"));
    when(jobKeys.getDerivedFields()).thenReturn(derivedFields);
    pushDowns.addProperty("non-formula", "testValue");
    actual = Whitebox.invokeMethod(jsonExtractor, "addDerivedFields", row);
    Assert.assertEquals(actual.entrySet().size(), 3);
    Assert.assertTrue(actual.has("formula"));
    Assert.assertEquals(actual.get("formula").toString(), "true");

    // Testing derived fields from push downs that are two levels deep
    derivedFields = ImmutableMap.of("formula",
        ImmutableMap.of("type", "string", "source", "result.key1"));
    when(jobKeys.getDerivedFields()).thenReturn(derivedFields);
    pushDowns.addProperty("nested", "result.key1");
    row.add("result", gson.fromJson("{\"key1\": \"value1\"}", JsonObject.class));
    when(jsonExtractorKeys.getPushDowns()).thenReturn(pushDowns);
    actual = Whitebox.invokeMethod(jsonExtractor, "addDerivedFields", row);
    Assert.assertEquals(actual.entrySet().size(), 4);
    Assert.assertTrue(actual.has("formula"));
    Assert.assertEquals(actual.get("formula").toString(), "\"value1\"");

    // Testing derived fields from variable
    JsonObject parameters = new JsonObject();
    parameters.addProperty("dateString", "2019-11-01");
    parameters.addProperty("dateTimeString", "2019-11-01 12:00:00");
    parameters.addProperty("someInteger", 123456);
    parameters.addProperty("someNumber", 123.456);
    parameters.addProperty("someEpoc", 1601038688000L);
    jsonExtractor.currentParameters = parameters;

    derivedFields = ImmutableMap.of("dateString",
        ImmutableMap.of("type", "epoc", "source", "{{dateString}}", "format", "yyyy-MM-dd"),
        "dateTimeString",
        ImmutableMap.of("type", "string", "source", "{{dateTimeString}}"),
        "someInteger",
        ImmutableMap.of("type", "int", "source", "{{someInteger}}"),
        "someEpoc",
        ImmutableMap.of("type", "epoc", "source", "{{someEpoc}}"),
        "someNumber",
        ImmutableMap.of("type", "number", "source", "{{someNumber}}"));
    when(jobKeys.getDerivedFields()).thenReturn(derivedFields);
    pushDowns.addProperty("non-formula", "testValue");
    actual = Whitebox.invokeMethod(jsonExtractor, "addDerivedFields", row);
    Assert.assertEquals(actual.entrySet().size(), 9);
    DateTimeFormatter datetimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd");
    DateTime dateTime = datetimeFormatter.withZone(DateTimeZone.forID("America/Los_Angeles")).parseDateTime("2019-11-01");
    Assert.assertEquals(actual.get("dateString").toString(), String.valueOf(dateTime.getMillis()));
    Assert.assertEquals(actual.get("dateTimeString").toString(), "\"2019-11-01 12:00:00\"");
    Assert.assertEquals(actual.get("someInteger").toString(), "123456");
    Assert.assertEquals(actual.get("someNumber").toString(), "123.456");
    Assert.assertEquals(actual.get("start_time").toString(), "\"1592809200000\"");
  }

  @Test
  public void testGetNextPaginationValues() throws Exception {
    Map<ParameterTypes, String> paginationKeys = ImmutableMap.of(
        ParameterTypes.PAGESTART, "page_start",
        ParameterTypes.PAGESIZE, "page_size",
        ParameterTypes.PAGENO, "page_number");

    when(jobKeys.getPaginationFields()).thenReturn(paginationKeys);
    JsonElement input = gson.fromJson("{\"page_start\":0, \"page_size\":100, \"page_number\":1}", JsonObject.class);
    HashMap<ParameterTypes, Long> paginationValues = Whitebox.invokeMethod(jsonExtractor, "getNextPaginationValues", input);
    Assert.assertEquals(paginationValues.size(), 3);
    Assert.assertEquals(paginationValues.get(ParameterTypes.PAGESIZE), Long.valueOf(100));
    Assert.assertEquals(paginationValues.get(ParameterTypes.PAGESTART), Long.valueOf(100));

    input = gson.fromJson("{\"page_size\":100, \"page_number\":1}", JsonObject.class);
    paginationValues = Whitebox.invokeMethod(jsonExtractor, "getNextPaginationValues", input);
    Assert.assertEquals(paginationValues.size(), 1);
    Assert.assertEquals(paginationValues.get(ParameterTypes.PAGENO), Long.valueOf(2));

    input = gson.fromJson("{\"page_start\":0, \"page_number\":1}", JsonObject.class);
    paginationValues = Whitebox.invokeMethod(jsonExtractor, "getNextPaginationValues", input);
    Assert.assertEquals(paginationValues.size(), 1);
    Assert.assertEquals(paginationValues.get(ParameterTypes.PAGENO), Long.valueOf(2));

    input = gson.fromJson("{\"page_number\":1}", JsonObject.class);
    paginationValues = Whitebox.invokeMethod(jsonExtractor, "getNextPaginationValues", input);
    Assert.assertEquals(paginationValues.size(), 1);
    Assert.assertEquals(paginationValues.get(ParameterTypes.PAGENO), Long.valueOf(2));

    gson.fromJson("{\"page_start\":null, \"page_size\":100, \"page_number\":1}", JsonObject.class);
    paginationValues = Whitebox.invokeMethod(jsonExtractor, "getNextPaginationValues", input);
    Assert.assertEquals(paginationValues.size(), 1);
    Assert.assertEquals(paginationValues.get(ParameterTypes.PAGENO), Long.valueOf(2));

    gson.fromJson("{\"page_start\":0, \"page_size\":null, \"page_number\":1}", JsonObject.class);
    paginationValues = Whitebox.invokeMethod(jsonExtractor, "getNextPaginationValues", input);
    Assert.assertEquals(paginationValues.size(), 1);
    Assert.assertEquals(paginationValues.get(ParameterTypes.PAGENO), Long.valueOf(2));

    input = gson.fromJson("test_primitive_value", JsonPrimitive.class);
    paginationValues = Whitebox.invokeMethod(jsonExtractor, "getNextPaginationValues", input);
    Assert.assertEquals(paginationValues.size(), 0);
  }

  @Test
  public void testRetrieveSessionKeyValue() throws Exception {
    JsonObject sessionKeyField = gson.fromJson("{\"name\": \"hasMore\", \"condition\": {\"regexp\": \"false|False\"}}", JsonObject.class);
    when(jobKeys.getSessionKeyField()).thenReturn(sessionKeyField);
    JsonElement input = gson.fromJson("[{\"notMore\": \"dummy\"}]", JsonArray.class);
    Assert.assertEquals(
        Whitebox.invokeMethod(jsonExtractor, "retrieveSessionKeyValue", input), JsonNull.INSTANCE.toString());

    input = gson.fromJson("[{\"hasMore\": \"dummy\"}]", JsonArray.class);
    Assert.assertEquals(
        Whitebox.invokeMethod(jsonExtractor, "retrieveSessionKeyValue", input), "dummy");

    input = gson.fromJson("{\"notMore\": \"someValue\"}", JsonObject.class);
    Assert.assertEquals(
        Whitebox.invokeMethod(jsonExtractor, "retrieveSessionKeyValue", input), StringUtils.EMPTY);
  }

  /**
   * Test getTotalCountValue with jsonObject data field
   */
  @Test
  public void testGetTotalCountValueWithJsonObjectDataField() throws Exception {
    when(source.getJobKeys().getTotalCountField()).thenReturn("");
    when(source.getJobKeys().getDataField()).thenReturn("items");
    JsonObject data = gson.fromJson("{\"records\":{\"totalRecords\":2},\"items\":{\"callId\":\"001\"}}", JsonObject.class);
    Assert.assertEquals(Whitebox.invokeMethod(jsonExtractor, "getTotalCountValue", data), Long.valueOf(1));
  }

  /**
   * Test getTotalCountValue with invalid data field
   * Expect: RuntimeException
   */
  @Test(expectedExceptions = RuntimeException.class)
  public void testGetTotalCountValueWithInvalidDataField() throws Exception {
    when(source.getJobKeys().getTotalCountField()).thenReturn("");
    when(source.getJobKeys().getDataField()).thenReturn("items.callId");
    JsonObject data = gson.fromJson("{\"records\":{\"totalRecords\":2},\"items\":{\"callId\":\"001\"}}", JsonObject.class);
    Assert.assertEquals(Whitebox.invokeMethod(jsonExtractor, "getTotalCountValue", data), Long.valueOf(0));
  }

  @Test
  public void testLimitedCleanse() throws Exception {
    JsonElement input;
    input = gson.fromJson("{\"key\": \"value\"}", JsonObject.class);
    Assert.assertEquals(Whitebox.invokeMethod(jsonExtractor, "limitedCleanse", input).toString()
        , input.toString());

    input = gson.fromJson("[{\"key\": \"value\"}]", JsonArray.class);
    Assert.assertEquals(Whitebox.invokeMethod(jsonExtractor, "limitedCleanse", input).toString()
        , input.toString());

    input = gson.fromJson("test_primitive_value", JsonPrimitive.class);
    Assert.assertEquals(Whitebox.invokeMethod(jsonExtractor, "limitedCleanse", input).toString()
        , input.toString());
  }

  /**
   * Test the timeout scenario: session timeout and condition is not met
   * @throws Exception
   */
  @Test (expectedExceptions = {RuntimeException.class})
  public void testWaitingBySessionKeyWithTimeout() throws Exception {
    when(jobKeys.isSessionStateEnabled()).thenReturn(false);
    Assert.assertTrue(Whitebox.invokeMethod(jsonExtractor, "waitingBySessionKeyWithTimeout"));

    when(jobKeys.isSessionStateEnabled()).thenReturn(true);
    when(jobKeys.getSessionStateCondition()).thenReturn("success|ready");
    when(jsonExtractorKeys.getSessionKeyValue()).thenReturn("failed");
    long secondsBeforeCurrentTime = DateTime.now().minus(3000).getMillis();
    long timeout = 2000;
    when(jsonExtractorKeys.getStartTime()).thenReturn(secondsBeforeCurrentTime);
    when(source.getJobKeys().getSessionTimeout()).thenReturn(timeout);
    Whitebox.invokeMethod(jsonExtractor, "waitingBySessionKeyWithTimeout");
  }

  /**
   * Test the in-session scenario: session condition not met, but not timeout, therefore no exception
   * @throws Exception
   */
  @Test
  public void testWaitingBySessionKeyWithTimeout2() throws Exception {
    when(jobKeys.isSessionStateEnabled()).thenReturn(false);
    Assert.assertTrue(Whitebox.invokeMethod(jsonExtractor, "waitingBySessionKeyWithTimeout"));

    when(jobKeys.isSessionStateEnabled()).thenReturn(true);
    when(jobKeys.getSessionStateCondition()).thenReturn("success|ready");
    when(jobKeys.getSessionStateFailCondition()).thenReturn(StringUtils.EMPTY);
    when(jsonExtractorKeys.getSessionKeyValue()).thenReturn("failed");
    long secondsBeforeCurrentTime = DateTime.now().minus(3000).getMillis();
    long timeout = 4000;
    when(jsonExtractorKeys.getStartTime()).thenReturn(secondsBeforeCurrentTime);
    when(jobKeys.getSessionTimeout()).thenReturn(timeout);
    Assert.assertFalse(Whitebox.invokeMethod(jsonExtractor, "waitingBySessionKeyWithTimeout"));
  }

  /**
   * Test the in-session scenario: Exception when session failCondition met
   * @throws Exception
   */
  @Test (expectedExceptions = {RuntimeException.class})
  public void testWaitingBySessionKeyWithTimeoutWhenFailConditionIsMet() throws Exception {
    when(jobKeys.isSessionStateEnabled()).thenReturn(true);
    when(jobKeys.getSessionStateCondition()).thenReturn("success|ready");
    when(jobKeys.getSessionStateFailCondition()).thenReturn("failed");
    when(jsonExtractorKeys.getSessionKeyValue()).thenReturn("failed");

    Whitebox.invokeMethod(jsonExtractor, "waitingBySessionKeyWithTimeout");
  }

  @Test
  public void testIsSessionStateMatch() throws Exception {
    when(jobKeys.isSessionStateEnabled()).thenReturn(false);
    Assert.assertFalse(Whitebox.invokeMethod(jsonExtractor, "isSessionStateMatch"));

    when(jobKeys.isSessionStateEnabled()).thenReturn(true);
    when(jobKeys.getSessionStateCondition()).thenReturn("success|ready");
    when(jsonExtractorKeys.getSessionKeyValue()).thenReturn("success");
    Assert.assertTrue(Whitebox.invokeMethod(jsonExtractor, "isSessionStateMatch"));

    when(jsonExtractorKeys.getSessionKeyValue()).thenReturn("failed");
    Assert.assertFalse(Whitebox.invokeMethod(jsonExtractor, "isSessionStateMatch"));
  }

  @Test
  public void testRetrievePushDowns() throws Exception {
    Map<String, Map<String, String>> derivedFields = new HashMap<>();
    JsonElement response = null;
    Assert.assertEquals(
        Whitebox.invokeMethod(jsonExtractor, "retrievePushDowns", response, derivedFields),
        new JsonObject());

    response = JsonNull.INSTANCE;
    Assert.assertEquals(
        Whitebox.invokeMethod(jsonExtractor, "retrievePushDowns", response, derivedFields),
        new JsonObject());

    response = new JsonArray();
    Assert.assertEquals(
        Whitebox.invokeMethod(jsonExtractor, "retrievePushDowns", response, derivedFields),
        new JsonObject());
  }

  @Test
  public void testExtractJson() throws Exception {
    InputStream input = null;
    Assert.assertNull(Whitebox.invokeMethod(jsonExtractor, "extractJson", input));
  }
}

// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.extractor;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.linkedin.cdi.filter.JsonSchemaBasedFilter;
import com.linkedin.cdi.keys.ExtractorKeys;
import com.linkedin.cdi.keys.JobKeys;
import com.linkedin.cdi.preprocessor.GpgDecryptProcessor;
import com.linkedin.cdi.preprocessor.GunzipProcessor;
import com.linkedin.cdi.preprocessor.InputStreamProcessor;
import com.linkedin.cdi.source.HttpSource;
import com.linkedin.cdi.source.MultistageSource;
import com.linkedin.cdi.util.ParameterTypes;
import com.linkedin.cdi.util.WorkUnitStatus;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.source.extractor.WatermarkInterval;
import org.apache.gobblin.source.extractor.extract.LongWatermark;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.joda.time.DateTime;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.cdi.configuration.MultistageProperties.*;
import static org.mockito.Mockito.*;


@PrepareForTest({Thread.class, IOUtils.class})
public class MultistageExtractorTest extends PowerMockTestCase {
  private Gson gson;
  private ExtractorKeys extractorKeys;
  private MultistageExtractor multistageExtractor;
  private MultistageSource source;
  private WorkUnitState state;
  private WorkUnitStatus workUnitStatus;
  private JobKeys jobKeys;
  private JsonArray jsonSchema;
  private JsonArray outputSchema;

  @BeforeMethod
  public void setUp() {
    gson = new Gson();
    extractorKeys = Mockito.mock(ExtractorKeys.class);
    state = mock(WorkUnitState.class);
    workUnitStatus = Mockito.mock(WorkUnitStatus.class);
    source = mock(MultistageSource.class);
    jobKeys = Mockito.mock(JobKeys.class);
    jsonSchema = new JsonArray();
    outputSchema = new JsonArray();
    multistageExtractor = new MultistageExtractor(state, source.getJobKeys());
    multistageExtractor.extractorKeys = extractorKeys;
    multistageExtractor.jobKeys = jobKeys;
  }

  @Test
  public void testInitialization() {
    WorkUnitState state = mock(WorkUnitState.class);
    when(state.getProp("ms.derived.fields", new JsonArray().toString())).thenReturn("[{\"name\": \"activityDate\", \"formula\": {\"type\": \"epoc\", \"source\": \"fromDateTime\", \"format\": \"yyyy-MM-dd'T'HH:mm:ss'Z'\"}}]");
    when(state.getProp("ms.output.schema", new JsonArray().toString())).thenReturn("");
    when(state.getProp("ms.activation.property", new JsonObject().toString())).thenReturn("{\"a\":\"x\"}");

    SourceState sourceState = mock(SourceState.class);
    when(sourceState.contains("source.conn.use.proxy.url")).thenReturn(true);
    when(sourceState.getProp(MSTAGE_OUTPUT_SCHEMA.getConfig(), "")).thenReturn("");
    MultistageSource source = new HttpSource();
    source.getWorkunits(sourceState);

    MultistageExtractor extractor = new MultistageExtractor(state, source.getJobKeys());
    Assert.assertNotNull(source.getJobKeys().getDerivedFields());
  }

  @Test
  public void testJobProperties() {
    WorkUnitState state = mock(WorkUnitState.class);
    when(state.getProp("ms.derived.fields", new JsonArray().toString())).thenReturn("[{\"name\": \"activityDate\", \"formula\": {\"type\": \"epoc\", \"source\": \"fromDateTime\", \"format\": \"yyyy-MM-dd'T'HH:mm:ss'Z'\"}}]");
    when(state.getProp("ms.output.schema", new JsonArray().toString())).thenReturn("");

    SourceState sourceState = mock(SourceState.class);

    when(state.getProp("ms.activation.property", new JsonObject().toString())).thenReturn("{\"a\":\"x\"}");
    Assert.assertNotNull(MSTAGE_ACTIVATION_PROPERTY.getProp(state));
    Assert.assertNotNull(MSTAGE_ACTIVATION_PROPERTY.getValidNonblankWithDefault(state));
    Assert.assertTrue(MSTAGE_ACTIVATION_PROPERTY.validate(state));
    Assert.assertTrue(MSTAGE_ACTIVATION_PROPERTY.validateNonblank(state));

    when(state.getProp("ms.activation.property", new JsonObject().toString())).thenReturn("{\"a\"}");
    Assert.assertFalse(MSTAGE_ACTIVATION_PROPERTY.validate(state));
    Assert.assertFalse(MSTAGE_ACTIVATION_PROPERTY.validateNonblank(state));
    Assert.assertNotNull(MSTAGE_ACTIVATION_PROPERTY.getValidNonblankWithDefault(state));

    when(state.getProp("ms.activation.property", new JsonObject().toString())).thenReturn("{}");
    Assert.assertTrue(MSTAGE_ACTIVATION_PROPERTY.validate(state));
    Assert.assertFalse(MSTAGE_ACTIVATION_PROPERTY.validateNonblank(state));
    Assert.assertNotNull(MSTAGE_ACTIVATION_PROPERTY.getValidNonblankWithDefault(state));

    when(state.getProp("ms.activation.property", new JsonObject().toString())).thenReturn("");
    Assert.assertTrue(MSTAGE_ACTIVATION_PROPERTY.validate(state));
    Assert.assertFalse(MSTAGE_ACTIVATION_PROPERTY.validateNonblank(state));
    Assert.assertNotNull(MSTAGE_ACTIVATION_PROPERTY.getValidNonblankWithDefault(state));
  }


  @Test
  public void testWorkUnitWatermark(){
    SourceState state = mock(SourceState.class);
    when(state.getProp(MSTAGE_OUTPUT_SCHEMA.getConfig(), "")).thenReturn("");
    MultistageSource source = new MultistageSource();
    List<WorkUnit> workUnits = source.getWorkunits(state);
    WorkUnitState workUnitState = new WorkUnitState(workUnits.get(0));
    JsonExtractor extractor = new JsonExtractor(workUnitState, source.getJobKeys());

    // low watermark by default is 2017-01-01
    Assert.assertEquals("1546329600000", extractor.getWorkUnitWaterMarks().get("low").getAsString());
  }

  @Test
  public void testGetOnePreprocessor() {
    WorkUnitState state = mock(WorkUnitState.class);
    when(state.getProp("ms.derived.fields", new JsonArray().toString())).thenReturn(
        "[]");
    when(state.getProp("ms.output.schema", new JsonArray().toString())).thenReturn("");
    when(state.getProp("ms.activation.property", new JsonObject().toString())).thenReturn(
        "{\"a\":\"x\"}");
    when(state.getProp("ms.extract.preprocessor.parameters", new JsonObject().toString())).thenReturn(
        "{\"com.linkedin.cdi.preprocessor.GpgProcessor\":" +
            "{\"keystore_path\" :\"some path\", \"keystore_password\" : \"some password\"}}");
    when(state.getProp("ms.extract.preprocessors", new String())).thenReturn(
        "com.linkedin.cdi.preprocessor.GpgProcessor");

    SourceState sourceState = mock(SourceState.class);
    when(sourceState.contains("source.conn.use.proxy.url")).thenReturn(true);
    when(sourceState.getProp(MSTAGE_OUTPUT_SCHEMA.getConfig(), "")).thenReturn("");
    MultistageSource source = new HttpSource();
    source.getWorkunits(sourceState);

    MultistageExtractor extractor = new MultistageExtractor(state, source.getJobKeys());

    List<InputStreamProcessor> res = extractor.getPreprocessors(state);
    Assert.assertEquals(res.size(), 1);
    Assert.assertTrue(res.get(0) instanceof GpgDecryptProcessor);
  }

  @Test
  public void testGetTwoPreprocessors() {
    WorkUnitState state = mock(WorkUnitState.class);
    when(state.getProp("ms.derived.fields", new JsonArray().toString())).thenReturn(
        "[]");
    when(state.getProp("ms.output.schema", new JsonArray().toString())).thenReturn("");
    when(state.getProp("ms.activation.property", new JsonObject().toString())).thenReturn(
        "{\"a\":\"x\"}");
    when(state.getProp("ms.extract.preprocessor.parameters", new JsonObject().toString())).thenReturn(
        "{\"com.linkedin.cdi.preprocessor.GpgProcessor\":" +
            "{\"keystore_path\" :\"some path\", \"keystore_password\" : \"some password\"}}");
    when(state.getProp("ms.extract.preprocessors", new String())).thenReturn(
        "com.linkedin.cdi.preprocessor.GpgProcessor,"+
            "com.linkedin.cdi.preprocessor.GunzipProcessor");

    SourceState sourceState = mock(SourceState.class);
    when(sourceState.contains("source.conn.use.proxy.url")).thenReturn(true);
    when(sourceState.getProp(MSTAGE_OUTPUT_SCHEMA.getConfig(), "")).thenReturn("");
    MultistageSource source = new HttpSource();
    source.getWorkunits(sourceState);

    MultistageExtractor extractor = new MultistageExtractor(state, source.getJobKeys());

    List<InputStreamProcessor> res = extractor.getPreprocessors(state);
    Assert.assertEquals(res.size(), 2);
    Assert.assertTrue(res.get(0) instanceof GpgDecryptProcessor);
    Assert.assertTrue(res.get(1) instanceof GunzipProcessor);
  }

  @Test
  public void testGetSchema() {
    Assert.assertNull(multistageExtractor.getSchema());
  }

  @Test
  public void testGetExpectedRecordCount() {
    Assert.assertEquals(multistageExtractor.getExpectedRecordCount(), 0);
  }

  @Test
  public void testGetHighWatermark() {
    Assert.assertEquals(multistageExtractor.getHighWatermark(), 0);
  }

  @Test
  public void testReadRecord() {
    Assert.assertNull(multistageExtractor.readRecord(null));
  }

  @Test
  public void testClose() {
    WorkUnit dummyWU = WorkUnit.create(null, new WatermarkInterval(new LongWatermark(-1L), new LongWatermark(-1L)));
    when(state.getWorkingState()).thenReturn(WorkUnitState.WorkingState.CANCELLED);
    when(state.getWorkunit()).thenReturn(dummyWU);
    multistageExtractor.close();
  }

  @Test
  public void testProcessInputStream() {
    MultistageSource source = new MultistageSource();
    List<WorkUnit> wus = source.getWorkunits(new SourceState());
    WorkUnitState state = new WorkUnitState(wus.get(0), new JobState());
    multistageExtractor = new MultistageExtractor(state, source.getJobKeys());
    multistageExtractor.initialize(new ExtractorKeys());
    Assert.assertFalse(multistageExtractor.processInputStream(100L));
  }

  @Test
  public void testSetRowFilter() {
    JsonSchemaBasedFilter filter = Mockito.mock(JsonSchemaBasedFilter.class);
    JsonArray schema = new JsonArray();
    multistageExtractor.rowFilter = filter;
    multistageExtractor.setRowFilter(schema);

    multistageExtractor.rowFilter = null;
    when(state.getProp(MSTAGE_ENABLE_SCHEMA_BASED_FILTERING.getConfig(), StringUtils.EMPTY)).thenReturn("false");
    multistageExtractor.setRowFilter(new JsonArray());
    Assert.assertNull(multistageExtractor.rowFilter);
  }

  @Test
  public void testGetOrInferSchema() {
    MultistageSource source = new MultistageSource();
    List<WorkUnit> wus = source.getWorkunits(new SourceState());
    WorkUnitState state = new WorkUnitState(wus.get(0), new JobState());
    multistageExtractor = new MultistageExtractor(state, source.getJobKeys());
    multistageExtractor.initialize(new ExtractorKeys());

    JsonObject schema = new JsonObject();
    schema.addProperty("testAttribute", "something");

    JsonArray schemaArray = new JsonArray();
    Map<String, String> defaultFieldTypes = new HashMap<>();

    Assert.assertEquals(multistageExtractor.getOrInferSchema(), schemaArray);

    ExtractorKeys extractorKeys = Mockito.mock(ExtractorKeys.class);
    JsonArray inferredSchema = new JsonArray();
    JsonObject schemaObj = new JsonObject();
    schemaObj.addProperty("type", "null");
    multistageExtractor.extractorKeys = extractorKeys;
    when(extractorKeys.getInferredSchema()).thenReturn(inferredSchema);
    when(extractorKeys.getActivationParameters()).thenReturn(schemaObj);
    when(extractorKeys.getPayloads()).thenReturn(new JsonArray());
    when(jobKeys.hasSourceSchema()).thenReturn(false);
    Assert.assertEquals(multistageExtractor.getOrInferSchema(), schemaArray);

    when(jobKeys.hasSourceSchema()).thenReturn(true);
    Assert.assertEquals(multistageExtractor.getOrInferSchema(), schemaArray);
  }

  @Test
  public void testHoldExecutionUnitPresetStartTime() throws Exception {
    multistageExtractor.extractorKeys = extractorKeys;
    //current time + 3 s
    Long currentSeconds = DateTime.now().plusSeconds(3).getMillis();
    when(extractorKeys.getDelayStartTime()).thenReturn(currentSeconds);

    PowerMockito.mockStatic(Thread.class);
    PowerMockito.doNothing().when(Thread.class);
    Thread.sleep(100L);
    multistageExtractor.holdExecutionUnitPresetStartTime();

    when(extractorKeys.getDelayStartTime()).thenReturn(DateTime.now().plusSeconds(3).getMillis());
    PowerMockito.doThrow(new InterruptedException()).when(Thread.class);
    Thread.sleep(100L);
    multistageExtractor.holdExecutionUnitPresetStartTime();
  }

  @Test
  public void testsFailWorkUnit() {
    state = new WorkUnitState();
    WorkUnitState stateSpy = spy(state);
    multistageExtractor.state = stateSpy;
    multistageExtractor.failWorkUnit(StringUtils.EMPTY);
    verify(stateSpy).setWorkingState(WorkUnitState.WorkingState.FAILED);
    multistageExtractor.failWorkUnit("NON_EMPTY_ERROR_STRING");
  }

  @Test
  public void testDeriveEpoc() {
    String format = "yyyy-MM-dd";
    String strValue = "2020-06-20";
    Assert.assertNotEquals(multistageExtractor.deriveEpoc(format, strValue), StringUtils.EMPTY);

    strValue = "2018-07-14Txsdfs";
    Assert.assertNotEquals(multistageExtractor.deriveEpoc(format, strValue), StringUtils.EMPTY);

    format = "yyyy-MM-dd'T'HH:mm:ssZ";
    strValue = "2018/07/14T14:31:30+0530";
    Assert.assertEquals(multistageExtractor.deriveEpoc(format, strValue), StringUtils.EMPTY);
  }

  @Test
  public void testsAddDerivedFieldsToAltSchema() {
    Map<String, String> items = ImmutableMap.of("type", "some_type", "source", "token.full_token");
    Map<String, Map<String, String>> derivedFields = ImmutableMap.of("formula", items);
    JsonArray outputSchema = gson.fromJson("[{\"token.full_token\": {\"type\":\"string\"}}]", JsonArray.class);
    when(source.getJobKeys()).thenReturn(jobKeys);
    when(jobKeys.getOutputSchema()).thenReturn(outputSchema);
    when(jobKeys.getDerivedFields()).thenReturn(derivedFields);
    Assert.assertEquals(multistageExtractor.addDerivedFieldsToAltSchema().toString(),
        "[{\"columnName\":\"formula\",\"dataType\":{\"type\":\"string\"}}]");
  }

  @Test
  public void testExtractText() throws Exception {
    Assert.assertEquals(multistageExtractor.extractText(null), StringUtils.EMPTY);

    String expected = "test_string";
    InputStream input = new ByteArrayInputStream(expected.getBytes());
    when(state.getProp(MSTAGE_SOURCE_DATA_CHARACTER_SET.getConfig(), StringUtils.EMPTY)).thenReturn("UTF-8");
    Assert.assertEquals(multistageExtractor.extractText(input), expected);

    PowerMockito.mockStatic(IOUtils.class);
    PowerMockito.doThrow(new IOException()).when(IOUtils.class, "toString", input, Charset.forName("UTF-8"));
    multistageExtractor.extractText(input);
    Assert.assertEquals(multistageExtractor.extractText(null), StringUtils.EMPTY);
  }

  @Test
  public void testCheckContentType() {
    String expectedContentType = "application/json";
    Map<String, String> messages = new HashMap<>();
    when(workUnitStatus.getMessages()).thenReturn(messages);
    Assert.assertTrue(multistageExtractor.checkContentType(workUnitStatus, expectedContentType));

    messages.put("contentType", expectedContentType);
    when(workUnitStatus.getMessages()).thenReturn(messages);
    Assert.assertTrue(multistageExtractor.checkContentType(workUnitStatus, expectedContentType));

    messages.put("contentType", "non-expected-contentType");
    when(workUnitStatus.getMessages()).thenReturn(messages);
    Assert.assertFalse(multistageExtractor.checkContentType(workUnitStatus, expectedContentType));

    when(workUnitStatus.getMessages()).thenReturn(null);
    Assert.assertTrue(multistageExtractor.checkContentType(workUnitStatus, expectedContentType));
    HashSet<String> expectedContentTypeSet = new LinkedHashSet<>(
        Arrays.asList("text/csv", "application/gzip", "application/json")
    );
    messages.clear();
    when(workUnitStatus.getMessages()).thenReturn(messages);
    Assert.assertTrue(multistageExtractor.checkContentType(workUnitStatus, expectedContentTypeSet));

    messages.put("contentType", expectedContentType);
    when(workUnitStatus.getMessages()).thenReturn(messages);
    Assert.assertTrue(multistageExtractor.checkContentType(workUnitStatus, expectedContentTypeSet));

    messages.put("contentType", "non-expected-contentType");
    when(workUnitStatus.getMessages()).thenReturn(messages);
    Assert.assertFalse(multistageExtractor.checkContentType(workUnitStatus, expectedContentTypeSet));

    when(workUnitStatus.getMessages()).thenReturn(null);
    Assert.assertTrue(multistageExtractor.checkContentType(workUnitStatus, expectedContentTypeSet));
  }

  /**
   * test getting session key value when the value is in the headers
   */
  @Test
  public void testGetSessionKeyValue() {
    String headers = "{\"cursor\": \"123\"}";
    Map<String, String> messages = new HashMap<>();
    messages.put("headers", headers);
    when(workUnitStatus.getMessages()).thenReturn(messages);

    JsonObject sessionKeyField = gson.fromJson("{\"name\": \"cursor\"}", JsonObject.class);
    when(source.getJobKeys()).thenReturn(jobKeys);
    when(jobKeys.getSessionKeyField()).thenReturn(sessionKeyField);

    Assert.assertEquals(multistageExtractor.getSessionKey(workUnitStatus), "123");
  }

  @Test
  public void testMinimumSchema() {
    WorkUnitState state = new WorkUnitState();
    state.setProp(ConfigurationKeys.EXTRACT_PRIMARY_KEY_FIELDS_KEY, "id");
    state.setProp(ConfigurationKeys.EXTRACT_DELTA_FIELDS_KEY, "date");
    MultistageSource<JsonArray, JsonObject> source = new MultistageSource<>();
    MultistageExtractor<JsonArray, JsonObject> extractor = new MultistageExtractor<>(state, source.getJobKeys());
    JsonArray schema = extractor.createMinimumSchema();
    String expected = "[{\"columnName\":\"id\",\"isNullable\":true,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"date\",\"isNullable\":true,\"dataType\":{\"type\":\"timestamp\"}}]";
    Assert.assertEquals(schema.toString(), expected);
  }

  @Test
  public void testMinimumSchemaEmpty() {
    WorkUnitState state = new WorkUnitState();
    state.setProp(ConfigurationKeys.EXTRACT_PRIMARY_KEY_FIELDS_KEY, "");
    state.setProp(ConfigurationKeys.EXTRACT_DELTA_FIELDS_KEY, "date");
    MultistageSource<JsonArray, JsonObject> source = new MultistageSource<>();
    MultistageExtractor<JsonArray, JsonObject> extractor = new MultistageExtractor<>(state, source.getJobKeys());
    JsonArray schema = extractor.createMinimumSchema();
    String expected = "[{\"columnName\":\"date\",\"isNullable\":true,\"dataType\":{\"type\":\"timestamp\"}}]";
    Assert.assertEquals(schema.toString(), expected);
  }
  /**
   * ReplaceVariablesInParameters() replace placeholders with their real values. This process
   * is called substitution.
   *
   * When the substituted parameter starts with tmp, the parameter is removed from the final.
   *
   * @throws Exception
   */
  @Test
  public void testReplaceVariablesInParameters() throws Exception {
    WorkUnitState state = new WorkUnitState();
    MultistageSource<JsonArray, JsonObject> source = new MultistageSource<>();
    MultistageExtractor<JsonArray, JsonObject> extractor = new MultistageExtractor<>(state, source.getJobKeys());

    JsonObject parameters = gson.fromJson("{\"param1\":\"value1\"}", JsonObject.class);
    JsonObject replaced = extractor.replaceVariablesInParameters(parameters);
    Assert.assertEquals(replaced, parameters);

    parameters = gson.fromJson("{\"param1\":\"value1\",\"param2\":\"{{param1}}\"}", JsonObject.class);
    JsonObject parameters2Expected = gson.fromJson("{\"param1\":\"value1\",\"param2\":\"value1\"}", JsonObject.class);
    replaced = extractor.replaceVariablesInParameters(parameters);
    Assert.assertEquals(replaced, parameters2Expected);

    parameters = gson.fromJson("{\"tmpParam1\":\"value1\",\"param2\":\"{{tmpParam1}}\"}", JsonObject.class);
    parameters2Expected = gson.fromJson("{\"param2\":\"value1\"}", JsonObject.class);
    replaced = extractor.replaceVariablesInParameters(parameters);
    Assert.assertEquals(replaced, parameters2Expected);
  }

  @Test
  public void testAppendActivationParameter() throws Exception {
    MultistageExtractor extractor = Mockito.mock(MultistageExtractor.class);
    ExtractorKeys extractorKeys = Mockito.mock(ExtractorKeys.class);
    extractor.extractorKeys =  extractorKeys;

    JsonObject obj = gson.fromJson("{\"survey\": \"id1\"}", JsonObject.class);
    when(extractorKeys.getActivationParameters()).thenReturn(obj);

    Method method = MultistageExtractor.class.getDeclaredMethod("appendActivationParameter", JsonObject.class);
    method.setAccessible(true);

    Assert.assertEquals(method.invoke(extractor, obj), obj);
  }

  @Test
  public void testGetUpdatedWorkUnitVariableValues() throws Exception {
    MultistageExtractor extractor = Mockito.mock(MultistageExtractor.class);
    WorkUnitStatus wuStatus = Mockito.mock(WorkUnitStatus.class);

    when(extractor.getWorkUnitStatus()).thenReturn(wuStatus);
    when(wuStatus.getPageSize()).thenReturn(100L);
    when(wuStatus.getPageNumber()).thenReturn(5L);
    when(wuStatus.getPageStart()).thenReturn(1L);
    when(wuStatus.getSessionKey()).thenReturn("test_session_key");

    JsonObject jsonObject = new JsonObject();
    jsonObject.addProperty(ParameterTypes.SESSION.toString(), "{\"name\": \"status\"}");
    jsonObject.addProperty(ParameterTypes.PAGESTART.toString(), 1);
    jsonObject.addProperty(ParameterTypes.PAGESIZE.toString(), 100);
    jsonObject.addProperty(ParameterTypes.PAGENO.toString(), 5);

    Method method = MultistageExtractor.class.getDeclaredMethod("getUpdatedWorkUnitVariableValues", JsonObject.class);
    method.setAccessible(true);

    Assert.assertEquals(method.invoke(extractor, jsonObject).toString(),
        "{\"session\":\"test_session_key\",\"pagestart\":1,\"pagesize\":100,\"pageno\":5}");

    when(wuStatus.getPageSize()).thenReturn(-1L);
    Assert.assertEquals(method.invoke(extractor, jsonObject).toString(),
        "{\"pagesize\":100,\"session\":\"test_session_key\",\"pagestart\":1,\"pageno\":5}");
  }

  @Test
  public void testGetInitialWorkUnitVariableValues() throws Exception {
    MultistageExtractor extractor = Mockito.mock(MultistageExtractor.class);
    Method method = MultistageExtractor.class.getDeclaredMethod("getInitialWorkUnitVariableValues");
    method.setAccessible(true);

    JobKeys jobKeys = Mockito.mock(JobKeys.class);
    extractor.jobKeys = jobKeys;
    JsonObject waterMarkObj = gson.fromJson("{\"watermark\":{\"low\":-100,\"high\":1564642800}}", JsonObject.class);
    when(extractor.getWorkUnitWaterMarks()).thenReturn(waterMarkObj);
    when(jobKeys.getPaginationInitValues()).thenReturn(ImmutableMap.of(ParameterTypes.PAGESIZE, 10L));
    Assert.assertEquals(method.invoke(extractor).toString(),
        "{\"watermark\":{\"watermark\":{\"low\":-100,\"high\":1564642800}},\"pagesize\":10}");
  }


}
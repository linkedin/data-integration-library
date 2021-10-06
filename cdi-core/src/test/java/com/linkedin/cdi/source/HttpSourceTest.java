// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.source;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.linkedin.cdi.extractor.JsonExtractor;
import com.linkedin.cdi.helpers.GobblinMultiStageTestHelpers;
import com.linkedin.cdi.keys.HttpKeys;
import com.linkedin.cdi.keys.JobKeys;
import com.linkedin.cdi.util.EncryptionUtils;
import com.linkedin.cdi.util.ParameterTypes;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.runtime.embedded.EmbeddedGobblin;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.cdi.configuration.MultistageProperties.*;
import static com.linkedin.cdi.source.HttpSource.*;
import static org.mockito.Mockito.*;


@PrepareForTest({EncryptionUtils.class})
public class HttpSourceTest extends PowerMockTestCase {
  private static final Logger LOG = LoggerFactory.getLogger(HttpSourceTest.class);
  private Gson gson;
  private WorkUnitState state;
  private HttpSource source;
  private JobKeys jobKeys;
  private SourceState sourceState;
  private String token;
  private JsonObject pagination;
  private JsonObject sessionKeyField;
  private String totalCountField;
  private JsonArray parameters;
  private JsonArray encryptionFields;
  private String dataField;
  private Long callInterval;
  private Long waitTimeoutSeconds;
  private Boolean enableCleansing;
  private Boolean workUnitPartialPartition;
  private JsonArray watermark;
  private JsonArray secondaryInput;
  private String httpClientFactory;
  private JsonObject httpRequestHeaders;
  private String sourceUri;
  private String httpRequestMethod;
  private String extractorClass;
  private JsonObject authentication;
  private JsonObject httpStatus;
  private JsonObject httpStatusReasons;

  @BeforeMethod
  public void setUp() {
    gson = new Gson();
    state = Mockito.mock(WorkUnitState.class);
    jobKeys = Mockito.mock(JobKeys.class);
    sourceState = Mockito.mock(SourceState.class);
    source = new HttpSource();
  }

  @Test(enabled = false)
  public void testAuthentication() {
    HttpSource source = new HttpSource();

    SourceState state = mock(SourceState.class);
    when(state.getProp("ms.watermark", "")).thenReturn("[{\"name\": \"system\",\"type\": \"datetime\", \"range\": {\"from\": \"2017-01-01\", \"to\": \"-\"}}]");
    when(state.getProp("extract.table.type", "SNAPSHOT_ONLY")).thenReturn("SNAPSHOT_ONLY");
    when(state.getProp("extract.namespace", "")).thenReturn("test");
    when(state.getProp("extract.table.name", "")).thenReturn("table1");
    when(state.getProp("source.conn.username", "")).thenReturn("X7CWBD5V4T6DR77WY23YSHACH55K2OXA");
    when(state.getProp("source.conn.password", "")).thenReturn("");
    when(state.getProp("ms.source.uri", "")).thenReturn("https://host/v2/users");
    when(state.getProp("ms.authentication", new JsonObject().toString())).thenReturn("{\"method\":\"basic\",\"encryption\":\"base64\", \"header\": \"Authorization\"}");
    when(state.getProp("ms.http.request.headers", new JsonObject().toString())).thenReturn("{\"Content-Type\": \"application/json\"}");
    when(state.getProp("ms.http.request.method", "")).thenReturn("GET");
    when(state.getProp("ms.session.key.field", new JsonObject().toString())).thenReturn("{\"name\": \"records.cursor\"}");
    when(state.getProp("ms.parameters", new JsonArray().toString())).thenReturn("[{\"name\":\"cursor\",\"type\":\"session\"}]");
    when(state.getProp("ms.data.field", "")).thenReturn("users");
    when(state.getProp("ms.total.count.field", "")).thenReturn("records.totalRecords");
    when(state.getProp("ms.work.unit.partition", "")).thenReturn("");
    when(state.getProp("ms.pagination", new JsonObject().toString())).thenReturn("{}");

    List<WorkUnit> workUnits = source.getWorkunits(state);

    Assert.assertFalse(source.getJobKeys().isPaginationEnabled());
    Assert.assertNotNull(source.getJobKeys());
    Assert.assertNotNull(source.getHttpSourceKeys());
    Assert.assertNotNull(source.getJobKeys().getSourceParameters());
    Assert.assertTrue(workUnits.size() == 1);
    Assert.assertEquals(source.getHttpSourceKeys().getHttpRequestHeaders().toString(), "{\"Content-Type\":\"application/json\"}");

    WorkUnitState unitState = new WorkUnitState(workUnits.get(0));

    JsonExtractor extractor = new JsonExtractor(unitState, source.getHttpSourceKeys());

    JsonObject record = extractor.readRecord(new JsonObject());

    // should return 14 columns
    Assert.assertEquals(14, record.entrySet().size());
    Assert.assertTrue(extractor.getWorkUnitStatus().getTotalCount() > 0);
    Assert.assertTrue(extractor.getWorkUnitStatus().getSessionKey().length() > 0);
  }

  /*
   * basic test with no watermark created.
   */
  @Test(enabled=false)
  public void getWorkUnitsTestEmpty() {
    HttpSource source = new HttpSource();
    List<WorkUnit> workUnits = source.getWorkunits(GobblinMultiStageTestHelpers.prepareSourceStateWithoutWaterMark());
    Assert.assertTrue(workUnits.size() == 1);
    Assert.assertEquals(workUnits.get(0).getLowWatermark().getAsJsonObject().get("value").toString(), "-1");
    Assert.assertEquals(workUnits.get(0).getExpectedHighWatermark().getAsJsonObject().get("value").toString(), "-1");
  }

  /*
   * basic test with watermark.
   */
  @Test(enabled=false)
  public void getWorkUnitsTest() {
    HttpSource source = new HttpSource();
    List<WorkUnit> workUnits = source.getWorkunits(GobblinMultiStageTestHelpers.prepareSourceStateWithWaterMark());
    Assert.assertTrue(workUnits.size() == 1);

    //time stamps below corresponds to the date given in watermark fields in test data.
    Assert.assertEquals(GobblinMultiStageTestHelpers
            .getDateFromTimeStamp(
                Long.parseLong(workUnits.get(0).getLowWatermark().getAsJsonObject().get("value").toString())),
        "2019-08-01");
    Assert.assertEquals(GobblinMultiStageTestHelpers
            .getDateFromTimeStamp(
                Long.parseLong(workUnits.get(0).getExpectedHighWatermark().getAsJsonObject().get("value").toString())),
        "2019-08-02");
  }

  /*
   * precondition check failure test.
   */
  @Test(enabled=false)
  public void preConditionCheckFail() {
    boolean isIllegalState = false;
    try {
      HttpSource source = new HttpSource();
      SourceState state = GobblinMultiStageTestHelpers.prepareSourceStateWithWaterMark();
      when(state.getProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY)).thenReturn(null);
      List<WorkUnit> workUnits = source.getWorkunits(state);
    } catch (Exception e) {
      isIllegalState = e.getClass().getCanonicalName()
          .contains("IllegalStateException");
    }
    Assert.assertTrue(isIllegalState);
  }

  @Test
  public void testGetAuthenticationHeader() {
    SourceState state = new SourceState();
    HttpSource httpSource = new HttpSource();
    state.setProp("source.conn.username", "1");
    state.setProp("source.conn.password", "2");

    state.setProp("ms.authentication", "{\"method\":\"basic\",\"encryption\":\"base64\", \"header\": \"Authorization\"}");
    httpSource.initialize(state);
    Assert.assertEquals(httpSource.getHttpSourceKeys().getHttpRequestHeadersWithAuthentication().toString(), "{Authorization=Basic MToy}");

    state.setProp("ms.authentication", "{\"method\":\"bearer\",\"encryption\":\"base64\", \"header\": \"Authorization\"}");
    httpSource.initialize(state);
    Assert.assertEquals(httpSource.getHttpSourceKeys().getHttpRequestHeadersWithAuthentication().toString(), "{Authorization=Bearer MToy}");

    state.setProp("ms.authentication", "{\"method\":\"bearer\",\"encryption\":\"base64\", \"header\": \"Authorization\", \"token\": \"xyz\"}");
    httpSource.initialize(state);
    Assert.assertEquals(httpSource.getHttpSourceKeys().getHttpRequestHeadersWithAuthentication().toString(), "{Authorization=Bearer eHl6}");
  }

  /**
   * Test getAuthenticationHeader
   */
  @Test
  public void testGetAuthenticationHeader2() {
    PowerMockito.mockStatic(EncryptionUtils.class);

    HttpKeys httpSourceKeys = mock(HttpKeys.class);
    source.setHttpSourceKeys(httpSourceKeys);

    JsonObject authObj = gson.fromJson("{\"method\":\"some-method\",\"encryption\":\"base32\",\"header\":\"Authorization\"}", JsonObject.class);
    when(httpSourceKeys.getAuthentication()).thenReturn(authObj);
    Assert.assertEquals(source.getAuthenticationHeader(state), new HashMap<>());

    authObj = gson.fromJson("{\"method\":\"oauth\",\"encryption\":\"base32\",\"header\":\"Authorization\",\"token\":\"sdf23someresfsdwrw24234\"}", JsonObject.class);
    when(httpSourceKeys.getAuthentication()).thenReturn(authObj);
    String token = "someDecryptedToken";
    when(EncryptionUtils.decryptGobblin(any(), any())).thenReturn(token);
    Assert.assertEquals(source.getAuthenticationHeader(state).get("Authorization"), OAUTH_TOKEN_PREFIX + TOKEN_PREFIX_SEPARATOR + token);

    authObj = gson.fromJson("{\"method\":\"custom\",\"encryption\":\"base32\",\"header\":\"Authorization\",\"token\":\"sdf23someresfsdwrw24234\"}", JsonObject.class);
    when(httpSourceKeys.getAuthentication()).thenReturn(authObj);
    Assert.assertEquals(source.getAuthenticationHeader(state).get("Authorization"), token);
  }

  /**
   * This test, by simulation, verifies that when the http error message is contained in a normal response,
   * we will be able to retrieve that if the content type is different from the expected
   *
   * The test queries non-existent S3 endpoint, which returns 404 error as expected, but we are simulating 404
   * as success by overriding status codes.
   *
   * @throws Exception
   */
  @Test (enabled=false)
  void testHttpErrorInNormalResponse() throws Exception {
    EmbeddedGobblin job = new EmbeddedGobblin("test");
    Assert.assertTrue(job.jobFile(getClass().getResource("/pull/http-error.pull").getPath()).run().isSuccessful());
  }

   /**
   * Test getExtractor
   */
  @Test
  public void testGetExtractor() {
    initializeHelper();
    PowerMockito.mockStatic(EncryptionUtils.class);
    when(EncryptionUtils.decryptGobblin(token, state)).thenReturn(token);
    source.getExtractor(state);
    jobKeys = source.getJobKeys();
    Map<ParameterTypes, String> paginationFields = new HashMap<>();
    Map<ParameterTypes, Long> paginationInitValues = new HashMap<>();
    JsonArray fields = pagination.get("fields").getAsJsonArray();
    for (int i = 0; i < fields.size(); i++) {
      switch (fields.get(i).getAsString()) {
        case "page_start":
          paginationFields.put(ParameterTypes.PAGESTART, "page_start");
          break;
        case "page_size":
          paginationFields.put(ParameterTypes.PAGESIZE, "page_size");
          break;
        case "page_number":
          paginationFields.put(ParameterTypes.PAGENO, "page_number");
          break;
      }
    }

    JsonArray initialvalues = pagination.get("initialvalues").getAsJsonArray();
    for (int i = 0; i < initialvalues.size(); i++) {
      switch (i) {
        case 0:
          paginationInitValues.put(ParameterTypes.PAGESTART, initialvalues.get(0).getAsLong());
          break;
        case 1:
          paginationInitValues.put(ParameterTypes.PAGESIZE, initialvalues.get(1).getAsLong());
          break;
        case 2:
          paginationInitValues.put(ParameterTypes.PAGENO, initialvalues.get(2).getAsLong());
          break;
      }
    }

    Assert.assertEquals(jobKeys.getPaginationFields(), paginationFields);
    Assert.assertEquals(jobKeys.getPaginationInitValues(), paginationInitValues);
    Assert.assertEquals(jobKeys.getSessionKeyField(), sessionKeyField);
    Assert.assertEquals(jobKeys.getTotalCountField(), totalCountField);
    Assert.assertEquals(jobKeys.getSourceParameters(), parameters);
    Assert.assertEquals(jobKeys.getEncryptionField(), encryptionFields);
    Assert.assertEquals(jobKeys.getDataField(), dataField);
    Assert.assertEquals(jobKeys.getCallInterval(), callInterval.longValue());
    Assert.assertEquals(jobKeys.getSessionTimeout(), waitTimeoutSeconds.longValue() * 1000);
    Assert.assertEquals(jobKeys.getWatermarkDefinition(), watermark);
    Assert.assertEquals(jobKeys.getSecondaryInputs(), secondaryInput);
    Assert.assertEquals(source.getHttpSourceKeys().getAuthentication(), authentication);
    Assert.assertEquals(source.getHttpSourceKeys().getSourceUri(), sourceUri);
    Assert.assertEquals(source.getHttpSourceKeys().getHttpRequestMethod(), httpRequestMethod);

    Map<String, List<Integer>> httpStatuses = new HashMap<>();
    for (Map.Entry<String, JsonElement> entry : httpStatus.entrySet()) {
      String key = entry.getKey();
      List<Integer> codes = new ArrayList<>();
      for (int i = 0; i < entry.getValue().getAsJsonArray().size(); i++) {
        codes.add(entry.getValue().getAsJsonArray().get(i).getAsInt());
      }
      httpStatuses.put(key, codes);
    }
    Assert.assertEquals(source.getHttpSourceKeys().getHttpStatuses(), httpStatuses);

    Map<String, List<String>> StatusesReasons = new HashMap<>();
    for (Map.Entry<String, JsonElement> entry : httpStatusReasons.entrySet()) {
      String key = entry.getKey();
      List<String> reasons = new ArrayList<>();
      for (int i = 0; i < entry.getValue().getAsJsonArray().size(); i++) {
        reasons.add(entry.getValue().getAsJsonArray().get(i).getAsString());
      }
      StatusesReasons.put(key, reasons);
    }
    Assert.assertEquals(source.getHttpSourceKeys().getHttpStatusReasons(), StatusesReasons);
  }

  /**
   * Test getHttpStatuses
   */
  @Test
  public void testGetHttpStatuses() throws Exception {
    String statuses = "{\"success\":{\"someKey\":\"someValue\"},\"warning\":null}";
    when(state.getProp(MSTAGE_HTTP_STATUSES.getConfig(), new JsonObject().toString())).thenReturn(statuses);
    Assert.assertEquals(Whitebox.invokeMethod(source, "getHttpStatuses", state), new HashMap<>());
  }

  /**
   * Test getHttpStatusReasons
   */
  @Test
  public void testGetHttpStatusReasons() throws Exception {
    String reasons = "{\"success\":{\"someReason\":\"someValue\"},\"warning\":null}";
    when(state.getProp(MSTAGE_HTTP_STATUS_REASONS.getConfig(), new JsonObject().toString())).thenReturn(reasons);
    Assert.assertEquals(Whitebox.invokeMethod(source, "getHttpStatusReasons", state), new HashMap<>());
  }

  private void initializeHelper() {
    JsonObject allKeys = gson.fromJson(new InputStreamReader(this.getClass().getResourceAsStream("/json/sample-data-for-source.json")), JsonObject.class);
    pagination = allKeys.get(MSTAGE_PAGINATION.getConfig()).getAsJsonObject();
    when(state.getProp(MSTAGE_PAGINATION.getConfig(), new JsonObject().toString())).thenReturn(pagination.toString());

    sessionKeyField = allKeys.get(MSTAGE_SESSION_KEY_FIELD.getConfig()).getAsJsonObject();
    when(state.getProp(MSTAGE_SESSION_KEY_FIELD.getConfig(), new JsonObject().toString())).thenReturn(sessionKeyField.toString());

    totalCountField = allKeys.get(MSTAGE_TOTAL_COUNT_FIELD.getConfig()).getAsString();
    when(state.getProp(MSTAGE_TOTAL_COUNT_FIELD.getConfig(), StringUtils.EMPTY)).thenReturn(totalCountField);

    parameters = allKeys.get(MSTAGE_PARAMETERS.getConfig()).getAsJsonArray();
    when(state.getProp(MSTAGE_PARAMETERS.getConfig(), new JsonArray().toString())).thenReturn(parameters.toString());

    encryptionFields = allKeys.get(MSTAGE_ENCRYPTION_FIELDS.getConfig()).getAsJsonArray();
    when(state.getProp(MSTAGE_ENCRYPTION_FIELDS.getConfig(), new JsonArray().toString())).thenReturn(encryptionFields.toString());

    dataField = allKeys.get(MSTAGE_DATA_FIELD.getConfig()).getAsString();
    when(state.getProp(MSTAGE_DATA_FIELD.getConfig(), StringUtils.EMPTY)).thenReturn(dataField);

    callInterval = allKeys.get(MSTAGE_CALL_INTERVAL.getConfig()).getAsLong();
    when(state.getPropAsLong(MSTAGE_CALL_INTERVAL.getConfig(), 0L)).thenReturn(callInterval);

    waitTimeoutSeconds = allKeys.get(MSTAGE_WAIT_TIMEOUT_SECONDS.getConfig()).getAsLong();
    when(state.getPropAsLong(MSTAGE_WAIT_TIMEOUT_SECONDS.getConfig(), 0L)).thenReturn(waitTimeoutSeconds);

    enableCleansing = allKeys.get(MSTAGE_ENABLE_CLEANSING.getConfig()).getAsBoolean();
    when(state.getPropAsBoolean(MSTAGE_ENABLE_CLEANSING.getConfig())).thenReturn(enableCleansing);

    workUnitPartialPartition = allKeys.get(MSTAGE_WORK_UNIT_PARTIAL_PARTITION.getConfig()).getAsBoolean();
    when(state.getPropAsBoolean(MSTAGE_WORK_UNIT_PARTIAL_PARTITION.getConfig())).thenReturn(workUnitPartialPartition);

    watermark = allKeys.get(MSTAGE_WATERMARK.getConfig()).getAsJsonArray();
    when(state.getProp(MSTAGE_WATERMARK.getConfig(), new JsonArray().toString())).thenReturn(watermark.toString());

    secondaryInput = allKeys.get(MSTAGE_SECONDARY_INPUT.getConfig()).getAsJsonArray();
    when(state.getProp(MSTAGE_SECONDARY_INPUT.getConfig(), new JsonArray().toString())).thenReturn(secondaryInput.toString());

    httpClientFactory = allKeys.get(MSTAGE_CONNECTION_CLIENT_FACTORY.getConfig()).getAsString();
    when(state.getProp(MSTAGE_CONNECTION_CLIENT_FACTORY.getConfig(), StringUtils.EMPTY)).thenReturn(httpClientFactory);

    httpRequestHeaders = allKeys.get(MSTAGE_HTTP_REQUEST_HEADERS.getConfig()).getAsJsonObject();
    when(state.getProp(MSTAGE_HTTP_REQUEST_HEADERS.getConfig(), new JsonObject().toString())).thenReturn(httpRequestHeaders.toString());

    sourceUri = allKeys.get(MSTAGE_SOURCE_URI.getConfig()).getAsString();
    when(state.getProp(MSTAGE_SOURCE_URI.getConfig(), StringUtils.EMPTY)).thenReturn(sourceUri);

    httpRequestMethod = allKeys.get(MSTAGE_HTTP_REQUEST_METHOD.getConfig()).getAsString();
    when(state.getProp(MSTAGE_HTTP_REQUEST_METHOD.getConfig(), StringUtils.EMPTY)).thenReturn(httpRequestMethod);

    extractorClass = allKeys.get(MSTAGE_EXTRACTOR_CLASS.getConfig()).getAsString();
    when(state.getProp(MSTAGE_EXTRACTOR_CLASS.getConfig(), StringUtils.EMPTY)).thenReturn(extractorClass);

    authentication = allKeys.get(MSTAGE_AUTHENTICATION.getConfig()).getAsJsonObject();
    token = authentication.get("token").getAsString();
    when(state.getProp(MSTAGE_AUTHENTICATION.getConfig(), new JsonObject().toString())).thenReturn(authentication.toString());

    httpStatus = allKeys.get(MSTAGE_HTTP_STATUSES.getConfig()).getAsJsonObject();
    when(state.getProp(MSTAGE_HTTP_STATUSES.getConfig(), new JsonObject().toString())).thenReturn(httpStatus.toString());

    httpStatusReasons = allKeys.get(MSTAGE_HTTP_STATUS_REASONS.getConfig()).getAsJsonObject();
    when(state.getProp(MSTAGE_HTTP_STATUS_REASONS.getConfig(), new JsonObject().toString())).thenReturn(httpStatusReasons.toString());
  }
}
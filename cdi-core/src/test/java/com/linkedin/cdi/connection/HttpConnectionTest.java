// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.connection;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.linkedin.cdi.exception.RetriableAuthenticationException;
import com.linkedin.cdi.extractor.JsonExtractor;
import com.linkedin.cdi.extractor.MultistageExtractor;
import com.linkedin.cdi.keys.ExtractorKeys;
import com.linkedin.cdi.keys.HttpKeys;
import com.linkedin.cdi.keys.JobKeys;
import com.linkedin.cdi.source.HttpSource;
import com.linkedin.cdi.util.HttpRequestMethod;
import com.linkedin.cdi.util.WorkUnitStatus;
import gobblin.runtime.JobState;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.extract.LongWatermark;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.AutoRetryHttpClient;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.powermock.reflect.Whitebox;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.cdi.configuration.MultistageProperties.*;
import static org.mockito.Mockito.*;


@Test
@PrepareForTest({EntityUtils.class, CloseableHttpClient.class})
public class HttpConnectionTest extends PowerMockTestCase {
  private Gson gson;
  private WorkUnitState state;
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
  }

  /**
   * Test Execute
   * @throws IOException
   */
  @Test(expectedExceptions = RetriableAuthenticationException.class)
  public void testExecute() throws IOException, RetriableAuthenticationException {
    initializeHelper();

    // the source getExtractor() method will initialize source Keys
    HttpSource source = new HttpSource();
    HttpConnection conn = new HttpConnection(state, source.getHttpSourceKeys(),
        ((MultistageExtractor)source.getExtractor(state)).getExtractorKeys());

    CloseableHttpClient client = mock(CloseableHttpClient.class);
    CloseableHttpResponse response = mock(CloseableHttpResponse.class);

    conn.setHttpClient(client);
    when(client.execute(any(HttpUriRequest.class), any(HttpClientContext.class))).thenReturn(response);

    WorkUnit workUnit = mock(WorkUnit.class);
    LongWatermark lowWatermark = mock(LongWatermark.class);
    LongWatermark highWatermark = mock(LongWatermark.class);

    long lowWaterMark = 1590994800000L; //2020-06-01
    long highWaterMark = 1591513200000L; //2020-06-07
    when(workUnit.getLowWatermark(LongWatermark.class)).thenReturn(lowWatermark);
    when(lowWatermark.getValue()).thenReturn(lowWaterMark);
    when(workUnit.getExpectedHighWatermark(LongWatermark.class)).thenReturn(highWatermark);
    when(highWatermark.getValue()).thenReturn(highWaterMark);
    when(state.getWorkunit()).thenReturn(workUnit);

    HttpRequestMethod command = mock(HttpRequestMethod.class);
    WorkUnitStatus status = mock(WorkUnitStatus.class);

    JsonObject parameters = new JsonObject();
    parameters.addProperty("param1", "dummy");
    parameters.add("payload", new JsonObject());

    when(command.toString()).thenReturn("Some http method");
    conn.getExtractorKeys().setDynamicParameters(parameters);

    StatusLine statusLine = mock(StatusLine.class);
    when(response.getStatusLine()).thenReturn(statusLine);
    when(statusLine.getStatusCode()).thenReturn(200);
    when(statusLine.getReasonPhrase()).thenReturn("reason1 for success");
    Assert.assertNotNull(conn.execute(command, status));

    HttpEntity entity = mock(HttpEntity.class);
    Header header = mock(Header.class);
    when(response.getEntity()).thenReturn(entity);
    when(entity.getContentType()).thenReturn(header);

    HeaderElement element = mock(HeaderElement.class);
    when(header.getElements()).thenReturn(new HeaderElement[]{element});
    when(element.getName()).thenReturn("application/json");
    PowerMockito.mockStatic(EntityUtils.class);
    when(EntityUtils.toString(entity)).thenReturn("dummy error reason");
    Assert.assertNotNull(conn.execute(command, status));

    when(response.getEntity()).thenReturn(null);

    when(statusLine.getStatusCode()).thenReturn(204);
    Assert.assertNotNull(conn.execute(command, status));

    when(statusLine.getStatusCode()).thenReturn(302);
    when(statusLine.getReasonPhrase()).thenReturn("reason1 for warning");
    Assert.assertNull(conn.execute(command, status));

    when(statusLine.getStatusCode()).thenReturn(405);
    Assert.assertNull(conn.execute(command, status));

    when(statusLine.getReasonPhrase()).thenReturn("reason1 for error");
    Assert.assertNull(conn.execute(command, status));

    when(statusLine.getStatusCode()).thenReturn(408);
    Assert.assertNull(conn.execute(command, status));

    when(response.getEntity()).thenReturn(entity);
    doThrow(new RuntimeException()).when(entity).getContentType();
    Assert.assertNull(conn.execute(command, status));
  }


  /**
   * Test getNext
   */
  @Test
  public void testGetNext() throws RetriableAuthenticationException {
    HttpKeys httpSourceKeys = Mockito.mock(HttpKeys.class);
    when(httpSourceKeys.getCallInterval()).thenReturn(1L);
    ExtractorKeys extractorKeys = new ExtractorKeys();
    WorkUnitStatus workUnitStatus = Mockito.mock(WorkUnitStatus.class);
    WorkUnitStatus.WorkUnitStatusBuilder builder = Mockito.mock(WorkUnitStatus.WorkUnitStatusBuilder.class);
    HttpConnection conn = new HttpConnection(null, httpSourceKeys, extractorKeys);

    extractorKeys.setSignature("testSignature");
    extractorKeys.setActivationParameters(new JsonObject());
    when(builder.build()).thenReturn(workUnitStatus);
    when(workUnitStatus.toBuilder()).thenReturn(builder);
    when(httpSourceKeys.getHttpRequestMethod()).thenReturn("GET");

    Assert.assertNull(conn.executeNext(workUnitStatus));
  }

  /**
   * Test closeStream
   */
  @Test
  public void testCloseStream() throws IOException {
    HttpConnection conn = new HttpConnection(null, new HttpKeys(), new ExtractorKeys());
    MultistageExtractor extractor = mock(MultistageExtractor.class);
    ExtractorKeys keys = mock(ExtractorKeys.class);
    String testSignature = "test_signature";
    when(extractor.getExtractorKeys()).thenReturn(keys);
    when(keys.getSignature()).thenReturn(testSignature);
    conn.closeStream();

    CloseableHttpResponse httpResponse = mock(CloseableHttpResponse.class);
    conn.setResponse(httpResponse);
    doThrow(new RuntimeException()).when(httpResponse).close();
    conn.closeStream();
  }

  /**
   * Test shutdown
   */
  @Test
  public void testShutdown() throws IOException {
    HttpConnection conn = new HttpConnection(null, new HttpKeys(), null);
    CloseableHttpClient client = mock(CloseableHttpClient.class);
    conn.setHttpClient(client);

    doNothing().when(client).close();
    conn.closeAll("");

    client = mock(CloseableHttpClient.class);
    conn.setHttpClient(client);
    doThrow(IOException.class).when(client).close();
    conn.closeAll("");

    client = mock(CloseableHttpClient.class);
    conn.setHttpClient(client);
    AutoRetryHttpClient retryHttpClient = mock(AutoRetryHttpClient.class);
    conn.setHttpClient(retryHttpClient);
    conn.closeAll("");
  }

  @Test(enabled=true)
  public void retriesTest() throws IOException {

    HttpClient mockHttpClient = mock(CloseableHttpClient.class);
    HttpResponse httpResponse = mock(CloseableHttpResponse.class);
    StatusLine statusLine = mock(StatusLine.class);
    HttpEntity entity = mock(HttpEntity.class);
    SourceState state = mock(SourceState.class);

    when(entity.getContent()).thenReturn(null);
    when(httpResponse.getEntity()).thenReturn(entity);
    when(statusLine.getStatusCode()).thenReturn(401);
    when(statusLine.getReasonPhrase()).thenReturn("pagination error");
    when(httpResponse.getStatusLine()).thenReturn(statusLine);
    when(mockHttpClient.execute(any(HttpUriRequest.class), any(HttpClientContext.class))).thenReturn(httpResponse);

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
    when(state.getProp(MSTAGE_OUTPUT_SCHEMA.getConfig(), "")).thenReturn("");
    HttpSource httpSource = new HttpSource();
    List<WorkUnit> workUnits = httpSource.getWorkunits(state);
    WorkUnitState unitState = new WorkUnitState(workUnits.get(0), new JobState());

    HttpConnection conn = new HttpConnection(null, httpSource.getJobKeys(), new ExtractorKeys());
    conn.setHttpClient(mockHttpClient);
    JsonExtractor extractor = new JsonExtractor(unitState, httpSource.getHttpSourceKeys());
    extractor.setConnection(conn);

    JsonObject record = extractor.readRecord(new JsonObject());
    // since we are setting the buffer to null, the final record object will be null
    Assert.assertEquals(null, record);
  }

  /**
   * Test getResponseContentType
   */
  @Test
  public void testGetResponseContentType() throws Exception {
    HttpConnection conn = new HttpConnection(null, new HttpKeys(), null);
    HttpResponse response = mock(HttpResponse.class);
    String methodName = "getResponseContentType";
    when(response.getEntity()).thenReturn(null);
    Assert.assertEquals(Whitebox.invokeMethod(conn, methodName, response), StringUtils.EMPTY);

    HttpEntity entity = mock(HttpEntity.class);
    when(response.getEntity()).thenReturn(entity);
    when(entity.getContentType()).thenReturn(null);
    Assert.assertEquals(Whitebox.invokeMethod(conn, methodName, response), StringUtils.EMPTY);

    Header contentType = mock(Header.class);
    when(entity.getContentType()).thenReturn(contentType);

    HeaderElement[] headerElements = new HeaderElement[]{};
    when(contentType.getElements()).thenReturn(headerElements);
    Assert.assertEquals(Whitebox.invokeMethod(conn, methodName, response), StringUtils.EMPTY);

    String type = "some_type";
    HeaderElement element = mock(HeaderElement.class);
    when(element.getName()).thenReturn(type);
    headerElements = new HeaderElement[]{element};
    when(contentType.getElements()).thenReturn(headerElements);
    Assert.assertEquals(Whitebox.invokeMethod(conn, methodName, response), type);
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

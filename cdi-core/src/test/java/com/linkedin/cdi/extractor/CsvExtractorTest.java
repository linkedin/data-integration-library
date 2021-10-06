// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.extractor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.linkedin.cdi.configuration.MultistageProperties;
import com.linkedin.cdi.connection.MultistageConnection;
import com.linkedin.cdi.exception.RetriableAuthenticationException;
import com.linkedin.cdi.filter.JsonSchemaBasedFilter;
import com.linkedin.cdi.keys.CsvExtractorKeys;
import com.linkedin.cdi.keys.JobKeys;
import com.linkedin.cdi.source.HttpSource;
import com.linkedin.cdi.source.MultistageSource;
import com.linkedin.cdi.util.WorkUnitStatus;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.utils.InputStreamCSVReader;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static com.linkedin.cdi.configuration.MultistageProperties.*;
import static org.mockito.Mockito.*;


@Test
public class CsvExtractorTest {
  private static final Logger LOG = LoggerFactory.getLogger(CsvExtractorTest.class);
  private final static String DATA_SET_URN_KEY = "com.linkedin.somecase.SeriesCollection";
  private final static String ACTIVATION_PROP = "{\"name\": \"survey\", \"type\": \"unit\", \"units\": \"id1,id2\"}";
  private final static String DATA_FINAL_DIR = "/jobs/testUser/gobblin/useCaseRoot";
  private final static String FILE_PERMISSION = "775";
  private final static long ONE_HOUR_IN_MILLS = 3600000L;
  private final static long WORK_UNIT_START_TIME_KEY = 1590994800000L;
  JsonArray outputJsonSchema;
  JsonObject schema;
  private WorkUnitState state;
  private SourceState sourceState;
  private MultistageSource multiStageSource;
  private HttpSource httpSource;
  private HttpSource realHttpSource;
  private WorkUnit workUnit;
  private JobKeys jobKeys;
  private CsvExtractor csvExtractor;
  private WorkUnitStatus workUnitStatus;
  private CsvExtractorKeys csvExtractorKeys;
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

    csvExtractorKeys = mock(CsvExtractorKeys.class);
    when(csvExtractorKeys.getActivationParameters()).thenReturn(new JsonObject());


    outputJsonSchema = new JsonArray();
    schema = new JsonObject();

    // mock for state
    when(state.getWorkunit()).thenReturn(workUnit);
    when(state.getProp(MSTAGE_ACTIVATION_PROPERTY.getConfig(), new JsonObject().toString())).thenReturn(ACTIVATION_PROP);
    when(state.getPropAsLong(MSTAGE_WORKUNIT_STARTTIME_KEY.getConfig(), 0L)).thenReturn(WORK_UNIT_START_TIME_KEY);
    when(state.getProp(DATA_PUBLISHER_FINAL_DIR.getConfig(), StringUtils.EMPTY)).thenReturn(DATA_FINAL_DIR);
    when(state.getProp(MSTAGE_EXTRACTOR_TARGET_FILE_PERMISSION.getConfig(), StringUtils.EMPTY)).thenReturn(FILE_PERMISSION);

    // mock for source state
    when(sourceState.getProp("extract.table.type", "SNAPSHOT_ONLY")).thenReturn("SNAPSHOT_ONLY");
    when(sourceState.contains("source.conn.use.proxy.url")).thenReturn(true);


    // mock for source
    when(multiStageSource.getJobKeys()).thenReturn(jobKeys);

    // mock for source keys
    when(jobKeys.getOutputSchema()).thenReturn(outputJsonSchema);
    when(jobKeys.getDerivedFields()).thenReturn(new HashMap<>());

    csvExtractor = new CsvExtractor(state, multiStageSource.getJobKeys());
    csvExtractor.setCsvExtractorKeys(csvExtractorKeys);
    csvExtractor.jobKeys = jobKeys;

    multistageConnection = Mockito.mock(MultistageConnection.class);
    when(multistageConnection.executeFirst(workUnitStatus)).thenReturn(workUnitStatus);
    when(multistageConnection.executeNext(workUnitStatus)).thenReturn(workUnitStatus);
    csvExtractor.setConnection(multistageConnection);  }

  @BeforeTest
  public void setup() {
    if (System.getProperty("hadoop.home.dir") == null) {
      System.setProperty("hadoop.home.dir", "/tmp");
    }
  }

  /**
   * testing vanilla CSV Extractor
   */
  @Test
  void testExtractCSV1() throws RetriableAuthenticationException {
    InputStream inputStream = getClass().getResourceAsStream("/csv/common-crawl-files.csv");
    WorkUnitStatus status = WorkUnitStatus.builder().buffer(inputStream).build();

    when(sourceState.getProp("ms.output.schema", "" )).thenReturn("");
    when(state.getProp("ms.csv.separator", "")).thenReturn("u0009");

    // replace mocked keys with default keys
    realHttpSource.getWorkunits(sourceState);
    csvExtractor.jobKeys = realHttpSource.getJobKeys();
    csvExtractor.setCsvExtractorKeys(new CsvExtractorKeys());

    when(multistageConnection.executeFirst(csvExtractor.workUnitStatus)).thenReturn(status);

    csvExtractor.readRecord(null);
    while (csvExtractor.hasNext()) {
      String[] rst = csvExtractor.readRecord(null);
    }
    Assert.assertEquals(10, csvExtractor.getCsvExtractorKeys().getProcessedCount());
  }

  /**
   * testing u0004
   */
  @Test
  void testExtractCSV2() throws RetriableAuthenticationException {
    InputStream inputStream = getClass().getResourceAsStream("/csv/ctl_d_text.dat");
    WorkUnitStatus status = WorkUnitStatus.builder().buffer(inputStream).build();

    when(sourceState.getProp("ms.output.schema", "" )).thenReturn("");
    when(state.getProp("ms.csv.separator", "")).thenReturn("u0004");

    realHttpSource.getWorkunits(sourceState);
    CsvExtractor extractor = new CsvExtractor(state, realHttpSource.getHttpSourceKeys());
    when(multistageConnection.executeFirst(extractor.workUnitStatus)).thenReturn(status);
    extractor.setConnection(multistageConnection);

    extractor.readRecord(null);
    while (extractor.hasNext()) {
      String[] x = extractor.readRecord(null);
      Assert.assertNotNull(x);
      Assert.assertEquals(15, x.length);
    }
    Assert.assertEquals(2, extractor.getCsvExtractorKeys().getProcessedCount());
  }

  @Test
  void testExtractCSV3() throws RetriableAuthenticationException {
    InputStream inputStream = getClass().getResourceAsStream("/csv/comma-separated.csv");
    WorkUnitStatus status = WorkUnitStatus.builder().buffer(inputStream).build();

    when(sourceState.getProp("ms.output.schema", "" )).thenReturn("");
    when(MultistageProperties.MSTAGE_CSV_SEPARATOR.getValidNonblankWithDefault(state)).thenReturn("u002c");

    realHttpSource.getWorkunits(sourceState);
    CsvExtractor extractor = new CsvExtractor(state, realHttpSource.getHttpSourceKeys());
    when(multistageConnection.executeFirst(extractor.workUnitStatus)).thenReturn(status);
    extractor.setConnection(multistageConnection);

    extractor.readRecord(null);
    while (extractor.hasNext()) {
      String[] record = extractor.readRecord(null);
      Assert.assertNotNull(record);
      Assert.assertEquals(record.length, 2);
    }
    Assert.assertEquals(5, extractor.getCsvExtractorKeys().getProcessedCount());
  }

  /**
   * testing CSV extractor with Gunzip processor
   */
  @Test
  void testExtractGzippedCSV() throws RetriableAuthenticationException {
    InputStream inputStream = getClass().getResourceAsStream("/gzip/cc-index.paths.gz");
    WorkUnitStatus status = WorkUnitStatus.builder().buffer(inputStream).build();

    when(sourceState.getProp("ms.output.schema", "" )).thenReturn("");
    when(state.getProp("ms.extract.preprocessors", "")).thenReturn("com.linkedin.cdi.preprocessor.GunzipProcessor");

    realHttpSource.getWorkunits(sourceState);
    CsvExtractor extractor = new CsvExtractor(state, realHttpSource.getHttpSourceKeys());
    when(multistageConnection.executeFirst(extractor.workUnitStatus)).thenReturn(status);
    extractor.setConnection(multistageConnection);

    extractor.readRecord(null);
    while (extractor.hasNext()) {
      extractor.readRecord(null);
    }
    Assert.assertEquals(302, extractor.getCsvExtractorKeys().getProcessedCount());
  }


  /**
   * testing CSV Extractor schema inference
   * In this case, a column name contains an illegal character. Since ms.enable.cleansing is enabled by default,
   * "$" in the header should be converted to "_" but the actual data will not be cleansed.
   */
  @Test
  void testExtractCSVSchemaInference() throws RetriableAuthenticationException {
    InputStream inputStream = getClass().getResourceAsStream("/csv/ids_need_cleansing.csv");
    WorkUnitStatus status = WorkUnitStatus.builder().buffer(inputStream).build();

    when(sourceState.getProp("ms.derived.fields", new JsonArray().toString())).thenReturn("[{\"name\": \"snapshotDate\", \"formula\": {\"type\": \"epoc\", \"source\": \"currentdate\"}}]");
    // The following line is added intentionally to make sure that column projection is not enabled when user does not specify the output schema
    when(state.getProp("ms.csv.column.projection", StringUtils.EMPTY)).thenReturn("0,4,2-3");
    when(state.getProp("ms.csv.column.header", StringUtils.EMPTY)).thenReturn("true");
    when(state.getProp(MSTAGE_ENABLE_CLEANSING.getConfig(), StringUtils.EMPTY)).thenReturn("");
    when(state.getProp("ms.csv.column.header", StringUtils.EMPTY)).thenReturn("true");
    when(state.getPropAsBoolean("ms.csv.column.header")).thenReturn(true);
    when(sourceState.getProp(MultistageProperties.MSTAGE_OUTPUT_SCHEMA.getConfig(), "")).thenReturn("");

    realHttpSource.getWorkunits(sourceState);
    CsvExtractor extractor = new CsvExtractor(state, realHttpSource.getHttpSourceKeys());
    when(multistageConnection.executeFirst(extractor.workUnitStatus)).thenReturn(status);
    extractor.setConnection(multistageConnection);

    JsonParser parser = new JsonParser();
    JsonArray schema = parser.parse(extractor.getSchema()).getAsJsonArray();
    Assert.assertEquals(schema.get(0).getAsJsonObject().get("columnName").getAsString(), "id_0");
    Assert.assertEquals(schema.size(), 2);

    // check if schema has been added
    String[] row;
    row = extractor.readRecord(null);
    Assert.assertNotNull(row);
    Assert.assertEquals(row[0], "497766636$");
    Assert.assertEquals(row.length, 2);
    while (extractor.hasNext()) {
      row = extractor.readRecord(null);
      Assert.assertNotNull(row);
      Assert.assertEquals(row.length, 2);
    }
    Assert.assertEquals(10, extractor.getCsvExtractorKeys().getProcessedCount());
  }

  /**
   * Various tests for column projection
   */
  @Test
  void testColumnProjection() throws RetriableAuthenticationException {
    // testing column projection with schema and ms.csv.column.projection specified
    testColumnProjectionHelper("/csv/flat.csv",
        "[{\"columnName\":\"col1\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}}, {\"columnName\":\"col3\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}}, {\"columnName\":\"col4\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}}, {\"columnName\":\"col5\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}}]",
        true,
        false, null, "0,4,2-3");

    // testing column projection with schema and header, but without ms.csv.column.projection specified
    testColumnProjectionHelper("/csv/flat.csv",
        "[{\"columnName\":\"col1\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}}, {\"columnName\":\"col3\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}}, {\"columnName\":\"col4\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}}, {\"columnName\":\"col5\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}}]",
        true,
        false, null, "");

    // testing column projection with schema, but without header and ms.csv.column.projection specified
    testColumnProjectionHelper("/csv/flat_without_header.csv",
        "[{\"columnName\":\"col1\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}}, {\"columnName\":\"col2\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}}, {\"columnName\":\"col3\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}}, {\"columnName\":\"col4\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}}]",
        false,
        false, null, "");

    // testing column projection with schema and header, but schema contains some fields not in the header
    testColumnProjectionHelper("/csv/flat.csv",
        "[{\"columnName\":\"col11\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}}, {\"columnName\":\"col3\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}}, {\"columnName\":\"col4\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}}, {\"columnName\":\"col5\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}}]",
        true,
        true, new String[][]{{"val1", "val2", "val3", "val4"}, {"val6", "val7", "val8", "val9"}}, "");

    // testing column projection with schema and header, but headers are in upper case
    testColumnProjectionHelper("/csv/flat_uppercase_header.csv",
        "[{\"columnName\":\"col1\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}}, {\"columnName\":\"col3\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}}, {\"columnName\":\"col4\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}}, {\"columnName\":\"col5\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}}]",
        true,
        true, new String[][]{{"val1", "val3", "val4", "val5"}, {"val6", "val8", "val9", "val10"}}, "");
  }

  /**
   * Utility function to test column projection
   * @param filePath csv file path string
   * @param outputSchema output schema
   * @param hasHeader flag for having header
   * @param shouldValidateContent flag for checking content explicitly
   * @param contents array of contents for explicit checking
   * @param columnProjection explicit column projection string
   */
  private void testColumnProjectionHelper(String filePath, String outputSchema, boolean hasHeader,
      boolean shouldValidateContent, String[][] contents, String columnProjection)
      throws RetriableAuthenticationException {
    InputStream inputStream = getClass().getResourceAsStream(filePath);
    WorkUnitStatus status = WorkUnitStatus.builder().buffer(inputStream).build();
    when(sourceState.getProp("ms.output.schema", new JsonArray().toString())).thenReturn(outputSchema);
    when(state.getProp("ms.csv.column.projection", StringUtils.EMPTY)).thenReturn(columnProjection);
    when(state.getProp("ms.csv.column.header", StringUtils.EMPTY)).thenReturn(String.valueOf(hasHeader));
    when(state.getPropAsBoolean("ms.csv.column.header")).thenReturn(hasHeader);

    realHttpSource.getWorkunits(sourceState);
    CsvExtractor extractor = new CsvExtractor(state, realHttpSource.getHttpSourceKeys());
    when(multistageConnection.executeFirst(extractor.workUnitStatus)).thenReturn(status);
    extractor.setConnection(multistageConnection);

    // check if schema has been added
    JsonParser parser = new JsonParser();
    String schema = extractor.getSchema();
    Assert.assertEquals(parser.parse(schema).getAsJsonArray().size(), 4);
    String[] row;
    int index = 0;
    row = extractor.readRecord(null);
    Assert.assertNotNull(row);
    Assert.assertEquals(row.length, 4);
    if(shouldValidateContent) {
      Assert.assertEquals(row, contents[index++]);
    }
    while (extractor.hasNext()) {
      row = extractor.readRecord(null);
      Assert.assertNotNull(row);
      Assert.assertEquals(row.length, 4);
      if(shouldValidateContent) {
        Assert.assertEquals(row, contents[index++]);
      }
    }
    Assert.assertEquals(2, extractor.getCsvExtractorKeys().getProcessedCount());
  }

  /**
   * testing the interaction between add derived field with column projection
   * column projection defined and the column excluded in the middle
   */
  @Test
  void testAddDerivedFieldWithColumnProjection1() throws RetriableAuthenticationException {
    InputStream inputStream = getClass().getResourceAsStream("/csv/ids_flat.csv");
    WorkUnitStatus status = WorkUnitStatus.builder().buffer(inputStream).build();

    when(sourceState.getProp("ms.output.schema", new JsonArray().toString())).thenReturn("[{\"columnName\":\"id0\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"date\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"id2\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"id3\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"id4\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"id5\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"id6\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"id7\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"id8\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}}]");
    when(sourceState.getProp("ms.derived.fields", new JsonArray().toString())).thenReturn("[{\"name\": \"date\", \"formula\": {\"type\": \"epoc\", \"source\": \"date\", \"format\": \"yyyy-MM-dd Z\"}}]");
    when(state.getProp("ms.csv.column.projection", StringUtils.EMPTY)).thenReturn("0,2-9");
    when(state.getProp("ms.csv.column.header", StringUtils.EMPTY)).thenReturn("true");
    when(state.getPropAsBoolean("ms.csv.column.header")).thenReturn(true);

    realHttpSource.getWorkunits(sourceState);
    CsvExtractor extractor = new CsvExtractor(state, realHttpSource.getHttpSourceKeys());
    extractor.setConnection(multistageConnection);
    extractor.setJobKeys(realHttpSource.getJobKeys());
    when(multistageConnection.executeFirst(extractor.workUnitStatus)).thenReturn(status);

    // check if schema has been added
    JsonParser parser = new JsonParser();
    String schema = extractor.getSchema();
    Assert.assertEquals(parser.parse(schema).getAsJsonArray().size(), 9);

    int index = 0;
    long[] dates = new long[]{1586502000000L, 1586588400000L, 1586674800000L, 1586761200000L, 1586847600000L,
        1586934000000L, 1587020400000L, 1587106800000L, 1587193200000L, 1587279600000L};
    String[] row;
    row = extractor.readRecord(null);
    Assert.assertNotNull(row);
    Assert.assertEquals(row.length, 10);
    Assert.assertEquals(Long.parseLong(row[9]), dates[index++]);
    while (extractor.hasNext()) {
      row = extractor.readRecord(null);
      Assert.assertNotNull(row);
      Assert.assertEquals(row.length, 10);
      Assert.assertEquals(Long.parseLong(row[9]), dates[index++]);
    }
    Assert.assertEquals(10, extractor.getCsvExtractorKeys().getProcessedCount());

  }

  /**
   * testing the interaction between add derived field with column projection
   * header exists and the column excluded in the middle
   */
  @Test
  void testAddDerivedFieldWithColumnProjection2() throws RetriableAuthenticationException {
    InputStream inputStream = getClass().getResourceAsStream("/csv/ids_flat.csv");
    WorkUnitStatus status = WorkUnitStatus.builder().buffer(inputStream).build();

    when(sourceState.getProp("ms.output.schema", new JsonArray().toString())).thenReturn("[{\"columnName\":\"id0\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"date\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"id2\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"id3\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"id4\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"id5\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"id6\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"id7\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"id8\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}}]");
    when(sourceState.getProp("ms.derived.fields", new JsonArray().toString())).thenReturn("[{\"name\": \"date\", \"formula\": {\"type\": \"epoc\", \"source\": \"date\", \"format\": \"yyyy-MM-dd Z\"}}]");
    when(state.getProp("ms.csv.column.header", StringUtils.EMPTY)).thenReturn("true");
    when(state.getPropAsBoolean("ms.csv.column.header")).thenReturn(true);

    realHttpSource.getWorkunits(sourceState);
    CsvExtractor extractor = new CsvExtractor(state, realHttpSource.getHttpSourceKeys());
    extractor.setConnection(multistageConnection);
    extractor.setJobKeys(realHttpSource.getJobKeys());
    when(multistageConnection.executeFirst(extractor.workUnitStatus)).thenReturn(status);

    // check if schema has been added
    JsonParser parser = new JsonParser();
    String schema = extractor.getSchema();
    Assert.assertEquals(parser.parse(schema).getAsJsonArray().size(), 9);

    int index = 0;
    long[] dates = new long[]{1586502000000L, 1586588400000L, 1586674800000L, 1586761200000L, 1586847600000L,
        1586934000000L, 1587020400000L, 1587106800000L, 1587193200000L, 1587279600000L};
    String[] row;
    row = extractor.readRecord(null);
    Assert.assertNotNull(row);
    Assert.assertEquals(row.length, 10);
    Assert.assertEquals(Long.parseLong(row[9]), dates[index++]);
    while (extractor.hasNext()) {
      row = extractor.readRecord(null);
      Assert.assertNotNull(row);
      Assert.assertEquals(row.length, 10);
      Assert.assertEquals(Long.parseLong(row[9]), dates[index++]);
    }
    Assert.assertEquals(10, extractor.getCsvExtractorKeys().getProcessedCount());

  }

  @Test
  void testCSVParser() {
    String input = "S1234\u001AS12345\u001ATrue\u001Atest@gmail.com\u001Atest\u001AAtar-תיווך ושיווק נדל\"ן\u001AONLINE";
    InputStream stream = new ByteArrayInputStream(input.getBytes());
    CSVReader reader = new CSVReaderBuilder(new InputStreamReader(stream, StandardCharsets.UTF_8))
        .withCSVParser(new CSVParserBuilder().withSeparator("\u001A".charAt(0)).withQuoteChar("\u0000".charAt(0)).build())
        .build();
    Assert.assertEquals(7,reader.iterator().next().length);
  }

  @Test
  void testInputStreamCSVReader () throws IOException {
    String input = "S1234\u001AS12345\u001ATrue\u001Atest@gmail.com\u001Atest\u001AAtar-תיווך ושיווק נדל\"ן\u001AONLINE";
    InputStreamCSVReader reader = new InputStreamCSVReader(input,"\u001A".charAt(0),"\u0000".charAt(0));
    Assert.assertEquals(7,reader.splitRecord().size());
  }

  @Test
  public void testProcessInputStream() throws RetriableAuthenticationException {
    Iterator<String[]> csvIterator = Mockito.mock(Iterator.class);
    when(csvExtractorKeys.getCsvIterator()).thenReturn(csvIterator);
    CsvExtractor extractor = new CsvExtractor(state, multiStageSource.getJobKeys());
    extractor.setConnection(multistageConnection);
    extractor.setJobKeys(new JobKeys());
    when(multistageConnection.executeNext(extractor.workUnitStatus)).thenReturn(null);
    doNothing().when(state).setWorkingState(WorkUnitState.WorkingState.FAILED);
    Assert.assertFalse(extractor.processInputStream(10));

    when(multistageConnection.executeNext(extractor.workUnitStatus)).thenReturn(workUnitStatus);
    Map<String, String> messages = ImmutableMap.of("contentType", "non-text/csv");
    when(workUnitStatus.getMessages()).thenReturn(messages);
    Assert.assertFalse(extractor.processInputStream(10));

    messages = ImmutableMap.of("contentType", "text/csv", "schema", "test_schema");
    when(workUnitStatus.getMessages()).thenReturn(messages);
    when(workUnitStatus.getSchema()).thenReturn(new JsonArray());
    when(workUnitStatus.getBuffer()).thenReturn(null);
    Assert.assertFalse(extractor.processInputStream(10));

    when(workUnitStatus.getBuffer()).thenReturn(new ByteArrayInputStream("test_string".getBytes()));
    when(csvExtractorKeys.getCsvIterator()).thenReturn(null);
    when(multistageConnection.executeFirst(extractor.workUnitStatus)).thenReturn(workUnitStatus);
    when(csvExtractorKeys.getSeparator()).thenReturn(",");
    when(csvExtractorKeys.getQuoteCharacter()).thenReturn("\"");
    when(csvExtractorKeys.getEscapeCharacter()).thenReturn("u005C");
    Assert.assertTrue(extractor.processInputStream(10));

    schema.addProperty("someKey", "someValue");
    //when(outputJsonSchema.getSchema()).thenReturn(schema);
    doThrow(new RuntimeException()).when(csvExtractorKeys).setCsvIterator(any());
    Assert.assertFalse(extractor.processInputStream(10));
  }

  @Test
  public void testExpandColumnProjection() throws Exception {
    state = new WorkUnitState();
    WorkUnitState workUnitState = PowerMockito.spy(state);
    initExtractor(workUnitState);

    csvExtractor = new CsvExtractor(workUnitState, multiStageSource.getJobKeys());
    Method method = CsvExtractor.class.getDeclaredMethod("expandColumnProjection", String.class, int.class);
    method.setAccessible(true);
    Assert.assertEquals(method.invoke(csvExtractor, "0,4,2-3", 4).toString(), "[0, 2, 3, 4]");

    Assert.assertEquals(method.invoke(csvExtractor, "0,4,2-3", 3).toString(), "[0, 2, 3, 4]");
    Assert.assertEquals(method.invoke(csvExtractor, null, 4).toString(), "[]");
    Assert.assertEquals(method.invoke(csvExtractor, "", 4).toString(), "[]");
    Assert.assertEquals(method.invoke(csvExtractor, "-1-4", 4).toString(), "[]");
    Assert.assertEquals(method.invoke(csvExtractor, "2--2", 4).toString(), "[]");
    Assert.assertEquals(method.invoke(csvExtractor, "-2--3", 4).toString(), "[]");
    Assert.assertEquals(method.invoke(csvExtractor, "3-3", 4).toString(), "[]");
    verify(workUnitState, atLeast(7)).setWorkingState(WorkUnitState.WorkingState.FAILED);

    when(workUnitState.getProp(MSTAGE_CSV_COLUMN_HEADER.getConfig(), StringUtils.EMPTY)).thenReturn("false");
    csvExtractor = new CsvExtractor(workUnitState, multiStageSource.getJobKeys());
    method = CsvExtractor.class.getDeclaredMethod("expandColumnProjection", String.class, int.class);
    method.setAccessible(true);
    Assert.assertEquals(method.invoke(csvExtractor, "3-1", 4).toString(), "[]");
    Assert.assertEquals(method.invoke(csvExtractor, "-1", 4).toString(), "[]");
    Assert.assertEquals(method.invoke(csvExtractor, "abc", 4).toString(), "[]");
    verify(workUnitState, atLeast(3)).setWorkingState(WorkUnitState.WorkingState.FAILED);
  }

  @Test
  public void testProcessGzipInputStream() throws RetriableAuthenticationException {
    CsvExtractor extractor = new CsvExtractor(state, multiStageSource.getJobKeys());
    extractor.setConnection(multistageConnection);
    extractor.setCsvExtractorKeys(new CsvExtractorKeys());
    extractor.setJobKeys(new JobKeys());
    when(multistageConnection.executeFirst(extractor.workUnitStatus)).thenReturn(workUnitStatus);
    when(multistageConnection.executeNext(extractor.workUnitStatus)).thenReturn(workUnitStatus);

    Map<String, String>  messages = ImmutableMap.of("contentType", "application/gzip", "schema", "test_schema");
    when(workUnitStatus.getMessages()).thenReturn(messages);
    Assert.assertFalse(extractor.processInputStream(10));
  }

  @Test
  public void testAddDerivedFields() throws Exception {
    initExtractor(state);
    csvExtractor.setTimezone("America/Los_Angeles");

    // derived field is in unsupported type
    Map<String, Map<String, String>> derivedFields = ImmutableMap.of("formula",
        ImmutableMap.of("type", "non-epoc", "source", "start_time", "format", "yyyy-MM-dd"));
    when(jobKeys.getDerivedFields()).thenReturn(derivedFields);
    when(csvExtractorKeys.getColumnToIndexMap()).thenReturn(ImmutableMap.of());
    Object[] row = new Object[]{new String[1]};
    String[] res = Whitebox.invokeMethod(csvExtractor, "addDerivedFields", row);
    // Since the type is supported, we created a new record with new columns.
    // In reality, the work unit will fail when processing the derived field's value.
    Assert.assertEquals(res.length, 2);
    Assert.assertNull(res[0]);

    // derived field is empty early exit
    derivedFields = ImmutableMap.of();
    when(jobKeys.getDerivedFields()).thenReturn(derivedFields);
    when(csvExtractorKeys.getColumnToIndexMap()).thenReturn(ImmutableMap.of());
    row = new Object[]{new String[1]};
    res = Whitebox.invokeMethod(csvExtractor, "addDerivedFields", row);
    Assert.assertEquals(res.length, 1);
    Assert.assertNull(res[0]);

    // derived field is currentdate
    derivedFields = ImmutableMap.of("current_date",
        ImmutableMap.of("type", "epoc", "source", "currentdate"));
    when(jobKeys.getDerivedFields()).thenReturn(derivedFields);
    when(csvExtractorKeys.getColumnToIndexMap()).thenReturn(ImmutableMap.of("a", 0));
    row = new Object[]{new String[]{"a"}};
    res = Whitebox.invokeMethod(csvExtractor, "addDerivedFields", row);
    Assert.assertEquals(res.length, 2);
    Assert.assertEquals(res[0], "a");
    Assert.assertTrue(Math.abs(Long.parseLong(res[1]) - DateTime.now().getMillis()) < ONE_HOUR_IN_MILLS);

    // derived field is P1D
    derivedFields = ImmutableMap.of("current_date",
        ImmutableMap.of("type", "epoc", "source", "P1D"));
    when(jobKeys.getDerivedFields()).thenReturn(derivedFields);
    when(csvExtractorKeys.getColumnToIndexMap()).thenReturn(ImmutableMap.of("a", 0));
    row = new Object[]{new String[]{"a"}};
    res = Whitebox.invokeMethod(csvExtractor, "addDerivedFields", row);
    Assert.assertEquals(res.length, 2);
    Assert.assertEquals(res[0], "a");
    DateTimeZone timeZone = DateTimeZone.forID("America/Los_Angeles");
    Period period = Period.parse("P1D");
    long p1d = DateTime.now().withZone(timeZone).minus(period).dayOfMonth().roundFloorCopy().getMillis();
    Assert.assertTrue(Math.abs(Long.parseLong(res[1]) - p1d) < ONE_HOUR_IN_MILLS);

    // derived field is in the specified format
    derivedFields = ImmutableMap.of("current_date",
        ImmutableMap.of("type", "epoc", "source", "start_time", "format", "yyyy-MM-dd"));
    when(jobKeys.getDerivedFields()).thenReturn(derivedFields);
    when(csvExtractorKeys.getColumnToIndexMap()).thenReturn(ImmutableMap.of("start_time", 0));
    row = new Object[]{new String[]{"2020-06-01"}};
    res = Whitebox.invokeMethod(csvExtractor, "addDerivedFields", row);
    Assert.assertEquals(res.length, 2);
    Assert.assertEquals(res[0], "2020-06-01");
    DateTimeFormatter datetimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd");
    Assert.assertEquals(Long.parseLong(res[1]), datetimeFormatter.parseDateTime("2020-06-01").getMillis());

    // derived field is NOT in the specified format
    derivedFields = ImmutableMap.of("current_date",
        ImmutableMap.of("type", "epoc", "source", "start_time", "format", "yyyy-MM-dd"));
    when(jobKeys.getDerivedFields()).thenReturn(derivedFields);
    when(csvExtractorKeys.getColumnToIndexMap()).thenReturn(ImmutableMap.of("start_time", 0));
    row = new Object[]{new String[]{"notdatatime"}};
    res = Whitebox.invokeMethod(csvExtractor, "addDerivedFields", row);
    // Since the type is supported, we created a new record with new columns.
    // In reality, the work unit will fail when processing the derived field's value.
    Assert.assertEquals(res.length, 2);
    Assert.assertEquals(res[0], "notdatatime");
    Assert.assertEquals(res[1], "");

    // derived fields are from variables
    JsonObject parameters = new JsonObject();
    parameters.addProperty("dateString", "2019-11-01 12:00:00");
    parameters.addProperty("someInteger", 123456);
    parameters.addProperty("someNumber", 123.456);
    parameters.addProperty("someEpoc", 1601038688000L);
    csvExtractor.currentParameters = parameters;

    derivedFields = ImmutableMap.of("dateString",
        ImmutableMap.of("type", "string", "source", "{{dateString}}"),
        "someInteger",
        ImmutableMap.of("type", "integer", "source", "{{someInteger}}"),
        "someEpoc",
        ImmutableMap.of("type", "epoc", "source", "{{someEpoc}}"),
        "someNumber",
        ImmutableMap.of("type", "number", "source", "{{someNumber}}"));
    when(jobKeys.getDerivedFields()).thenReturn(derivedFields);
    when(csvExtractorKeys.getColumnToIndexMap()).thenReturn(ImmutableMap.of("start_time", 0));
    row = new Object[]{new String[]{"2020-06-01"}};
    res = Whitebox.invokeMethod(csvExtractor, "addDerivedFields", row);
    Assert.assertEquals(res.length, 5);
    Assert.assertEquals(res[0], "2020-06-01");
    Assert.assertEquals(res[1], "2019-11-01 12:00:00");
    Assert.assertEquals(res[2], "123456");
    Assert.assertEquals(res[3], "1601038688000");
    Assert.assertEquals(res[4], "123.456");
  }

  private void testSkipRowAndSaveHeaderHelper(CsvExtractor csvExtractor, List<String[]> rows, int expectedLinesRemaining)
      throws Exception {
    Iterator<String[]> rowIterator = rows.iterator();
    Whitebox.invokeMethod(csvExtractor, "skipRowAndSaveHeader", rowIterator);
    int linesRemaining = 0;
    while (rowIterator.hasNext()) {
      linesRemaining ++;
      rowIterator.next();
    }
    Assert.assertEquals(linesRemaining, expectedLinesRemaining);
  }

  @Test
  public void testSkipRowAndSaveHeader() throws Exception {
    initExtractor(state);
    String[] someData = new String[]{"some_date"};
    String[] moreData = new String[]{"more_data"};
    CsvExtractorKeys csvExtractorKeys = new CsvExtractorKeys();
    CsvExtractorKeys spy = spy(csvExtractorKeys);
    csvExtractor.setCsvExtractorKeys(spy);
    // Three lines of data, skipping two, so there should be one left
    List<String[]> rows = ImmutableList.of(someData, moreData, moreData);
    when(spy.getRowsToSkip()).thenReturn(2);
    when(spy.getColumnHeader()).thenReturn(false);
    testSkipRowAndSaveHeaderHelper(csvExtractor, rows, 1);
    verify(spy, atMost(0)).setHeaderRow(someData);
    verify(spy, atMost(0)).setHeaderRow(moreData);

    rows = ImmutableList.of(someData, someData, moreData, moreData, moreData);
    when(spy.getRowsToSkip()).thenReturn(3);
    when(spy.getColumnHeaderIndex()).thenReturn(2);
    when(spy.getColumnHeader()).thenReturn(true);
    testSkipRowAndSaveHeaderHelper(csvExtractor, rows, 2);

    rows = ImmutableList.of(someData, someData, moreData, someData, moreData);
    when(spy.getRowsToSkip()).thenReturn(4);
    when(spy.getColumnHeaderIndex()).thenReturn(2);
    when(spy.getColumnHeader()).thenReturn(true);
    testSkipRowAndSaveHeaderHelper(csvExtractor, rows, 1);
  }

  private void testExtractCSVWithSkipLinesHelper(String filepath, boolean hasHeader, int headerIndex,
      int explicitRowsToSkip, int expectedRecordLength, int expectedProcessedCount) throws RetriableAuthenticationException {
    InputStream inputStream = getClass().getResourceAsStream(filepath);
    WorkUnitStatus status = WorkUnitStatus.builder().buffer(inputStream).build();

    when(sourceState.getProp("ms.output.schema", "" )).thenReturn("");
    when(state.getProp("ms.csv.column.header", StringUtils.EMPTY)).thenReturn(String.valueOf(hasHeader));
    when(state.getPropAsBoolean("ms.csv.column.header")).thenReturn(hasHeader);
    if (explicitRowsToSkip > 0) {
      when(state.getPropAsInt("ms.csv.skip.lines", 0)).thenReturn(explicitRowsToSkip);
    }
    when(state.getPropAsInt("ms.csv.column.header.index", 0)).thenReturn(headerIndex);
    when(MultistageProperties.MSTAGE_CSV_SEPARATOR.getValidNonblankWithDefault(state)).thenReturn("u002c");

    realHttpSource.getWorkunits(sourceState);
    CsvExtractor extractor = new CsvExtractor(state, realHttpSource.getHttpSourceKeys());
    when(multistageConnection.executeFirst(extractor.workUnitStatus)).thenReturn(status);
    extractor.setConnection(multistageConnection);

    extractor.readRecord(null);
    while (extractor.hasNext()) {
      String[] record = extractor.readRecord(null);
      Assert.assertNotNull(record);
      Assert.assertEquals(record.length, expectedRecordLength);
    }
    Assert.assertEquals(expectedProcessedCount, extractor.getCsvExtractorKeys().getProcessedCount());
  }

  @Test
  public void testExtractCSVWithSkipLines() throws RetriableAuthenticationException {
    testExtractCSVWithSkipLinesHelper("/csv/ids_multiple_header_lines.csv",
        true, 2, 0,2, 10);

    testExtractCSVWithSkipLinesHelper("/csv/ids_multiple_header_lines.csv",
        true, 1, 3, 2, 10);

    // the work unit should fail in reality, but in unit test records are still processed
    // there should be an error log saying header index out of bound
    testExtractCSVWithSkipLinesHelper("/csv/ids_multiple_header_lines.csv",
        true, 3, 3, 2, 10);
  }

  @Test
  public void testInferSchemaWithSample() throws Exception {
    initExtractor(state);

    String[] someData = new String[]{"some_date"};
    String[] moreData = new String[]{"more_data"};
    List<String[]> rows = ImmutableList.of(someData, moreData);
    csvExtractorKeys = new CsvExtractorKeys();
    CsvExtractorKeys spy = spy(csvExtractorKeys);
    csvExtractor.setCsvExtractorKeys(spy);
    when(spy.getRowsToSkip()).thenReturn(0);
    Deque deque = new LinkedList();
    when(spy.getSampleRows()).thenReturn(deque);

    when(spy.getHeaderRow()).thenReturn(new String[]{"col1", "col2"});
    Assert.assertEquals(
        Whitebox.invokeMethod(csvExtractor, "inferSchemaWithSample", rows.iterator()).toString(),
        "[{\"columnName\":\"col0\",\"isNullable\":false,\"dataType\":{\"type\":\"string\"}}]");

    when(spy.getHeaderRow()).thenReturn(null);
    Whitebox.invokeMethod(csvExtractor, "inferSchemaWithSample", rows.iterator());
    Assert.assertEquals(
        Whitebox.invokeMethod(csvExtractor, "inferSchemaWithSample", rows.iterator()).toString(),
        "[{\"columnName\":\"col0\",\"isNullable\":false,\"dataType\":{\"type\":\"string\"}}]"
    );
  }

  @Test
  public void testSetRowFilter() {
    JsonSchemaBasedFilter filter = Mockito.mock(JsonSchemaBasedFilter.class);
    JsonArray schema = new JsonArray();
    csvExtractor.rowFilter = filter;
    csvExtractor.setRowFilter(schema);

    csvExtractor.rowFilter = null;
    when(state.getProp(MultistageProperties.MSTAGE_ENABLE_SCHEMA_BASED_FILTERING.getConfig(), StringUtils.EMPTY)).thenReturn("false");
    csvExtractor.setRowFilter(new JsonArray());
    Assert.assertNull(csvExtractor.rowFilter);
  }

  @Test
  public void testAddParsedCSVData() throws Exception {
    initExtractor(state);
    Method method = CsvExtractor.class.getDeclaredMethod("addParsedCSVData", String.class, String.class, JsonObject.class);
    method.setAccessible(true);
    when(csvExtractorKeys.getDefaultFieldType()).thenReturn("");
    method.invoke(csvExtractor, "key1", "true", schema);
    Assert.assertEquals(schema.get("key1").getAsBoolean(), true);

    when(csvExtractorKeys.getDefaultFieldType()).thenReturn("");
    method.invoke(csvExtractor, "key2", "false", schema);
    Assert.assertEquals(schema.get("key2").getAsBoolean(), false);

    when(csvExtractorKeys.getDefaultFieldType()).thenReturn("");
    method.invoke(csvExtractor, "key3", "1.234F", schema);
    Assert.assertEquals(schema.get("key3").getAsFloat(), 1.234F);

    when(csvExtractorKeys.getDefaultFieldType()).thenReturn("");
    method.invoke(csvExtractor, "key4", "something else", schema);
    Assert.assertEquals(schema.get("key4").getAsString(), "something else");

    when(csvExtractorKeys.getDefaultFieldType()).thenReturn("string");
    method.invoke(csvExtractor, "key5", "123", schema);
    Assert.assertEquals(schema.get("key5").getAsString(), "123");

    when(csvExtractorKeys.getDefaultFieldType()).thenReturn("int");
    method.invoke(csvExtractor, "key5", "123", schema);
    Assert.assertEquals(schema.get("key5").getAsInt(), 123);

    when(csvExtractorKeys.getDefaultFieldType()).thenReturn("boolean");
    method.invoke(csvExtractor, "key1", "true", schema);
    Assert.assertEquals(schema.get("key1").getAsBoolean(), true);

    method.invoke(csvExtractor, "key2", "false", schema);
    Assert.assertEquals(schema.get("key2").getAsBoolean(), false);

    when(csvExtractorKeys.getDefaultFieldType()).thenReturn("float");
    method.invoke(csvExtractor, "key3", "1.234F", schema);
    Assert.assertEquals(schema.get("key3").getAsFloat(), 1.234F);

    when(csvExtractorKeys.getDefaultFieldType()).thenReturn("long");
    method.invoke(csvExtractor, "key5", "9993939399", schema);
    Assert.assertEquals(schema.get("key5").getAsLong(), 9993939399L);

    when(csvExtractorKeys.getDefaultFieldType()).thenReturn("double");
    method.invoke(csvExtractor, "key5", "1.234D", schema);
    Assert.assertEquals(schema.get("key5").getAsDouble(), 1.234D);
  }

  private void initExtractor(WorkUnitState state) {
    when(state.getProp(MSTAGE_CSV_COLUMN_HEADER.getConfig(), StringUtils.EMPTY)).thenReturn("true");
    when(state.getPropAsBoolean(MSTAGE_CSV_COLUMN_HEADER.getConfig())).thenReturn(true);
    when(state.getPropAsInt(MSTAGE_CSV_SKIP_LINES.getConfig(), 0)).thenReturn(2);
    when(state.getProp(MSTAGE_CSV_SEPARATOR.getConfig(), StringUtils.EMPTY)).thenReturn(",");
    when(state.getProp(MSTAGE_CSV_QUOTE_CHARACTER.getConfig(), StringUtils.EMPTY)).thenReturn("\"");
    when(state.getProp(MSTAGE_CSV_ESCAPE_CHARACTER.getConfig(), StringUtils.EMPTY)).thenReturn("u005C");
    when(state.getProp(MSTAGE_EXTRACT_PREPROCESSORS_PARAMETERS.getConfig(), new JsonObject().toString())).thenReturn(StringUtils.EMPTY);
    when(state.getProp(MSTAGE_EXTRACT_PREPROCESSORS.getConfig(), StringUtils.EMPTY)).thenReturn(StringUtils.EMPTY);
  }

  @Test
  private void testSchemaWithDuplicates() {
    initExtractor(state);
    String schemaString = "[{\"columnName\":\"derived_delta\"," + "\"comment\":\"\","
        + "\"isNullable\":\"true\"," + "\"dataType\":{\"type\":\"long\"}}]";
    Map<String, Map<String, String>> derivedFields = ImmutableMap.of("derived_delta",
        ImmutableMap.of("type", "non-epoc", "source", "start_time", "format", "yyyy-MM-dd"));
    when(jobKeys.getDerivedFields()).thenReturn(derivedFields);
    JsonArray schema = new Gson().fromJson(schemaString, JsonArray.class);
    when(jobKeys.hasOutputSchema()).thenReturn(true);
    when(jobKeys.getOutputSchema()).thenReturn(schema);
    Assert.assertEquals(csvExtractor.getSchema(), schemaString);
    Map<String, Map<String, String>> derivedFields1 = ImmutableMap.of("derived_field",
        ImmutableMap.of("type", "non-epoc", "source", "start_time", "format", "yyyy-MM-dd"));
    when(jobKeys.getDerivedFields()).thenReturn(derivedFields1);
    String resultSchema = "[{\"columnName\":\"derived_delta\",\"comment\":\"\",\"isNullable\":\"true\","
        + "\"dataType\":{\"type\":\"long\"}},{\"columnName\":\"derived_field\",\"dataType\":{\"type\":\"string\"}}]";
    Assert.assertEquals(csvExtractor.getSchema(), resultSchema);
  }
}

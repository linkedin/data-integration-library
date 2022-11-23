// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.source;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.linkedin.cdi.factory.reader.SchemaReader;
import com.linkedin.cdi.keys.JobKeys;
import com.linkedin.cdi.util.EndecoUtils;
import com.linkedin.cdi.util.WatermarkDefinition;
import com.linkedin.cdi.util.WorkUnitPartitionTypes;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.WatermarkInterval;
import org.apache.gobblin.source.extractor.extract.LongWatermark;
import org.apache.gobblin.source.workunit.Extract;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.cdi.configuration.PropertyCollection.*;
import static org.mockito.Mockito.*;


public class MultistageSourceTest {
  private final static DateTimeFormatter JODA_DATE_TIME_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ");
  private final static DateTimeZone PST_TIMEZONE = DateTimeZone.forTimeZone(TimeZone.getTimeZone("PST"));
  private final static DateTimeFormatter DTF_PST_TIMEZONE = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss").withZone(PST_TIMEZONE);
  private Gson gson;
  private MultistageSource source;

  @BeforeMethod
  public void setUp() {
    gson = new Gson();
    source = new MultistageSource();
  }

  @Test
  public void testWorkUnitPartitionDef(){
    SourceState state = new SourceState();
    state.setProp("extract.table.name", "xxx");
    state.setProp("ms.work.unit.partition", "daily");
    state.setProp(MSTAGE_OUTPUT_SCHEMA.getConfig(), "");

    MultistageSource source = new MultistageSource();
    source.getWorkunits(state);

    String expected = "daily";
    Assert.assertEquals(expected, MSTAGE_WORK_UNIT_PARTITION.get(state));
  }

  @Test
  public void testWorkUnitPacingDef(){
    SourceState state = new SourceState();
    state.setProp("extract.table.name", "xxx");
    state.setProp("ms.work.unit.pacing.seconds", "10");
    MultistageSource source = new MultistageSource();
    source.getWorkunits(state);
    Assert.assertEquals(((Integer) MSTAGE_WORK_UNIT_PACING_SECONDS.get(state)).intValue(), 10);
  }

  @Test
  public void testWorkUnitPacingConversion(){
    SourceState state = new SourceState();
    state.setProp("extract.table.name", "xxx");
    state.setProp("ms.work.unit.pacing.seconds", "10");
    MultistageSource source = new MultistageSource();
    source.getWorkunits(state);
    Assert.assertEquals(MSTAGE_WORK_UNIT_PACING_SECONDS.getMillis(state).longValue(), 10000L);
  }

  @Test (expectedExceptions = RuntimeException.class)
  public void testGetWorkUnitsMinimumUnits() {
    SourceState state = new SourceState();
    state.setProp("ms.watermark",
        "[{\"name\": \"system\",\"type\": \"datetime\", \"range\": {\"from\": \"2000-01-01\", \"to\": \"-\"}}]");
    state.setProp("extract.table.type", "SNAPSHOT_ONLY");
    state.setProp("extract.namespace", "test");
    state.setProp("extract.table.name", "table1");
    state.setProp("ms.work.unit.partition", "hourly");
    state.setProp("ms.pagination", "{}");
    state.setProp("ms.work.unit.min.units", "1");
    MultistageSource source = new MultistageSource();
    source.getWorkunits(state);
  }

  @Test (expectedExceptions = RuntimeException.class)
  public void testGetWorkUnitsMinimumUnits2() {
    SourceState state = new SourceState();
    state.setProp("ms.watermark",
        "[{\"name\": \"system\",\"type\": \"datetime\", \"range\": {\"from\": \"2000-01-01\", \"to\": \"-\"}}, "
            + "{\"name\": \"unitWatermark\",\"type\": \"unit\", \"units\": \"unit1\"}]");
    state.setProp("extract.table.type", "SNAPSHOT_ONLY");
    state.setProp("extract.namespace", "test");
    state.setProp("extract.table.name", "table1");
    state.setProp("ms.pagination", "{}");
    state.setProp("ms.work.unit.min.units", "2");
    MultistageSource source = new MultistageSource();
    source.getWorkunits(state);
  }

  @Test
  public void testGetWorkUnitsDefault(){
    SourceState state = new SourceState();
    state.setProp("ms.watermark", "[{\"name\": \"system\",\"type\": \"datetime\", \"range\": {\"from\": \"2017-01-01\", \"to\": \"-\"}}]");
    state.setProp("extract.table.type", "SNAPSHOT_ONLY");
    state.setProp("extract.namespace", "test");
    state.setProp("extract.table.name", "table1");
    state.setProp("ms.work.unit.partition", "");
    state.setProp("ms.pagination", "{}");
    MultistageSource source = new MultistageSource();
    source.getWorkunits(state);

    //Assert.assertEquals(source.getMyProperty(WORK_UNIT_PARTITION), "weekly");
    Extract extract = source.createExtractObject(true);
    WorkUnit workUnit = WorkUnit.create(extract,
        new WatermarkInterval(new LongWatermark(1483257600000L), new LongWatermark(1572660000000L)));
    workUnit.setProp("ms.watermark.groups", "[\"watermark.datetime\",\"watermark.unit\"]");
    workUnit.setProp("watermark.datetime", "(1483257600000,1572660000000)");
    workUnit.setProp("watermark.unit", "NONE");
    WorkUnit workUnit1 = (WorkUnit) source.getWorkunits(state).get(0);
    Assert.assertEquals(workUnit1.getLowWatermark().toString(), workUnit.getLowWatermark().toString());
    Assert.assertEquals(workUnit1.getProp(DATASET_URN.toString()), "[watermark.system.1483257600000, watermark.unit.{}]");
    Assert.assertEquals(workUnit1.getProp(MSTAGE_WATERMARK_GROUPS.toString()), "[\"watermark.system\",\"watermark.unit\"]");
  }

  @Test
  public void testDerivedFields() {
    SourceState sourceState = new SourceState();
    sourceState.setProp("extract.table.name","xxx");
    sourceState.setProp("ms.derived.fields", "[{\"name\": \"activityDate\", \"formula\": {\"type\": \"epoc\", \"source\": \"fromDateTime\", \"format\": \"yyyy-MM-dd'T'HH:mm:ss'Z'\"}}]");
    MultistageSource source = new MultistageSource();
    source.getWorkunits(sourceState);
    Assert.assertEquals(source.getJobKeys().getDerivedFields().keySet().toString(), "[activityDate]");
  }

  @Test
  public void testOutputSchema(){
    SourceState state = new SourceState();
    state.setProp("extract.table.name", "xxx");
    state.setProp("ms.output.schema", "");
    MultistageSource source = new MultistageSource();
    source.getWorkunits(state);
    Assert.assertEquals(0, source.getJobKeys().getOutputSchema().size());
  }

  @Test
  public void testSourceParameters(){
    SourceState sourceState = new SourceState();
    sourceState.setProp("extract.table.name", "xxx");
    sourceState.setProp(MSTAGE_OUTPUT_SCHEMA.getConfig(), "");
    MultistageSource source = new MultistageSource();
    source.getWorkunits(sourceState);
    sourceState.setProp("ms.parameters", "[{\"name\":\"cursor\",\"type\":\"session\"}]");
    source.getWorkunits(sourceState);
  }

  @Test
  public void testHadoopFsEncoding() {
    String plain = "[watermark.system.1483257600000, watermark.activation.{\"s3key\":\"cc-index/collections/CC-MAIN-2019-43/indexes/cdx-00000.gz\"}]";
    String expected = "[watermark.system.1483257600000, watermark.activation.{\"s3key\":\"cc-index%2Fcollections%2FCC-MAIN-2019-43%2Findexes%2Fcdx-00000.gz\"}]";
    String encoded = EndecoUtils.getHadoopFsEncoded(plain);
    Assert.assertEquals(encoded, expected);
  }

  @Test
  public void testUrlEncoding() {
    String plain = "{a b}";
    String expected = "%7Ba+b%7D";
    String encoded = EndecoUtils.getEncodedUtf8(plain);
    Assert.assertEquals(encoded, expected);
  }

  @Test
  public void testUnitWatermark(){
    SourceState state = new SourceState();
    state.setProp("ms.watermark", "[{\"name\": \"system\",\"type\": \"datetime\", \"range\": {\"from\": \"2020-01-01\", \"to\": \"2020-01-31\"}}, {\"name\": \"units\",\"type\": \"unit\", \"units\": \"id1,id2,id3\"}]");
    state.setProp("extract.table.type", "SNAPSHOT_ONLY");
    state.setProp("extract.namespace", "test");
    state.setProp("extract.table.name", "table1");
    state.setProp("ms.work.unit.partition", "");
    state.setProp("ms.pagination", "{}");
    MultistageSource source = new MultistageSource();
    Assert.assertEquals(source.getWorkunits(state).size(), 3);
  }

  @Test
  public void testGetUpdatedWorkUnitActivation() {
    WorkUnit workUnit = Mockito.mock(WorkUnit.class);
    JsonObject authentication = gson.fromJson("{\"method\": \"basic\", \"encryption\": \"base64\", \"header\": \"Authorization\"}", JsonObject.class);
    when(workUnit.getProp(MSTAGE_ACTIVATION_PROPERTY.toString(), StringUtils.EMPTY)).thenReturn(StringUtils.EMPTY);
    Assert.assertEquals(source.getUpdatedWorkUnitActivation(workUnit, authentication), authentication.toString());
  }

  /**
   * Test getExtractor when exception is thrown
   */
  @Test(expectedExceptions = RuntimeException.class)
  public void testGetExtractorWithException() {
    WorkUnitState state = Mockito.mock(WorkUnitState.class);
    source.getExtractor(state);
  }

  /**
   * Test generateWorkUnits when there are more than one DATETIME datetime type watermarks
   * Expected: RuntimeException
   */
  @Test(expectedExceptions = RuntimeException.class)
  public void testGenerateWorkUnitsWithException1() {
    testGenerateWorkUnitsHelper(WatermarkDefinition.WatermarkTypes.DATETIME);
  }

  /**
   * Test generateWorkUnits when there are more than one UNIT type watermarks
   * Expected: RuntimeException
   */
  @Test(expectedExceptions = RuntimeException.class)
  public void testGenerateWorkUnitsWithException2() {
    testGenerateWorkUnitsHelper(WatermarkDefinition.WatermarkTypes.UNIT);
  }

  private void testGenerateWorkUnitsHelper(WatermarkDefinition.WatermarkTypes watermarkTypes) {
    SourceState sourceState = Mockito.mock(SourceState.class);
    source.sourceState = sourceState;

    WatermarkDefinition watermarkDefinition1 = Mockito.mock(WatermarkDefinition.class);
    WatermarkDefinition watermarkDefinition2 = Mockito.mock(WatermarkDefinition.class);
    when(watermarkDefinition1.getType()).thenReturn(watermarkTypes);
    when(watermarkDefinition2.getType()).thenReturn(watermarkTypes);
    List<WatermarkDefinition> definitions = ImmutableList.of(watermarkDefinition1, watermarkDefinition2);

    Map<String, Long> previousHighWatermarks = new HashMap<>();
    source.generateWorkUnits(definitions, previousHighWatermarks);
  }

  @Test
  public void testCheckFullExtractState() throws Exception {
    State state = new SourceState();
    Map map = Mockito.mock(Map.class);
    Method method = MultistageSource.class.getDeclaredMethod("checkFullExtractState", State.class, Map.class);
    method.setAccessible(true);
    state.setProp("extract.table.type", "APPEND_ONLY");
    when(map.isEmpty()).thenReturn(true);
    Assert.assertTrue((Boolean) method.invoke(source, state, map));

    when(map.isEmpty()).thenReturn(false);
    Assert.assertFalse((Boolean) method.invoke(source, state, map));

    state.setProp("ms.enable.dynamic.full.load", "true");
    Assert.assertFalse((Boolean) method.invoke(source, state, map));
  }

  @Test
  public void testGetPreviousHighWatermarks() throws Exception {
    SourceState sourceState = Mockito.mock(SourceState.class);
    WorkUnitState workUnitState = Mockito.mock(WorkUnitState.class);
    source.sourceState = sourceState;

    Map<String, Iterable<WorkUnitState>> previousWorkUnitStatesByDatasetUrns = new HashMap<>();
    previousWorkUnitStatesByDatasetUrns.put("ColumnName.Number", ImmutableList.of(workUnitState));
    when(workUnitState.getActualHighWatermark(LongWatermark.class)).thenReturn(new LongWatermark(1000L));
    when(sourceState.getPreviousWorkUnitStatesByDatasetUrns()).thenReturn(previousWorkUnitStatesByDatasetUrns);

    Method method = MultistageSource.class.getDeclaredMethod("getPreviousHighWatermarks");
    method.setAccessible(true);
    Map<String, Long> actual = (Map) method.invoke(source);
    Assert.assertEquals(actual.size(), 1);
    Assert.assertEquals((long) actual.get("ColumnName.Number"), 1000L);
  }

  /**
   * Test normal cases
   */
  @Test
  public void testGetWorkUnitPartitionTypes() {
    SourceState state = new SourceState();
    source = new MultistageSource();

    state.setProp("ms.work.unit.partition", "");
    source.initialize(state);
    Assert.assertEquals(source.jobKeys.getWorkUnitPartitionType(), WorkUnitPartitionTypes.NONE);

    state.setProp("ms.work.unit.partition", "none");
    source.initialize(state);
    Assert.assertEquals(source.jobKeys.getWorkUnitPartitionType(), WorkUnitPartitionTypes.NONE);

    state.setProp("ms.work.unit.partition", "weekly");
    source.initialize(state);
    Assert.assertEquals(source.jobKeys.getWorkUnitPartitionType(), WorkUnitPartitionTypes.WEEKLY);

    state.setProp("ms.work.unit.partition", "monthly");
    source.initialize(state);
    Assert.assertEquals(source.jobKeys.getWorkUnitPartitionType(), WorkUnitPartitionTypes.MONTHLY);

    state.setProp("ms.work.unit.partition", "daily");
    source.initialize(state);
    Assert.assertEquals(source.jobKeys.getWorkUnitPartitionType(), WorkUnitPartitionTypes.DAILY);

    state.setProp("ms.work.unit.partition", "hourly");
    source.initialize(state);
    Assert.assertEquals(source.jobKeys.getWorkUnitPartitionType(), WorkUnitPartitionTypes.HOURLY);

    state.setProp("ms.work.unit.partition", "{\"none\": [\"2020-01-01\", \"2020-02-18\"]}");
    state.setProp("ms.work.unit.partial.partition", false);
    source.initialize(state);
    Assert.assertEquals(source.jobKeys.getWorkUnitPartitionType(), WorkUnitPartitionTypes.COMPOSITE);
    Assert.assertEquals(source.jobKeys.getWorkUnitPartitionType().getRanges(
        DateTime.parse("2020-01-01"),
        DateTime.parse("2020-02-18"),
        source.jobKeys.getIsPartialPartition()).size(), 1);

    state.setProp("ms.work.unit.partition", "{\"monthly\": [\"2020-01-01\", \"2020-02-18\"]}");
    state.setProp("ms.work.unit.partial.partition", false);
    source.initialize(state);
    Assert.assertEquals(source.jobKeys.getWorkUnitPartitionType(), WorkUnitPartitionTypes.COMPOSITE);
    Assert.assertEquals(source.jobKeys.getWorkUnitPartitionType().getRanges(
        DateTime.parse("2020-01-01"),
        DateTime.parse("2020-02-18"),
        source.jobKeys.getIsPartialPartition()).size(), 1);

    state.setProp("ms.work.unit.partition", "{\"monthly\": [\"2020-01-01\", \"2020-02-18\"]}");
    state.setProp("ms.work.unit.partial.partition", true);
    source.initialize(state);
    Assert.assertEquals(source.jobKeys.getWorkUnitPartitionType(), WorkUnitPartitionTypes.COMPOSITE);
    Assert.assertEquals(source.jobKeys.getWorkUnitPartitionType().getRanges(
        DateTime.parse("2020-01-01"),
        DateTime.parse("2020-02-18"),
        source.jobKeys.getIsPartialPartition()).size(), 2);

    state.setProp("ms.work.unit.partition", "{\"weekly\": [\"2020-01-01\", \"2020-02-01\"]}");
    state.setProp("ms.work.unit.partial.partition", true);
    source.initialize(state);
    Assert.assertEquals(source.jobKeys.getWorkUnitPartitionType(), WorkUnitPartitionTypes.COMPOSITE);
    Assert.assertEquals(source.jobKeys.getWorkUnitPartitionType().getRanges(
        DateTime.parse("2020-01-01"),
        DateTime.parse("2020-02-01"),
        source.jobKeys.getIsPartialPartition()).size(), 5);

    // this should gives out 3 ranges: 1/1 - 2/1, 2/1 - 2/2, 2/2 - 2/3
    state.setProp("ms.work.unit.partition", "{\"monthly\": [\"2020-01-01T00:00:00-00:00\", \"2020-02-01T00:00:00-00:00\"], \"daily\": [\"2020-02-01T00:00:00-00:00\", \"2020-02-03T00:00:00-00:00\"]}");
    state.setProp("ms.work.unit.partial.partition", false);
    source.initialize(state);
    Assert.assertEquals(source.jobKeys.getWorkUnitPartitionType(), WorkUnitPartitionTypes.COMPOSITE);
    Assert.assertEquals(source.jobKeys.getWorkUnitPartitionType().getRanges(
        DateTime.parse("2020-01-01T00:00:00-00:00").withZone(DateTimeZone.UTC),
        DateTime.parse("2020-02-03T00:00:00-00:00").withZone(DateTimeZone.UTC),
        source.jobKeys.getIsPartialPartition()).size(), 3);


    // this should gives out 3 ranges: 1/1 - 2/1, 2/1 - 2/2, 2/2 - 2/3
    state.setProp("ms.work.unit.partition", "{\"none\": [\"2010-01-01T00:00:00-00:00\", \"2020-02-01T00:00:00-00:00\"], \"daily\": [\"2020-02-01T00:00:00-00:00\", \"2020-02-03T00:00:00-00:00\"]}");
    state.setProp("ms.work.unit.partial.partition", false);
    source.initialize(state);
    Assert.assertEquals(source.jobKeys.getWorkUnitPartitionType(), WorkUnitPartitionTypes.COMPOSITE);
    Assert.assertEquals(source.jobKeys.getWorkUnitPartitionType().getRanges(
        DateTime.parse("2010-01-01T00:00:00-00:00").withZone(DateTimeZone.UTC),
        DateTime.parse("2020-02-03T00:00:00-00:00").withZone(DateTimeZone.UTC),
        source.jobKeys.getIsPartialPartition()).size(), 3);

    state.setProp("ms.work.unit.partition", "{\"monthly\": [\"2020-01-01\", \"-\"]}");
    state.setProp("ms.work.unit.partial.partition", true);
    source.initialize(state);
    Assert.assertEquals(source.jobKeys.getWorkUnitPartitionType(), WorkUnitPartitionTypes.COMPOSITE);
    Assert.assertEquals(source.jobKeys.getWorkUnitPartitionType().getRanges(
        DateTime.now().monthOfYear().roundFloorCopy(),
        DateTime.now().monthOfYear().roundCeilingCopy(),
        source.jobKeys.getIsPartialPartition()).size(), 1);
  }

  @Test
  public void testGetWorkUnitRangesForYearlyWithPartialPartitioning() {
    SourceState state = new SourceState();
    source = new MultistageSource();
    List<ImmutablePair<Long, Long>> actualRanges;
    List<ImmutablePair<Long, Long>> expectedRanges;
    state.setProp("ms.work.unit.partial.partition", true);

    // evenly partitioned, daily
    // Expected: 2017, 2018, 2019, Each day of 2020
    state.setProp("ms.work.unit.partition", "{\"yearly\": [\"2017-01-01T00:00:00-00:00\", \"2020-01-01T00:00:00-00:00\"], "
        + "\"daily\": [\"2020-01-01T00:00:00-00:00\", \"2020-02-03T00:00:00-00:00\"]}");
    source.initialize(state);
    actualRanges = source.jobKeys.getWorkUnitPartitionType().getRanges(
        DateTime.parse("2017-01-01T00:00:00-00:00").withZone(DateTimeZone.UTC),
        DateTime.parse("2020-02-03T00:00:00-00:00").withZone(DateTimeZone.UTC),
        source.jobKeys.getIsPartialPartition());
    Assert.assertEquals(source.jobKeys.getWorkUnitPartitionType(), WorkUnitPartitionTypes.COMPOSITE);
    Assert.assertEquals(actualRanges.size(), 36);

    // evenly partitioned, weekly
    // Expected: 2017, 2018, 2019, first 5 weeks of 2020
    state.setProp("ms.work.unit.partition", "{\"yearly\": [\"2017-01-01T00:00:00-00:00\", \"2020-01-01T00:00:00-00:00\"], "
        + "\"weekly\": [\"2020-01-01T00:00:00-00:00\", \"2020-02-03T00:00:00-00:00\"]}");
    source.initialize(state);
    actualRanges = source.jobKeys.getWorkUnitPartitionType().getRanges(
        DateTime.parse("2017-01-01T00:00:00-00:00").withZone(DateTimeZone.UTC),
        DateTime.parse("2020-02-03T00:00:00-00:00").withZone(DateTimeZone.UTC),
        source.jobKeys.getIsPartialPartition());
    expectedRanges = Arrays.asList(ImmutablePair.of(1483228800000l, 1514764800000l),
        ImmutablePair.of(1514764800000l, 1546300800000l),
        ImmutablePair.of(1546300800000l, 1577836800000l),
        ImmutablePair.of(1577836800000l, 1578441600000l),
        ImmutablePair.of(1578441600000l, 1579046400000l),
        ImmutablePair.of(1579046400000l, 1579651200000l),
        ImmutablePair.of(1579651200000l, 1580256000000l),
        ImmutablePair.of(1580256000000l, 1580688000000l));
    Assert.assertEquals(source.jobKeys.getWorkUnitPartitionType(), WorkUnitPartitionTypes.COMPOSITE);
    Assert.assertEquals(actualRanges.size(), 8);
    Assert.assertEquals(actualRanges, expectedRanges);

    // not evenly partitioned, daily
    // Expected: 2017, 2018, 2019, Jan 2020, Each day of Feb 2020
    state.setProp("ms.work.unit.partition", "{\"yearly\": [\"2017-01-01T00:00:00-00:00\", \"2020-02-01T00:00:00-00:00\"], "
        + "\"daily\": [\"2020-02-01T00:00:00-00:00\", \"2020-02-03T00:00:00-00:00\"]}");
    source.initialize(state);
    actualRanges = source.jobKeys.getWorkUnitPartitionType().getRanges(
        DateTime.parse("2017-01-01T00:00:00-00:00").withZone(DateTimeZone.UTC),
        DateTime.parse("2020-02-03T00:00:00-00:00").withZone(DateTimeZone.UTC),
        source.jobKeys.getIsPartialPartition());
    expectedRanges = Arrays.asList(ImmutablePair.of(1483228800000l, 1514764800000l),
        ImmutablePair.of(1514764800000l, 1546300800000l),
        ImmutablePair.of(1546300800000l, 1577836800000l),
        ImmutablePair.of(1577836800000l, 1580515200000l),
        ImmutablePair.of(1580515200000l, 1580601600000l),
        ImmutablePair.of(1580601600000l, 1580688000000l));
    Assert.assertEquals(source.jobKeys.getWorkUnitPartitionType(), WorkUnitPartitionTypes.COMPOSITE);
    Assert.assertEquals(actualRanges.size(), 6);
    Assert.assertEquals(actualRanges, expectedRanges);

    // not evenly partitioned, weekly
    // config is provided out of order
    // Expected: 2017, 2018, 2019, Jan 2020, First week of Feb 2020, second week of feb 2020
    state.setProp("ms.work.unit.partition", "{\"weekly\": [\"2020-02-01T00:00:00-00:00\", \"2020-02-11T00:00:00-00:00\"], "
        + "\"yearly\": [\"2017-01-01T00:00:00-00:00\", \"2020-02-01T00:00:00-00:00\"]}");
    source.initialize(state);
    Assert.assertEquals(source.jobKeys.getWorkUnitPartitionType(), WorkUnitPartitionTypes.COMPOSITE);
    actualRanges = source.jobKeys.getWorkUnitPartitionType().getRanges(
        DateTime.parse("2017-01-01T00:00:00-00:00").withZone(DateTimeZone.UTC),
        DateTime.parse("2020-02-11T00:00:00-00:00").withZone(DateTimeZone.UTC),
        source.jobKeys.getIsPartialPartition());
    expectedRanges = Arrays.asList(ImmutablePair.of(1483228800000l, 1514764800000l),
        ImmutablePair.of(1514764800000l, 1546300800000l),
        ImmutablePair.of(1546300800000l, 1577836800000l),
        ImmutablePair.of(1577836800000l, 1580515200000l),
        ImmutablePair.of(1580515200000l, 1581120000000l),
        ImmutablePair.of(1581120000000l, 1581379200000l));
    Assert.assertEquals(actualRanges.size(), 6);
    Assert.assertEquals(actualRanges, expectedRanges);
  }

  @Test
  public void testGetWorkUnitRangesForYearlyWithoutPartialPartitioning() {
    SourceState state = new SourceState();
    source = new MultistageSource();
    List<ImmutablePair<Long, Long>> actualRanges;
    List<ImmutablePair<Long, Long>> expectedRanges;
    state.setProp("ms.work.unit.partial.partition", false);

    // evenly partitioned, daily
    // Expected: 2017, 2018, 2019, Each day of 2020
    state.setProp("ms.work.unit.partition", "{\"yearly\": [\"2017-01-01T00:00:00-00:00\", \"2020-01-01T00:00:00-00:00\"], "
        + "\"daily\": [\"2020-01-01T00:00:00-00:00\", \"2020-02-03T00:00:00-00:00\"]}");
    source.initialize(state);
    actualRanges = source.jobKeys.getWorkUnitPartitionType().getRanges(
        DateTime.parse("2017-01-01T00:00:00-00:00").withZone(DateTimeZone.UTC),
        DateTime.parse("2020-02-03T00:00:00-00:00").withZone(DateTimeZone.UTC),
        source.jobKeys.getIsPartialPartition());
    Assert.assertEquals(source.jobKeys.getWorkUnitPartitionType(), WorkUnitPartitionTypes.COMPOSITE);
    Assert.assertEquals(actualRanges.size(), 36);

    // evenly partitioned, weekly
    // Expected: 2017, 2018, 2019, first 4 weeks of 2020
    state.setProp("ms.work.unit.partition", "{\"yearly\": [\"2017-01-01T00:00:00-00:00\", \"2020-01-01T00:00:00-00:00\"], "
        + "\"weekly\": [\"2020-01-01T00:00:00-00:00\", \"2020-02-03T00:00:00-00:00\"]}");
    source.initialize(state);
    actualRanges = source.jobKeys.getWorkUnitPartitionType().getRanges(
        DateTime.parse("2017-01-01T00:00:00-00:00").withZone(DateTimeZone.UTC),
        DateTime.parse("2020-02-03T00:00:00-00:00").withZone(DateTimeZone.UTC),
        source.jobKeys.getIsPartialPartition());
    expectedRanges = Arrays.asList(ImmutablePair.of(1483228800000l, 1514764800000l),
        ImmutablePair.of(1514764800000l, 1546300800000l),
        ImmutablePair.of(1546300800000l, 1577836800000l),
        ImmutablePair.of(1577836800000l, 1578441600000l),
        ImmutablePair.of(1578441600000l, 1579046400000l),
        ImmutablePair.of(1579046400000l, 1579651200000l),
        ImmutablePair.of(1579651200000l, 1580256000000l));
    Assert.assertEquals(source.jobKeys.getWorkUnitPartitionType(), WorkUnitPartitionTypes.COMPOSITE);
    Assert.assertEquals(actualRanges.size(), 7);
    Assert.assertEquals(actualRanges, expectedRanges);

    // not evenly partitioned, daily
    // Expected: 2017, 2018, 2019, Each day of Feb 2020
    state.setProp("ms.work.unit.partition", "{\"yearly\": [\"2017-01-01T00:00:00-00:00\", \"2020-02-01T00:00:00-00:00\"], "
        + "\"daily\": [\"2020-02-01T00:00:00-00:00\", \"2020-02-03T00:00:00-00:00\"]}");
    source.initialize(state);
    actualRanges = source.jobKeys.getWorkUnitPartitionType().getRanges(
        DateTime.parse("2017-01-01T00:00:00-00:00").withZone(DateTimeZone.UTC),
        DateTime.parse("2020-02-03T00:00:00-00:00").withZone(DateTimeZone.UTC),
        source.jobKeys.getIsPartialPartition());
    expectedRanges = Arrays.asList(ImmutablePair.of(1483228800000l, 1514764800000l),
        ImmutablePair.of(1514764800000l, 1546300800000l),
        ImmutablePair.of(1546300800000l, 1577836800000l),
        ImmutablePair.of(1580515200000l, 1580601600000l),
        ImmutablePair.of(1580601600000l, 1580688000000l));
    Assert.assertEquals(source.jobKeys.getWorkUnitPartitionType(), WorkUnitPartitionTypes.COMPOSITE);
    Assert.assertEquals(actualRanges.size(), 5);
    Assert.assertEquals(actualRanges, expectedRanges);

    // not evenly partitioned, weekly
    // Expected: 2017, 2018, 2019, First week of Feb 2020
    state.setProp("ms.work.unit.partition", "{\"yearly\": [\"2017-01-01T00:00:00-00:00\", \"2020-02-01T00:00:00-00:00\"], "
        + "\"weekly\": [\"2020-02-01T00:00:00-00:00\", \"2020-02-11T00:00:00-00:00\"]}");
    source.initialize(state);
    Assert.assertEquals(source.jobKeys.getWorkUnitPartitionType(), WorkUnitPartitionTypes.COMPOSITE);
    actualRanges = source.jobKeys.getWorkUnitPartitionType().getRanges(
        DateTime.parse("2017-01-01T00:00:00-00:00").withZone(DateTimeZone.UTC),
        DateTime.parse("2020-02-1T00:00:00-00:00").withZone(DateTimeZone.UTC),
        source.jobKeys.getIsPartialPartition());
    expectedRanges = Arrays.asList(ImmutablePair.of(1483228800000l, 1514764800000l),
        ImmutablePair.of(1514764800000l, 1546300800000l),
        ImmutablePair.of(1546300800000l, 1577836800000l),
        ImmutablePair.of(1580515200000l, 1581120000000l));
    Assert.assertEquals(actualRanges.size(), 4);
    Assert.assertEquals(actualRanges, expectedRanges);
  }

  @Test
  public void testGetWorkUnitRangesForYearlyWithOneSubRange() {
    SourceState state = new SourceState();
    source = new MultistageSource();
    List<ImmutablePair<Long, Long>> actualRanges;
    List<ImmutablePair<Long, Long>> expectedRanges;

    // Yearly without partial
    // Expected: 3 - 2017, 2018, 2019
    state.setProp("ms.work.unit.partition", "{\"yearly\": [\"2017-01-01\", \"2020-02-18\"]}");
    state.setProp("ms.work.unit.partial.partition", false);
    source.initialize(state);
    actualRanges = source.jobKeys.getWorkUnitPartitionType().getRanges(
        DateTime.parse("2017-01-01"),
        DateTime.parse("2020-02-18"),
        source.jobKeys.getIsPartialPartition());
    expectedRanges = Arrays.asList(ImmutablePair.of(1483257600000l, 1514793600000l),
        ImmutablePair.of(1514793600000l, 1546329600000l),
        ImmutablePair.of(1546329600000l, 1577865600000l));
    Assert.assertEquals(source.jobKeys.getWorkUnitPartitionType(), WorkUnitPartitionTypes.COMPOSITE);
    Assert.assertEquals(actualRanges.size(), 3);
    Assert.assertEquals(actualRanges, expectedRanges);

    // Yearly with partial
    // Expected: 3 - 2017, 2018, 2019, partial 2018
    state.setProp("ms.work.unit.partition", "{\"yearly\": [\"2017-01-01\", \"2020-02-18\"]}");
    state.setProp("ms.work.unit.partial.partition", true);
    source.initialize(state);
    actualRanges = source.jobKeys.getWorkUnitPartitionType().getRanges(
        DateTime.parse("2017-01-01"),
        DateTime.parse("2020-02-18"),
        source.jobKeys.getIsPartialPartition());
    expectedRanges = Arrays.asList(ImmutablePair.of(1483257600000l, 1514793600000l),
        ImmutablePair.of(1514793600000l, 1546329600000l),
        ImmutablePair.of(1546329600000l, 1577865600000l),
        ImmutablePair.of(1577865600000l, 1582012800000l));
    Assert.assertEquals(source.jobKeys.getWorkUnitPartitionType(), WorkUnitPartitionTypes.COMPOSITE);
    Assert.assertEquals(actualRanges.size(), 4);
    Assert.assertEquals(actualRanges, expectedRanges);
  }

  /**
   * test incorrect Json format
   */
  @Test
  public void testGetWorkUnitPartitionTypesWithExceptions1() {
    SourceState state = new SourceState();
    MultistageSource source = new MultistageSource();

    state.setProp("ms.work.unit.partition", "{\"monthly\": \"2020-01-01\"]}");
    state.setProp("ms.work.unit.partial.partition", true);
    source.initialize(state);
    Assert.assertEquals(source.jobKeys.getWorkUnitPartitionType(), null);
  }

  /**
   * test nonconforming property format
   */
  @Test
  public void testGetWorkUnitPartitionTypesWithExceptions2() {
    SourceState state = new SourceState();
    MultistageSource source = new MultistageSource();

    // in this case, the partition range is ignored, and there is no partitioning
    state.setProp("ms.work.unit.partition", "{\"monthly\": [\"2020-01-01\"]}");
    state.setProp("ms.work.unit.partial.partition", false);
    source.initialize(state);
    Assert.assertEquals(source.jobKeys.getWorkUnitPartitionType(), WorkUnitPartitionTypes.COMPOSITE);
    Assert.assertEquals(source.jobKeys.getWorkUnitPartitionType().getRanges(
        DateTime.now().monthOfYear().roundFloorCopy(),
        DateTime.now().monthOfYear().roundCeilingCopy(),
        source.jobKeys.getIsPartialPartition()).size(), 0);

    // supposedly we wanted 5 weekly partitions, but the range end date time format is incorrect
    // therefore it will not generate the number of partitions as wanted
    state.setProp("ms.work.unit.partition", "{\"weekly\": [\"2020-01-01\", \"2020-02-1\"]}");
    state.setProp("ms.work.unit.partial.partition", true);
    source.initialize(state);
    Assert.assertEquals(source.jobKeys.getWorkUnitPartitionType(), WorkUnitPartitionTypes.COMPOSITE);
    Assert.assertNotEquals(source.jobKeys.getWorkUnitPartitionType().getRanges(
        DateTime.parse("2001-01-01"),
        DateTime.parse("2090-01-01"),
        source.jobKeys.getIsPartialPartition()).size(), 5);
  }

  @Test
  public void testPassingSchema2WorkUnits() {
    SourceState state = new SourceState();
    String urn = "urn:li:dataset:(urn:li:dataPlatform:hive,rightnow.incidents,PROD)";
    JsonArray sampleSchema = gson.fromJson(
        "[{\"columnName\":\"column1\",\"isNullable\":true,\"dataType\":{\"type\":\"timestamp\"}}]",
        JsonArray.class);

    state.setProp(MSTAGE_SOURCE_SCHEMA_URN.toString(), urn);
    MultistageSource<?, ?> source = new MultistageSource<>();

    SchemaReader mockFactory = mock(SchemaReader.class);
    when(mockFactory.read(Matchers.any(), Matchers.any())).thenReturn(sampleSchema);

    source.setSourceState(state);
    source.jobKeys.setSchemaReader(mockFactory);
    source.jobKeys.initialize(state);
    Assert.assertTrue(source.jobKeys.hasOutputSchema());
    Assert.assertNotNull(source.generateWorkUnits(new ArrayList<>(), new HashMap<>()));
  }

  @Test
  public void testAvoidWatermarkGoingBeyondLeftBoundary() {
    // state and source: define 2 day grace period
    SourceState state = new SourceState();
    state.setProp("ms.grace.period.days", "2");
    MultistageSource<?, ?> source = new MultistageSource<>();
    source.setSourceState(state);
    source.jobKeys.initialize(state);

    // watermark definition: define from and to date watermark
    String jsonDef = "[{\"name\": \"system\",\"type\": \"datetime\", \"range\": {\"from\": \"2021-06-18\", \"to\": \"2021-06-19\"}}]";
    Gson gson = new Gson();
    JsonArray defArray = gson.fromJson(jsonDef, JsonArray.class);
    WatermarkDefinition watermarkDefinition = new WatermarkDefinition(defArray.get(0).getAsJsonObject(),
        false, WorkUnitPartitionTypes.DAILY);
    List<WatermarkDefinition> definitions = ImmutableList.of(watermarkDefinition);

    // previous highwatermarks: simulate state-store entry
    Map<String, Long> previousHighWatermarks = Mockito.mock(HashMap.class);
    when(previousHighWatermarks.containsKey(any())).thenReturn(true);
    when(previousHighWatermarks.get(any())).thenReturn(
        DTF_PST_TIMEZONE.parseDateTime("2021-06-19T00:00:00").getMillis());

    // expected workunits
    WorkUnit expectedWorkUnit = WorkUnit.create(null,
        new WatermarkInterval(
            new LongWatermark(DTF_PST_TIMEZONE.parseDateTime("2021-06-18T00:00:00").getMillis()),
            new LongWatermark(DTF_PST_TIMEZONE.parseDateTime("2021-06-19T00:00:00").getMillis())));

    List<WorkUnit> actualWorkUnits = source.generateWorkUnits(definitions, previousHighWatermarks);
    Assert.assertEquals(actualWorkUnits.size(), 1);
    Assert.assertEquals(actualWorkUnits.get(0).getLowWatermark(), expectedWorkUnit.getLowWatermark());
    Assert.assertEquals(actualWorkUnits.get(0).getExpectedHighWatermark(), expectedWorkUnit.getExpectedHighWatermark());
  }

  @Test
  public void testRemoveNoRangeWorkUnitEnabled() {
    SourceState state = new SourceState();
    state.setProp("ms.aux.keys", "{\"cleanseNoRangeWorkUnit\": true}");
    MultistageSource<?, ?> source = new MultistageSource<>();
    source.setSourceState(state);
    source.jobKeys.initialize(state);

    // watermark definition: define from and to date watermark
    String jsonDef = "[{\"name\": \"system\",\"type\": \"datetime\", \"range\": {\"from\": \"2021-06-18\", \"to\": \"2021-06-19\"}}]";
    Gson gson = new Gson();
    JsonArray defArray = gson.fromJson(jsonDef, JsonArray.class);
    WatermarkDefinition watermarkDefinition = new WatermarkDefinition(defArray.get(0).getAsJsonObject(),
        false, WorkUnitPartitionTypes.DAILY);
    List<WatermarkDefinition> definitions = ImmutableList.of(watermarkDefinition);

    // previous highwatermarks: simulate state-store entry
    Map<String, Long> previousHighWatermarks = Mockito.mock(HashMap.class);
    when(previousHighWatermarks.containsKey(any())).thenReturn(true);
    when(previousHighWatermarks.get(any())).thenReturn(
        DTF_PST_TIMEZONE.parseDateTime("2021-06-19T00:00:00").getMillis());

    List<WorkUnit> actualWorkUnits = source.generateWorkUnits(definitions, previousHighWatermarks);
    // expected result should not contain any no range work units
    Assert.assertEquals(actualWorkUnits.size(), 0);
  }

  @Test
  public void testRemoveNoRangeWorkUnitDisabled() {
    SourceState state = new SourceState();
    state.setProp("ms.aux.keys", "{\"cleanseNoRangeWorkUnit\": false}");
    MultistageSource<?, ?> source = new MultistageSource<>();
    source.setSourceState(state);
    source.jobKeys.initialize(state);

    // watermark definition: define from and to date watermark
    String jsonDef = "[{\"name\": \"system\",\"type\": \"datetime\", \"range\": {\"from\": \"2021-06-18\", \"to\": \"2021-06-19\"}}]";
    Gson gson = new Gson();
    JsonArray defArray = gson.fromJson(jsonDef, JsonArray.class);
    WatermarkDefinition watermarkDefinition = new WatermarkDefinition(defArray.get(0).getAsJsonObject(),
        false, WorkUnitPartitionTypes.DAILY);
    List<WatermarkDefinition> definitions = ImmutableList.of(watermarkDefinition);

    // previous highwatermarks: simulate state-store entry
    Map<String, Long> previousHighWatermarks = Mockito.mock(HashMap.class);
    when(previousHighWatermarks.containsKey(any())).thenReturn(true);
    when(previousHighWatermarks.get(any())).thenReturn(
        DTF_PST_TIMEZONE.parseDateTime("2021-06-19T00:00:00").getMillis());

    List<WorkUnit> actualWorkUnits = source.generateWorkUnits(definitions, previousHighWatermarks);
    // expected result contains no range work units
    WorkUnit expectedWorkUnit = WorkUnit.create(null,
        new WatermarkInterval(
            new LongWatermark(DTF_PST_TIMEZONE.parseDateTime("2021-06-19T00:00:00").getMillis()),
            new LongWatermark(DTF_PST_TIMEZONE.parseDateTime("2021-06-19T00:00:00").getMillis())));

    Assert.assertEquals(actualWorkUnits.size(), 1);
    Assert.assertEquals(actualWorkUnits.get(0).getLowWatermark(), expectedWorkUnit.getLowWatermark());
    Assert.assertEquals(actualWorkUnits.get(0).getExpectedHighWatermark(), expectedWorkUnit.getExpectedHighWatermark());
  }
}

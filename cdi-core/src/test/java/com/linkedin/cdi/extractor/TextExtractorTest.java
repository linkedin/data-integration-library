// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.extractor;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.linkedin.cdi.connection.MultistageConnection;
import com.linkedin.cdi.exception.RetriableAuthenticationException;
import com.linkedin.cdi.keys.JobKeys;
import com.linkedin.cdi.keys.JsonExtractorKeys;
import com.linkedin.cdi.source.MultistageSource;
import com.linkedin.cdi.util.WorkUnitStatus;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang.StringUtils;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.cdi.configuration.PropertyCollection.*;
import static org.mockito.Mockito.*;


@Test
public class TextExtractorTest {

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
  private TextExtractor textDumpJsonExtractor;
  private MultistageConnection multistageConnection;

  @BeforeMethod
  public void setUp() throws RetriableAuthenticationException {
    gson = new Gson();
    source = mock(MultistageSource.class);
    jobKeys = mock(JobKeys.class);

    SourceState sourceState = new SourceState();
    sourceState.setProp("extract.table.name", "xxx");
    List<WorkUnit> wus = new MultistageSource().getWorkunits(sourceState);
    workUnit = wus.get(0);

    workUnitStatus = mock(WorkUnitStatus.class);
    state = mock(WorkUnitState.class);
    when(state.getProp(MSTAGE_ACTIVATION_PROPERTY.getConfig(), new JsonObject().toString())).thenReturn(ACTIVATION_PROP);
    when(state.getPropAsLong(MSTAGE_WORK_UNIT_SCHEDULING_STARTTIME.getConfig(), 0L)).thenReturn(WORKUNIT_STARTTIME_KEY);
    when(state.getWorkunit()).thenReturn(workUnit);
    workUnit.setProp(DATASET_URN.getConfig(), DATA_SET_URN_KEY);
    when(source.getJobKeys()).thenReturn(jobKeys);
    when(jobKeys.getPaginationInitValues()).thenReturn(new HashMap<>());
    when(jobKeys.getSchemaCleansingPattern()).thenReturn("(\\s|\\$|@)");
    when(jobKeys.getSchemaCleansingReplacement()).thenReturn("_");
    when(jobKeys.getSchemaCleansingNullable()).thenReturn(false);
    when(jobKeys.getSessionInitialValue()).thenReturn(Optional.empty());
    jsonExtractorKeys = Mockito.mock(JsonExtractorKeys.class);
    textDumpJsonExtractor = new TextExtractor(state, source.getJobKeys());
    textDumpJsonExtractor.setJsonExtractorKeys(jsonExtractorKeys);
    textDumpJsonExtractor.jobKeys = jobKeys;

    multistageConnection = mock(MultistageConnection.class);
    textDumpJsonExtractor.setConnection(multistageConnection);
    when(multistageConnection.executeFirst(textDumpJsonExtractor.workUnitStatus)).thenReturn(workUnitStatus);
    when(multistageConnection.executeNext(textDumpJsonExtractor.workUnitStatus)).thenReturn(workUnitStatus);
  }

  @Test
  public void testReadRecord() throws RetriableAuthenticationException {
    when(jobKeys.getTotalCountField()).thenReturn(StringUtils.EMPTY);
    when(workUnitStatus.getMessages()).thenReturn(ImmutableMap.of("contentType", "application/json"));
    when(jsonExtractorKeys.getActivationParameters()).thenReturn(new JsonObject());
    when(jsonExtractorKeys.getJsonElementIterator()).thenReturn(null);
    when(jsonExtractorKeys.getPayloads()).thenReturn(new JsonArray());


    String outputText = "Text Dump Extractor";
    String outputJson = "{\"output\":\"" + outputText + "\"}";
    InputStream stream = new ByteArrayInputStream(outputText.getBytes());
    when(workUnitStatus.getBuffer()).thenReturn(stream);
    when(jobKeys.getDataField()).thenReturn(StringUtils.EMPTY);
    when(jobKeys.getSessionKeyField()).thenReturn(new JsonObject());
    Assert.assertEquals(textDumpJsonExtractor.readRecord(new JsonObject()).toString(), outputJson);

    outputText = StringUtils.repeat("long-output-string", 100);
    outputJson = "{\"output\":\"" + outputText + "\"}";
    stream = new ByteArrayInputStream(outputText.getBytes());
    when(workUnitStatus.getBuffer()).thenReturn(stream);
    when(jobKeys.getDataField()).thenReturn(StringUtils.EMPTY);
    when(jobKeys.getSessionKeyField()).thenReturn(new JsonObject());
    Assert.assertEquals(textDumpJsonExtractor.readRecord(new JsonObject()).toString(), outputJson);
  }

}

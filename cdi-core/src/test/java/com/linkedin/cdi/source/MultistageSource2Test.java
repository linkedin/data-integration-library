// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.source;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.linkedin.cdi.util.VariableUtils;
import java.io.InputStreamReader;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.cdi.configuration.MultistageProperties.*;
import static org.mockito.Mockito.*;


@PrepareForTest({VariableUtils.class, MultistageSource.class})
public class MultistageSource2Test extends PowerMockTestCase {

  private MultistageSource source;
  private SourceState state;
  private Gson gson;
  @BeforeClass
  public void setUp() {
    source = new MultistageSource();
    state = mock(SourceState.class);
    gson = new Gson();
  }

  @Test
  public void testGetWorkunits() {
    initializeHelper(state);

    List<WorkUnit> wuList = source.getWorkunits(state);
    Assert.assertEquals(wuList.size(), 1);
    WorkUnit workUnit = wuList.get(0);
    Assert.assertEquals(workUnit.getSpecProperties().getProperty(MSTAGE_WATERMARK_GROUPS.getConfig()), "[\"watermark.system\",\"watermark.unit\"]");
  }

  @Test
  public void testInitialize() {
    initializeHelper(state);

    when(state.getProp(MSTAGE_ENABLE_CLEANSING.getConfig(), StringUtils.EMPTY)).thenReturn("true");
    when(state.getProp(MSTAGE_SECONDARY_INPUT.getConfig(), new JsonArray().toString()))
        .thenReturn("[{\"fields\":[\"uuid\"],\"category\":\"authentication\",\"authentication\":{}}]");
    source.initialize(state);

    when(state.getProp(MSTAGE_ENABLE_CLEANSING.getConfig(), StringUtils.EMPTY)).thenReturn("");
    when(state.getProp(MSTAGE_SECONDARY_INPUT.getConfig(), new JsonArray().toString()))
        .thenReturn("[{\"path\":\"${job.dir}/${extract.namespace}/getResults\",\"fields\":[\"access_token\"],\"category\":\"authentication\",\"retry\":{}}]");
    source.initialize(state);

    when(state.getProp(MSTAGE_ENABLE_CLEANSING.getConfig(), StringUtils.EMPTY)).thenReturn("false");
    source.initialize(state);
  }

  private void initializeHelper(SourceState state) {
    JsonObject allKeys = gson.fromJson(new InputStreamReader(this.getClass().getResourceAsStream("/json/sample-data-for-source.json")), JsonObject.class);

    when(state.getProp(MSTAGE_PAGINATION.getConfig(), new JsonObject().toString())).thenReturn(allKeys.get(MSTAGE_PAGINATION.getConfig()).getAsJsonObject().toString());
    when(state.getProp(MSTAGE_SESSION_KEY_FIELD.getConfig(), new JsonObject().toString())).thenReturn(allKeys.get(MSTAGE_SESSION_KEY_FIELD.getConfig()).getAsJsonObject().toString());
    when(state.getProp(MSTAGE_TOTAL_COUNT_FIELD.getConfig(), StringUtils.EMPTY)).thenReturn(allKeys.get(MSTAGE_TOTAL_COUNT_FIELD.getConfig()).getAsString());
    when(state.getProp(MSTAGE_PARAMETERS.getConfig(), new JsonArray().toString())).thenReturn(allKeys.get(MSTAGE_PARAMETERS.getConfig()).getAsJsonArray().toString());
    when(state.getProp(MSTAGE_ENCRYPTION_FIELDS.getConfig(), new JsonArray().toString())).thenReturn(allKeys.get(MSTAGE_ENCRYPTION_FIELDS.getConfig()).getAsJsonArray().toString());
    when(state.getProp(MSTAGE_DATA_FIELD.getConfig(), StringUtils.EMPTY)).thenReturn(allKeys.get(MSTAGE_DATA_FIELD.getConfig()).getAsString());
    when(state.getPropAsLong(MSTAGE_CALL_INTERVAL.getConfig(), 0L)).thenReturn(allKeys.get(MSTAGE_CALL_INTERVAL.getConfig()).getAsLong());
    when(state.getPropAsLong(MSTAGE_WAIT_TIMEOUT_SECONDS.getConfig(), 0L)).thenReturn(allKeys.get(MSTAGE_WAIT_TIMEOUT_SECONDS.getConfig()).getAsLong());
    when(state.getPropAsBoolean(MSTAGE_ENABLE_CLEANSING.getConfig())).thenReturn(allKeys.get(MSTAGE_ENABLE_CLEANSING.getConfig()).getAsBoolean());
    when(state.getPropAsBoolean(MSTAGE_WORK_UNIT_PARTIAL_PARTITION.getConfig())).thenReturn(allKeys.get(MSTAGE_WORK_UNIT_PARTIAL_PARTITION.getConfig()).getAsBoolean());
    when(state.getProp(MSTAGE_WATERMARK.getConfig(), new JsonArray().toString())).thenReturn(allKeys.get(MSTAGE_WATERMARK.getConfig()).getAsJsonArray().toString());
    when(state.getProp(MSTAGE_SECONDARY_INPUT.getConfig(), new JsonArray().toString())).thenReturn(allKeys.get(MSTAGE_SECONDARY_INPUT.getConfig()).getAsJsonArray().toString());
    when(state.getProp(MSTAGE_CONNECTION_CLIENT_FACTORY.getConfig(), StringUtils.EMPTY)).thenReturn(allKeys.get(MSTAGE_CONNECTION_CLIENT_FACTORY.getConfig()).getAsString());
    when(state.getProp(MSTAGE_HTTP_REQUEST_HEADERS.getConfig(), new JsonObject().toString())).thenReturn(allKeys.get(MSTAGE_HTTP_REQUEST_HEADERS.getConfig()).getAsJsonObject().toString());
    when(state.getProp(MSTAGE_SOURCE_URI.getConfig(), StringUtils.EMPTY)).thenReturn(allKeys.get(MSTAGE_SOURCE_URI.getConfig()).getAsString());
    when(state.getProp(MSTAGE_HTTP_REQUEST_METHOD.getConfig(), StringUtils.EMPTY)).thenReturn(allKeys.get(MSTAGE_HTTP_REQUEST_METHOD.getConfig()).getAsString());
    when(state.getProp(MSTAGE_EXTRACTOR_CLASS.getConfig(), StringUtils.EMPTY)).thenReturn(allKeys.get(MSTAGE_EXTRACTOR_CLASS.getConfig()).getAsString());
    when(state.getProp(MSTAGE_AUTHENTICATION.getConfig(), new JsonObject().toString())).thenReturn(allKeys.get(MSTAGE_AUTHENTICATION.getConfig()).getAsJsonObject().toString());
    when(state.getProp(MSTAGE_HTTP_STATUSES.getConfig(), new JsonObject().toString())).thenReturn(allKeys.get(MSTAGE_HTTP_STATUSES.getConfig()).getAsJsonObject().toString());
    when(state.getProp(MSTAGE_HTTP_STATUS_REASONS.getConfig(), new JsonObject().toString())).thenReturn(allKeys.get(MSTAGE_HTTP_STATUS_REASONS.getConfig()).getAsJsonObject().toString());

    when(state.getProp(MSTAGE_SOURCE_S3_PARAMETERS.getConfig(), new JsonObject().toString())).thenReturn("{\"region\" : \"us-east-1\", \"connection_timeout\" : 10}");
    when(state.getProp(MSTAGE_SOURCE_FILES_PATTERN.getConfig(), StringUtils.EMPTY)).thenReturn(StringUtils.EMPTY);
    when(state.getPropAsInt(MSTAGE_S3_LIST_MAX_KEYS.getConfig())).thenReturn(100);
    when(state.getProp(SOURCE_CONN_USERNAME.getConfig(), StringUtils.EMPTY)).thenReturn(StringUtils.EMPTY);
    when(state.getProp(SOURCE_CONN_PASSWORD.getConfig(), StringUtils.EMPTY)).thenReturn(StringUtils.EMPTY);
    when(state.getProp(MSTAGE_EXTRACTOR_TARGET_FILE_NAME.getConfig(), StringUtils.EMPTY)).thenReturn(StringUtils.EMPTY);
    when(state.getProp(MSTAGE_OUTPUT_SCHEMA.getConfig(), StringUtils.EMPTY)).thenReturn("");
  }
}
// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.extractor;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.linkedin.cdi.connection.MultistageConnection;
import com.linkedin.cdi.exception.RetriableAuthenticationException;
import com.linkedin.cdi.keys.FileDumpExtractorKeys;
import com.linkedin.cdi.keys.JobKeys;
import com.linkedin.cdi.source.MultistageSource;
import com.linkedin.cdi.util.VariableUtils;
import com.linkedin.cdi.util.WorkUnitStatus;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.cdi.configuration.MultistageProperties.*;
import static org.mockito.Mockito.*;


/**
 * Test FileDumpExtractor under following scenarios:
 *
 * Scenario 1: download a file and save to /tmp
 */
@PrepareForTest({VariableUtils.class, FileSystem.class})
public class FileDumpExtractorTest2 extends PowerMockTestCase {

  private final static String DATA_SET_URN_KEY = "com.linkedin.SeriesCollection";
  private final static String ACTIVATION_PROP = "{\"name\": \"survey\", \"type\": \"unit\", \"units\": \"id1,id2\"}";
  private final static String DATA_FINAL_DIR = "/jobs/testUser/gobblin/useCaseRoot";
  private final static String FILE_PERMISSION = "775";
  private final static long WORK_UNIT_START_TIME_KEY = 1590994800000L;

  private WorkUnitState state;
  private MultistageSource source;
  private WorkUnit workUnit;
  private FileDumpExtractorKeys fileDumpExtractorKeys;
  private WorkUnitStatus workUnitStatus;
  private JobKeys jobKeys;
  private FileDumpExtractor fileDumpExtractor;
  private MultistageConnection multistageConnection;

  @BeforeMethod
  public void setUp() {
    state = Mockito.mock(WorkUnitState.class);
    source = Mockito.mock(MultistageSource.class);

    List<WorkUnit> wus = new MultistageSource().getWorkunits(new SourceState());
    workUnit = wus.get(0);
    workUnit.setProp(DATASET_URN_KEY.getConfig(), DATA_SET_URN_KEY);

    fileDumpExtractorKeys = Mockito.mock(FileDumpExtractorKeys.class);
    workUnitStatus = Mockito.mock(WorkUnitStatus.class);
    jobKeys = Mockito.mock(JobKeys.class);

    when(state.getProp(MSTAGE_ACTIVATION_PROPERTY.getConfig(), new JsonObject().toString())).thenReturn(ACTIVATION_PROP);
    when(state.getPropAsLong(MSTAGE_WORKUNIT_STARTTIME_KEY.getConfig(), 0L)).thenReturn(WORK_UNIT_START_TIME_KEY);
    when(state.getWorkunit()).thenReturn(workUnit);
    when(state.getProp(DATA_PUBLISHER_FINAL_DIR.getConfig(), StringUtils.EMPTY)).thenReturn(DATA_FINAL_DIR);
    when(state.getProp(MSTAGE_EXTRACTOR_TARGET_FILE_PERMISSION.getConfig(), StringUtils.EMPTY)).thenReturn(FILE_PERMISSION);

    fileDumpExtractor = new FileDumpExtractor(state, source.getJobKeys());
    fileDumpExtractor.setFileDumpExtractorKeys(fileDumpExtractorKeys);
    fileDumpExtractor.jobKeys = jobKeys;

    multistageConnection = Mockito.mock(MultistageConnection.class);
    fileDumpExtractor.setConnection(multistageConnection);
  }

  /**
   * Test FileDumpExtractor Constructor with a happy path
   */
  @Test
  public void testFileDumpExtractorConstructor() {
    FileDumpExtractor fileDumpExtractor = new FileDumpExtractor(state, source.getJobKeys());
    Assert.assertEquals(fileDumpExtractor.getFileDumpExtractorKeys().getFileName(), StringUtils.EMPTY);
    Assert.assertEquals(fileDumpExtractor.getFileDumpExtractorKeys().getFileWritePermissions(), FILE_PERMISSION);
    Assert.assertEquals(fileDumpExtractor.getFileDumpExtractorKeys().getFileDumpLocation(), DATA_FINAL_DIR);
    Assert.assertEquals(fileDumpExtractor.getFileDumpExtractorKeys().getCurrentFileNumber(), 0);
  }

  /**
   * Test FileDumpExtractor Constructor when a RuntimeException is thrown
   */
  @Test(expectedExceptions = RuntimeException.class)
  public void testFileDumpExtractorConstructorWithException() {
    doThrow(new RuntimeException()).when(state).getProp(DATA_PUBLISHER_FINAL_DIR.getConfig(), StringUtils.EMPTY);
    new FileDumpExtractor(state, source.getJobKeys());
  }

  /**
   * Test readRecord
   */
  @Test(expectedExceptions = RuntimeException.class)
  public void testReadRecord() throws IOException, RetriableAuthenticationException {
    when(jobKeys.getPaginationFields()).thenReturn(new HashMap<>());
    when(jobKeys.getPaginationInitValues()).thenReturn(new HashMap<>());
    when(jobKeys.isPaginationEnabled()).thenReturn(false);
    when(multistageConnection.executeNext(fileDumpExtractor.workUnitStatus)).thenReturn(workUnitStatus);
    when(workUnitStatus.getBuffer()).thenReturn(new ByteArrayInputStream("test_string".getBytes()));
    when(fileDumpExtractorKeys.getCurrentFileNumber()).thenReturn(Long.valueOf(10));
    when(fileDumpExtractorKeys.getFileName()).thenReturn("file_name");
    when(fileDumpExtractorKeys.getFileDumpLocation()).thenReturn("dir");
    when(fileDumpExtractorKeys.getFileWritePermissions()).thenReturn("775");
    when(fileDumpExtractorKeys.getCurrentFileNumber()).thenReturn(Long.valueOf(1));
    PowerMockito.mockStatic(FileSystem.class);
    FSDataOutputStream out = Mockito.mock(FSDataOutputStream.class);
    PowerMockito.when(FileSystem.create(any(), any(), any())).thenReturn(out);
    PowerMockito.doNothing().when(out).flush();
    PowerMockito.doNothing().when(out).close();

    Assert.assertNull(fileDumpExtractor.readRecord(""));

    when(jobKeys.isPaginationEnabled()).thenReturn(true);

    doThrow(new RuntimeException()).when(fileDumpExtractorKeys).incrCurrentFileNumber();
    fileDumpExtractor.readRecord("");
  }

  /**
   * Test processInputStream with two scenarios
   * 1: Happy path
   * 2: Invalid file name provided
   *
   * @throws IOException
   */
  @Test
  public void testProcessInputStream() throws RetriableAuthenticationException {
    // replace mocked source key with default source key
    fileDumpExtractor.jobKeys = new JobKeys();

    when(fileDumpExtractorKeys.getActivationParameters()).thenReturn(new JsonObject());
    when(fileDumpExtractorKeys.getPayloads()).thenReturn(new JsonArray());
    when(multistageConnection.executeNext(fileDumpExtractor.workUnitStatus)).thenReturn(null);
    Assert.assertFalse(fileDumpExtractor.processInputStream(10));

    WorkUnitStatus unitStatus = Mockito.mock(WorkUnitStatus.class);
    when(multistageConnection.executeNext(fileDumpExtractor.workUnitStatus)).thenReturn(unitStatus);
    fileDumpExtractor.getFileDumpExtractorKeys().setFileName(StringUtils.EMPTY);
    Assert.assertFalse(fileDumpExtractor.processInputStream(10));

    when(unitStatus.getBuffer()).thenReturn(null);
    fileDumpExtractor.getFileDumpExtractorKeys().setFileName("test_file_name");
    Assert.assertFalse(fileDumpExtractor.processInputStream(10));

    JobKeys keys = Mockito.mock(JobKeys.class);
    when(source.getJobKeys()).thenReturn(keys);
    when(keys.isPaginationEnabled()).thenReturn(true);
    InputStream input = new ByteArrayInputStream("test_string".getBytes());
    when(unitStatus.getBuffer()).thenReturn(input);
    Assert.assertFalse(fileDumpExtractor.processInputStream(10));
  }
  /**
   * Test getFileName with two scenarios
   * 1: Happy path
   * 2: Unresolved placeholder
   */
  @Test
  public void testGetFileName() throws Exception {
    PowerMockito.mockStatic(VariableUtils.class);
    String fileNameTemplate = "testFileTemplate";
    when(state.getProp(MSTAGE_EXTRACTOR_TARGET_FILE_NAME.getConfig(), StringUtils.EMPTY)).thenReturn(fileNameTemplate);
    String fileName = IOUtils.toString(this.getClass().getResourceAsStream("/other/sample-data-include-long-file-name.txt"), StandardCharsets.UTF_8.name());
    String filePath = String.join("/", "dir", fileName);
    Pair<String, JsonObject> pair = new MutablePair<>(filePath, new JsonObject());
    PowerMockito.when(VariableUtils.replaceWithTracking(any(), any())).thenReturn(pair);
    FileDumpExtractorKeys extractorKeys = new FileDumpExtractor(state, source.getJobKeys()).getFileDumpExtractorKeys();
    Assert.assertEquals(extractorKeys.getFileName(), String.join("/", "dir", fileName.substring(0, 255 - 1)));

    PowerMockito.doThrow(new UnsupportedEncodingException()).when(VariableUtils.class, "replaceWithTracking", any(), any());
    extractorKeys = new FileDumpExtractor(state, source.getJobKeys()).getFileDumpExtractorKeys();
    Assert.assertEquals(extractorKeys.getFileName(), fileNameTemplate);
  }
}
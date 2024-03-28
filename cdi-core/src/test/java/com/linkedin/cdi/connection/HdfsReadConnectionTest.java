// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.connection;

import java.util.ArrayList;
import java.util.List;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import com.linkedin.cdi.keys.ExtractorKeys;
import com.linkedin.cdi.source.HdfsSource;
import com.linkedin.cdi.util.WorkUnitStatus;
import org.apache.gobblin.source.extractor.filebased.FileBasedHelperException;
import org.apache.gobblin.source.extractor.filebased.TimestampAwareFileBasedHelper;
import org.apache.gobblin.source.extractor.hadoop.HadoopFsHelper;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Matchers.*;
import static org.powermock.api.mockito.PowerMockito.*;


@Test
@PrepareForTest({HadoopFsHelper.class, TimestampAwareFileBasedHelper.class})
@PowerMockIgnore("jdk.internal.reflect.*")
public class HdfsReadConnectionTest extends PowerMockTestCase {
  @Test
  public void testGetFileList() throws Exception {
    PowerMockito.mockStatic(HadoopFsHelper.class);
    HadoopFsHelper fsHelper = PowerMockito.mock(HadoopFsHelper.class);

    HdfsSource hdfsSource = new HdfsSource();
    SourceState sourceState = new SourceState();
    sourceState.setProp("ms.extractor.class", "com.linkedin.cdi.extractor.CsvExtractor");
    sourceState.setProp("ms.source.uri", "/data/test?RE=.*");
    sourceState.setProp("extract.table.name", "xxx");
    WorkUnitState state = new WorkUnitState(hdfsSource.getWorkunits(sourceState).get(0), new State());

    state.setProp("ms.extractor.class", "com.linkedin.cdi.extractor.CsvExtractor");
    state.setProp("ms.source.uri", "/data/test?RE=.*");
    hdfsSource.getExtractor(state);

    HdfsConnection conn = new HdfsConnection(state, hdfsSource.getHdfsKeys(), new ExtractorKeys());

    // getHdfsClient would fail as there is not real HDFS
    Assert.assertNull(conn.getHdfsClient());

    // use mocked helper
    conn.setFsHelper(fsHelper);

    doNothing().when(fsHelper).close();
    doNothing().when(fsHelper).connect();
    when(fsHelper.ls(any())).thenReturn(new ArrayList<>());
    when(fsHelper.getFileStream(any())).thenReturn(null);

    Assert.assertNull(conn.executeFirst(WorkUnitStatus.builder().build()).getBuffer());

    when(fsHelper.ls(any())).thenThrow(new FileBasedHelperException("error"));
    Assert.assertNull(conn.executeFirst(WorkUnitStatus.builder().build()).getBuffer());

    conn.closeAll("");
  }

  @Test
  public void testGetFileInputStream() throws Exception {
    PowerMockito.mockStatic(HadoopFsHelper.class);
    HadoopFsHelper fsHelper = PowerMockito.mock(HadoopFsHelper.class);

    HdfsSource hdfsSource = new HdfsSource();
    SourceState sourceState = new SourceState();
    sourceState.setProp("extract.table.name", "xxx");
    sourceState.setProp("ms.extractor.class", "com.linkedin.cdi.extractor.CsvExtractor");
    sourceState.setProp("ms.source.uri", "/jobs/exttest/data/external/snapshots/test");
    WorkUnitState state = new WorkUnitState(hdfsSource.getWorkunits(sourceState).get(0), new State());

    state.setProp("ms.extractor.class", "com.linkedin.cdi.extractor.CsvExtractor");
    state.setProp("ms.source.uri", "/jobs/exttest/data/external/snapshots/test");
    hdfsSource.getExtractor(state);

    HdfsConnection conn = new HdfsConnection(state, hdfsSource.getHdfsKeys(), new ExtractorKeys());
    conn.setFsHelper(fsHelper);

    doNothing().when(fsHelper).close();
    doNothing().when(fsHelper).connect();

    List<String> files = new ArrayList<>();
    files.add("dummy");

    when(fsHelper.ls(any())).thenReturn(files);
    when(fsHelper.getFileStream(any())).thenReturn(null);

    Assert.assertNull(conn.executeFirst(WorkUnitStatus.builder().build()).getBuffer());

    when(fsHelper.getFileStream(any())).thenThrow(new FileBasedHelperException("error"));
    Assert.assertNull(conn.executeFirst(WorkUnitStatus.builder().build()).getBuffer());

    conn.closeAll("");
  }
}

// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.extractor;

import java.io.File;
import java.io.FileFilter;
import java.io.InputStream;
import java.util.List;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import com.linkedin.cdi.connection.MultistageConnection;
import com.linkedin.cdi.exception.RetriableAuthenticationException;
import com.linkedin.cdi.source.HttpSource;
import com.linkedin.cdi.util.WorkUnitStatus;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


/**
 * Test FileDumpExtractor under following scenarios:
 *
 * Scenario 1: download a file and save to /tmp
 */
@Test
public class FileDumpExtractorTest {

  /**
   * Test for scenario 1: download a file and save to /tmp
   *
   * Input: a mocked InputStream from a resource file
   * Output: a non-empty schema, and a file saved in /tmp
   */
  @Test
  void testSaveCsvFile() throws RetriableAuthenticationException {
    InputStream inputStream = getClass().getResourceAsStream("/csv/common-crawl-files.csv");
    WorkUnitStatus status = WorkUnitStatus.builder().buffer(inputStream).build();

    SourceState sourceState = new SourceState();
    sourceState.setProp("extract.table.name", "xxx");
    HttpSource source = new HttpSource();
    source.getWorkunits(sourceState);

    List<WorkUnit> wus = source.getWorkunits(sourceState);
    WorkUnitState state = new WorkUnitState(wus.get(0), new JobState());
    state.setProp("fs.uri", "file://localhost/");
    state.setProp("state.store.fs.uri", "file://localhost");
    state.setProp("data.publisher.final.dir", "/tmp/gobblin/job-output");
    state.setProp("ms.extractor.target.file.name", "common-crawl-files.csv");

    FileDumpExtractor extractor = new FileDumpExtractor(state, source.getHttpSourceKeys());
    MultistageConnection connection = Mockito.mock(MultistageConnection.class);
    extractor.setConnection(connection);
    extractor.workUnitStatus = WorkUnitStatus.builder().build();
    when(connection.executeFirst(extractor.workUnitStatus)).thenReturn(status);

    String schema = extractor.getSchema();
    Assert.assertNotEquals(schema.length(), 0);

    extractor.readRecord(null);

    File[] dbFiles = new File("/tmp/gobblin/job-output").listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return pathname.isFile() && pathname.toString().matches(".*common-crawl-files.csv");
      }
    });
    Assert.assertNotEquals(dbFiles.length, 0);
    if (dbFiles != null) {
      for (File file : dbFiles) {
        file.delete();
      }
    }
  }
}

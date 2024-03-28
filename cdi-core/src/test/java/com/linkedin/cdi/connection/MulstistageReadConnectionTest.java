// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.connection;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import gobblin.configuration.SourceState;
import java.io.UnsupportedEncodingException;
import com.linkedin.cdi.exception.RetriableAuthenticationException;
import com.linkedin.cdi.keys.ExtractorKeys;
import com.linkedin.cdi.keys.JobKeys;
import com.linkedin.cdi.util.VariableUtils;
import com.linkedin.cdi.util.WorkUnitStatus;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.powermock.api.mockito.PowerMockito.*;


@Test
@PrepareForTest(VariableUtils.class)
@PowerMockIgnore("jdk.internal.reflect.*")
public class MulstistageReadConnectionTest extends PowerMockTestCase {
  @Test
  public void testGetNext() throws RetriableAuthenticationException {
    MultistageConnection conn = new MultistageConnection(new SourceState(), new JobKeys(), new ExtractorKeys());
    conn.getExtractorKeys().setSignature("testSignature");
    conn.getExtractorKeys().setActivationParameters(new JsonObject());

    WorkUnitStatus workUnitStatus = Mockito.mock(WorkUnitStatus.class);
    WorkUnitStatus.WorkUnitStatusBuilder builder = Mockito.mock(WorkUnitStatus.WorkUnitStatusBuilder.class);
    when(builder.build()).thenReturn(workUnitStatus);
    when(workUnitStatus.toBuilder()).thenReturn(builder);
    Assert.assertEquals(conn.executeNext(workUnitStatus), workUnitStatus);

    // cover the exception branch
    JobKeys jobKeys = Mockito.mock(JobKeys.class);
    when(jobKeys.getCallInterval()).thenReturn(1L);
    conn.setJobKeys(jobKeys);
    when(jobKeys.getCallInterval()).thenThrow(Mockito.mock(IllegalArgumentException.class));
    conn.executeNext(workUnitStatus);
    Assert.assertEquals(conn.executeNext(workUnitStatus), workUnitStatus);
  }

  @Test
  public void testGetWorkUnitSpecificString() throws UnsupportedEncodingException {
    // Test normal case
    MultistageConnection conn = new MultistageConnection(new SourceState(), new JobKeys(), new ExtractorKeys());
    String template = "test_template";
    JsonObject obj = new JsonObject();
    Assert.assertEquals(conn.getWorkUnitSpecificString(template, obj), template);

    // Test exception by PowerMock
    PowerMockito.mockStatic(VariableUtils.class);
    when(VariableUtils.replaceWithTracking(template, obj, false)).thenThrow(UnsupportedEncodingException.class);
    Assert.assertEquals(conn.getWorkUnitSpecificString(template, obj), template);
  }
}

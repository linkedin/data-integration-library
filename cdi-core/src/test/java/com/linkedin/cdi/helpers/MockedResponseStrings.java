// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.helpers;

public class MockedResponseStrings {
  public MockedResponseStrings() {
  }

  public static final String mockedStringResponse =
      "[{\"id\":1111,\"site_id\":3333,\"uuid\":null,\"name\":\"dummyGHPExitSurvey\",\"active\":false,\"kind\":\"embed\",\"canonical_name\":null,\"created_at\":\"2017-09-1123:20:05+0000\",\"survey_url\":\"https://dummy/surveys/179513\",\"type\":\"survey\"}]";
  public static final String mockedStringResponseMultipleRecords = "[{\"id\":1111,\"site_id\":3333,\"uuid\":null,\"name\":\"dummyGHPExitSurvey\",\"active\":false,\"kind\":\"embed\",\"canonical_name\":null,\"created_at\":\"2017-09-1123:20:05+0000\",\"survey_url\":\"https://dummy/surveys/179513\",\"type\":\"survey\"},{\"id\":2222,\"site_id\":1234,\"uuid\":null,\"name\":\"dummyGHPExitSurvey\",\"active\":false,\"kind\":\"embed\",\"canonical_name\":null,\"created_at\":\"2017-09-1123:20:05+0000\",\"survey_url\":\"https://dummy/surveys/179513\",\"type\":\"survey\"}]";
}
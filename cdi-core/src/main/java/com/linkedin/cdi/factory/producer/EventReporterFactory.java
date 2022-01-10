// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.
package com.linkedin.cdi.factory.producer;

import java.lang.reflect.Constructor;
import org.apache.gobblin.configuration.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.cdi.configuration.PropertyCollection.*;


public class EventReporterFactory {

  private static final Logger LOG = LoggerFactory.getLogger(EventReporterFactory.class);

  public static EventReporter getEventReporter(State state) {
    try {
      Class<?> reporterClass = Class.forName(MSTAGE_REPORTER_CLASS.get(state));
      Constructor<? extends EventReporter> eventReporterConstructor =
          (Constructor<? extends EventReporter>) reporterClass.getConstructor(State.class);
      return eventReporterConstructor.newInstance(state);
    } catch (Exception e) {
      LOG.error("Unable to instantiate Event Reporter make sure to pass a class that implements com.linkedin.cdi.factory.producer.EventReporter interface");
      LOG.error(e.getMessage());
    }
    return null;
  }
}

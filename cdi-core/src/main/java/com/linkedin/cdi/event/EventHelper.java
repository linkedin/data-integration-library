// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.
package com.linkedin.cdi.event;

import com.linkedin.cdi.configuration.MultistageProperties;
import com.linkedin.cdi.events.CdiTrackingEvent;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.source.workunit.WorkUnit;

import static com.linkedin.cdi.configuration.PropertyCollection.*;


/**
 * A helper class to build various events at different stages of job execution
 */
public class EventHelper {

  public static CdiTrackingEvent createInitializationEvent(State state, String namespace) {
    CdiTrackingEvent.Builder builder = CdiTrackingEvent.newBuilder();
    builder.setName("SourceInitializationEvent")
        .setNamespace(namespace)
        .setTimestamp(System.currentTimeMillis())
        .setMetadata(fillEssentialProperties(state));
    return builder.build();
  }

  public static CdiTrackingEvent createWorkunitCreationEvent(State state, List<WorkUnit> workUnit, String namespace) {
    CdiTrackingEvent.Builder builder = CdiTrackingEvent.newBuilder();
    HashMap<String, String> map = new HashMap<>();
    if (workUnit.size() > 0) {
      for (int i = 0; i < workUnit.size(); i++) {
        map.put(String.valueOf(i + 1), workUnit.get(i).toString());
      }
    }
    map.put("NoOfWorkUnitsCreated", String.valueOf(workUnit.size()));
    builder.setName("WorkUnitCreationEvent")
        .setNamespace(namespace)
        .setTimestamp(System.currentTimeMillis())
        .setMetadata(map);
    return builder.build();
  }

  private static Map<String, String> fillEssentialProperties(State state) {
    HashMap<String, String> map = new HashMap<>();
    for (MultistageProperties prop : allProperties) {
      map.put(prop.toString(), prop.get(state).toString());
    }
    return map;
  }
}

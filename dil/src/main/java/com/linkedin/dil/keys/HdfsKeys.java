// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.dil.keys;

import com.google.common.collect.Lists;
import java.util.List;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.State;
import com.linkedin.dil.configuration.MultistageProperties;


/**
 * This structure holds HDFS related parameters that could used to read from
 * or write to HDFS
 *
 * @author chrli
 */

@Slf4j
@Getter(AccessLevel.PUBLIC)
@Setter(AccessLevel.PUBLIC)
public class HdfsKeys extends JobKeys {
  final private static List<MultistageProperties> ESSENTIAL_PARAMETERS = Lists.newArrayList(
      // HDFS essential parameters
  );

  @Override
  public void logUsage(State state) {
    super.logUsage(state);
    for (MultistageProperties p : ESSENTIAL_PARAMETERS) {
      log.info("Property {} ({}) has value {} ", p.toString(), p.getClassName(), p.getValidNonblankWithDefault(state));
    }
  }
}

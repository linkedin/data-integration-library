// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.keys;

import com.google.common.collect.Lists;
import com.linkedin.cdi.configuration.MultistageProperties;
import java.util.List;
import org.apache.gobblin.configuration.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This structure holds HDFS related parameters that could used to read from
 * or write to HDFS
 *
 * @author chrli
 */
public class HdfsKeys extends JobKeys {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsKeys.class);
  final private static List<MultistageProperties> ESSENTIAL_PARAMETERS = Lists.newArrayList(
      // HDFS essential parameters
  );

  @Override
  public void logUsage(State state) {
    super.logUsage(state);
    for (MultistageProperties p : ESSENTIAL_PARAMETERS) {
      LOG.info("Property {} ({}) has value {} ", p.toString(), p.getClassName(), p.getProp(state));
    }
  }
}

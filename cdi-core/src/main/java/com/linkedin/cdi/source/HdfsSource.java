// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.source;

import com.linkedin.cdi.connection.HdfsConnection;
import com.linkedin.cdi.extractor.MultistageExtractor;
import com.linkedin.cdi.keys.HdfsKeys;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.Extractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class supports HDFS as just another protocol. The main function
 * of it is to launch a proper extractor with a HdfsConnection
 */
public class HdfsSource extends MultistageSource<Schema, GenericRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsSource.class);

  public HdfsKeys getHdfsKeys() {
    return hdfsKeys;
  }

  public void setHdfsKeys(HdfsKeys hdfsKeys) {
    this.hdfsKeys = hdfsKeys;
  }

  private HdfsKeys hdfsKeys;

  public HdfsSource() {
    hdfsKeys = new HdfsKeys();
    jobKeys = hdfsKeys;
  }

  protected void initialize(State state) {
    super.initialize(state);
    hdfsKeys.logDebugAll();
  }

  /**
   * Create extractor based on the input WorkUnitState, the extractor.class
   * configuration, and a new HdfsConnection
   *
   * @param state WorkUnitState passed in from Gobblin framework
   * @return the MultistageExtractor object
   */

  @Override
  public Extractor<Schema, GenericRecord> getExtractor(WorkUnitState state) {
    initialize(state);
    MultistageExtractor<Schema, GenericRecord> extractor =
        (MultistageExtractor<Schema, GenericRecord>) super.getExtractor(state);
    extractor.setConnection(new HdfsConnection(state, hdfsKeys, extractor.getExtractorKeys()));
    return extractor;

  }
}

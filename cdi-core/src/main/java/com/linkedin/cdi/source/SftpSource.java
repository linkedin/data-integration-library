// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.source;

import com.linkedin.cdi.connection.SftpConnection;
import com.linkedin.cdi.extractor.MultistageExtractor;
import com.linkedin.cdi.keys.SftpKeys;
import com.linkedin.cdi.util.VariableUtils;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.Extractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.cdi.configuration.PropertyCollection.*;


/**
 * Source class to handle sftp protocol
 */
public class SftpSource extends MultistageSource<Schema, GenericRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(SftpSource.class);
  SftpKeys sftpSourceKeys;

  public SftpKeys getSftpSourceKeys() {
    return sftpSourceKeys;
  }

  public void setSftpSourceKeys(SftpKeys sftpSourceKeys) {
    this.sftpSourceKeys = sftpSourceKeys;
  }

  public SftpSource() {
    sftpSourceKeys = new SftpKeys();
    jobKeys = sftpSourceKeys;
  }

  protected void initialize(State state) {
    super.initialize(state);
    this.parseUri(state);
    sftpSourceKeys.setFilesPattern(MSTAGE_SOURCE_FILES_PATTERN.get(state));
    sftpSourceKeys.setTargetFilePattern(
        MSTAGE_EXTRACTOR_TARGET_FILE_NAME.get(state));
    sftpSourceKeys.logDebugAll();
  }

  /**
   * Create extractor based on the input WorkUnitState, the extractor.class
   * configuration, and a new SftpConnection
   *
   * @param state WorkUnitState passed in from Gobblin framework
   * @return the MultistageExtractor object
   */
  @Override
  public Extractor<Schema, GenericRecord> getExtractor(WorkUnitState state) {
    initialize(state);
    MultistageExtractor<Schema, GenericRecord> extractor =
        (MultistageExtractor<Schema, GenericRecord>) super.getExtractor(state);
    extractor.setConnection(new SftpConnection(state, this.sftpSourceKeys, extractor.getExtractorKeys()));
    return extractor;
  }

  /**
   * This method parses ms.source.uri, following are the examples of Valid URIs
   * Valid[recommended] sftp://somehost.com:22/a/b/*c*.csv
   * Valid[Supported for backward compatibility] : /a/b/*c*.csv
   */
  private void parseUri(State state) {
    String sourceUri = MSTAGE_SOURCE_URI.get(state);
    if (VariableUtils.hasVariable(sourceUri)) {
      sftpSourceKeys.setFilesPath(sourceUri);
    } else {
      try {
        URI uri = new URI(sourceUri);
        if (uri.getHost() != null) {
          state.setProp(ConfigurationKeys.SOURCE_CONN_HOST_NAME, uri.getHost());
        }
        if (uri.getPort() != -1) {
          state.setProp(ConfigurationKeys.SOURCE_CONN_PORT, uri.getPort());
        }
        sftpSourceKeys.setFilesPath(uri.getPath());
      } catch (URISyntaxException e) {
        LOG.warn("Invalid URI format in ms.source.uri", e);
      }
    }
  }
}

// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.keys;

import com.google.common.collect.Lists;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.configuration.State;
import com.linkedin.cdi.configuration.MultistageProperties;
import org.apache.gobblin.source.workunit.WorkUnit;


/**
 * These attributes are defined and maintained in CsvExtractor
 *
 * @author chrli
 */
@Slf4j
@Getter(AccessLevel.PUBLIC)
@Setter
public class CsvExtractorKeys extends ExtractorKeys {
  final private static List<MultistageProperties> ESSENTIAL_PARAMETERS = Lists.newArrayList(
      MultistageProperties.MSTAGE_CSV_COLUMN_HEADER,
      MultistageProperties.MSTAGE_CSV_SEPARATOR,
      MultistageProperties.MSTAGE_CSV_SKIP_LINES,
      MultistageProperties.MSTAGE_CSV_QUOTE_CHARACTER,
      MultistageProperties.MSTAGE_CSV_ESCAPE_CHARACTER);

  private Iterator<String[]> csvIterator = null;
  private long currentPageNumber = 0;
  private Boolean columnHeader = false;
  private int columnHeaderIndex = 0;
  private int rowsToSkip = 0;
  private String separator = MultistageProperties.MSTAGE_CSV_SEPARATOR.getDefaultValue();
  private String quoteCharacter = MultistageProperties.MSTAGE_CSV_QUOTE_CHARACTER.getDefaultValue();
  private String escapeCharacter = MultistageProperties.MSTAGE_CSV_ESCAPE_CHARACTER.getDefaultValue();
  // column name --> index mapping created based on the output or inferred schema
  private Map<String, Integer> columnToIndexMap = new HashMap<>();
  // A queue that stores sample rows read in during schema inference
  // This is necessary as the input stream can only be read once
  private Deque<String[]> sampleRows = new ArrayDeque<>();
  private String[] headerRow;
  private Set<Integer> columnProjection = new HashSet<>();
  private Boolean isValidOutputSchema = true;
  private String defaultFieldType = StringUtils.EMPTY;

  public void incrCurrentPageNumber() {
    currentPageNumber++;
  }

  @Override
  public void logDebugAll(WorkUnit workUnit) {
    super.logDebugAll(workUnit);
    log.debug("These are values of CsvExtractor regarding to Work Unit: {}",
        workUnit == null ? "testing" : workUnit.getProp(MultistageProperties.DATASET_URN_KEY.toString()));
    log.debug("Is column header present: {}", columnHeader);
    log.debug("Total rows to skip: {}", rowsToSkip);
  }

  @Override
  public void logUsage(State state) {
    super.logUsage(state);
    for (MultistageProperties p: ESSENTIAL_PARAMETERS) {
      log.info("Property {} ({}) has value {} ", p.toString(), p.getClassName(), p.getValidNonblankWithDefault(state));
    }
  }
}

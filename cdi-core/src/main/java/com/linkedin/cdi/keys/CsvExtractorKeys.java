// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.keys;

import com.beust.jcommander.internal.Lists;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.cdi.configuration.PropertyCollection.*;


/**
 * These attributes are defined and maintained in CsvExtractor
 *
 * @author chrli
 */
public class CsvExtractorKeys extends ExtractorKeys {
  private static final Logger LOG = LoggerFactory.getLogger(CsvExtractorKeys.class);
  private Iterator<String[]> csvIterator = null;
  private long currentPageNumber = 0;
  private Boolean columnHeader = false;
  // column name --> index mapping created based on the output or inferred schema
  private Map<String, Integer> columnToIndexMap = new HashMap<>();
  // A queue that stores sample rows read in during schema inference
  // This is necessary as the input stream can only be read once
  private Deque<String[]> sampleRows = new ArrayDeque<>();
  private String[] headerRow;
  private List<Integer> columnProjection = Lists.newArrayList();
  private Boolean isValidOutputSchema = true;
  private String defaultFieldType = StringUtils.EMPTY;

  public void incrCurrentPageNumber() {
    currentPageNumber++;
  }

  @Override
  public void logDebugAll(WorkUnit workUnit) {
    super.logDebugAll(workUnit);
    LOG.debug("These are values of CsvExtractor regarding to Work Unit: {}",
        workUnit == null ? "testing" : workUnit.getProp(DATASET_URN.toString()));
    LOG.debug("Is column header present: {}", columnHeader);
  }

  public Iterator<String[]> getCsvIterator() {
    return csvIterator;
  }

  public void setCsvIterator(Iterator<String[]> csvIterator) {
    this.csvIterator = csvIterator;
  }

  public long getCurrentPageNumber() {
    return currentPageNumber;
  }

  public void setCurrentPageNumber(long currentPageNumber) {
    this.currentPageNumber = currentPageNumber;
  }

  public Boolean getColumnHeader() {
    return columnHeader;
  }

  public void setColumnHeader(Boolean columnHeader) {
    this.columnHeader = columnHeader;
  }

  public Map<String, Integer> getColumnToIndexMap() {
    return columnToIndexMap;
  }

  public void setColumnToIndexMap(Map<String, Integer> columnToIndexMap) {
    this.columnToIndexMap = columnToIndexMap;
  }

  public Deque<String[]> getSampleRows() {
    return sampleRows;
  }

  public void setSampleRows(Deque<String[]> sampleRows) {
    this.sampleRows = sampleRows;
  }

  public String[] getHeaderRow() {
    return headerRow;
  }

  public void setHeaderRow(String[] headerRow) {
    this.headerRow = headerRow;
  }

  public List<Integer> getColumnProjection() {
    return columnProjection;
  }

  public void setColumnProjection(List<Integer> columnProjection) {
    this.columnProjection = columnProjection;
  }

  public Boolean getIsValidOutputSchema() {
    return isValidOutputSchema;
  }

  public void setIsValidOutputSchema(Boolean validOutputSchema) {
    isValidOutputSchema = validOutputSchema;
  }

  public String getDefaultFieldType() {
    return defaultFieldType;
  }

  public void setDefaultFieldType(String defaultFieldType) {
    this.defaultFieldType = defaultFieldType;
  }
}

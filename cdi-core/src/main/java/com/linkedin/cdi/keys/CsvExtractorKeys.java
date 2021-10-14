// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.keys;

import com.google.common.collect.Lists;
import com.linkedin.cdi.configuration.MultistageProperties;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.configuration.State;
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
  final private static List<MultistageProperties> ESSENTIAL_PARAMETERS = Lists.newArrayList(
      MSTAGE_CSV_COLUMN_HEADER,
      MSTAGE_CSV_SEPARATOR,
      MSTAGE_CSV_SKIP_LINES,
      MSTAGE_CSV_QUOTE_CHARACTER,
      MSTAGE_CSV_ESCAPE_CHARACTER);

  private Iterator<String[]> csvIterator = null;
  private long currentPageNumber = 0;
  private Boolean columnHeader = false;
  private int columnHeaderIndex = 0;
  private int rowsToSkip = 0;
  private String separator = MSTAGE_CSV_SEPARATOR.getDefaultValue();
  private String quoteCharacter = MSTAGE_CSV_QUOTE_CHARACTER.getDefaultValue();
  private String escapeCharacter = MSTAGE_CSV_ESCAPE_CHARACTER.getDefaultValue();
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
    LOG.debug("These are values of CsvExtractor regarding to Work Unit: {}",
        workUnit == null ? "testing" : workUnit.getProp(DATASET_URN_KEY.toString()));
    LOG.debug("Is column header present: {}", columnHeader);
    LOG.debug("Total rows to skip: {}", rowsToSkip);
  }

  @Override
  public void logUsage(State state) {
    super.logUsage(state);
    for (MultistageProperties p: ESSENTIAL_PARAMETERS) {
      LOG.info("Property {} ({}) has value {} ", p.toString(), p.getClassName(), p.getProp(state));
    }
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

  public int getColumnHeaderIndex() {
    return columnHeaderIndex;
  }

  public void setColumnHeaderIndex(int columnHeaderIndex) {
    this.columnHeaderIndex = columnHeaderIndex;
  }

  public int getRowsToSkip() {
    return rowsToSkip;
  }

  public void setRowsToSkip(int rowsToSkip) {
    this.rowsToSkip = rowsToSkip;
  }

  public String getSeparator() {
    return separator;
  }

  public void setSeparator(String separator) {
    this.separator = separator;
  }

  public String getQuoteCharacter() {
    return quoteCharacter;
  }

  public void setQuoteCharacter(String quoteCharacter) {
    this.quoteCharacter = quoteCharacter;
  }

  public String getEscapeCharacter() {
    return escapeCharacter;
  }

  public void setEscapeCharacter(String escapeCharacter) {
    this.escapeCharacter = escapeCharacter;
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

  public Set<Integer> getColumnProjection() {
    return columnProjection;
  }

  public void setColumnProjection(Set<Integer> columnProjection) {
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

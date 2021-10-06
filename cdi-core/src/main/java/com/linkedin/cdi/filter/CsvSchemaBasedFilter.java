// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.filter;

import com.linkedin.cdi.keys.CsvExtractorKeys;
import com.linkedin.cdi.util.JsonIntermediateSchema;
import java.util.Arrays;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Filter CSV records by Json Intermediate schema
 *
 * @author esong
 *
 */
public class CsvSchemaBasedFilter extends MultistageSchemaBasedFilter<String[]> {
  private static final Logger LOG = LoggerFactory.getLogger(CsvSchemaBasedFilter.class);
  private CsvExtractorKeys csvExtractorKeys;

  public CsvSchemaBasedFilter(JsonIntermediateSchema schema, CsvExtractorKeys csvExtractorKeys) {
    super(schema);
    this.csvExtractorKeys = csvExtractorKeys;
  }

  @Override
  public String[] filter(String[] input) {
    Set<Integer> columnProjection = csvExtractorKeys.getColumnProjection();
    if (columnProjection.size() > 0) {
      // use user-defined column projection to filter
      return filter(input, columnProjection);
    } else if (csvExtractorKeys.getHeaderRow() != null && csvExtractorKeys.getIsValidOutputSchema()) {
      // use the header and schema to generate column projection, then filter
      String[] headerRow = csvExtractorKeys.getHeaderRow();
      for (int i = 0; i < headerRow.length; i++) {
        if (schema.getColumns().keySet().stream().anyMatch(headerRow[i]::equalsIgnoreCase)) {
          columnProjection.add(i);
        }
      }
      csvExtractorKeys.setColumnProjection(columnProjection);
      return filter(input, columnProjection);
    } else {
      LOG.debug("Defaulting to project first N columns");
      // take first N column, where N is the number of columns in the schema
      // if the schema's size larger than input, then the extra columns will be padded with null
      return Arrays.copyOf(input, schema.getColumns().size());
    }
  }

  /**
   * shift the wanted fields to front in place, and then truncate the array
   * @param input original row
   * @param columnProjection column projection
   * @return modified row
   */
  private String[] filter(String[] input, Set<Integer> columnProjection) {
    int curr = 0;
    for (int i = 0; i < input.length; i++) {
      if (columnProjection.contains(i)) {
        swap(input, i, curr++);
      }
    }
    return Arrays.copyOf(input, curr);
  }

  private void swap(String[] input, int i, int j) {
    String temp = input[i];
    input[i] = input[j];
    input[j] = temp;
  }
}

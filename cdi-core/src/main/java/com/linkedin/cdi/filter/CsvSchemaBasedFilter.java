// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.filter;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.linkedin.cdi.keys.CsvExtractorKeys;
import com.linkedin.cdi.util.JsonIntermediateSchema;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.cdi.configuration.StaticConstants.*;


/**
 * Filter CSV records by Json Intermediate schema
 *
 * @author esong
 *
 */
public class CsvSchemaBasedFilter extends MultistageSchemaBasedFilter<String[]> {
  private static final Logger LOG = LoggerFactory.getLogger(CsvSchemaBasedFilter.class);
  final private CsvExtractorKeys csvExtractorKeys;
  final private JsonArray schemaArray;

  public CsvSchemaBasedFilter(JsonArray schema, CsvExtractorKeys csvExtractorKeys) {
    super(new JsonIntermediateSchema(schema));
    this.csvExtractorKeys = csvExtractorKeys;
    this.schemaArray = schema;
  }

  @Override
  public String[] filter(String[] input) {
    List<Integer> columnProjection = csvExtractorKeys.getColumnProjection();

    if (columnProjection.isEmpty()
        && csvExtractorKeys.getHeaderRow() != null
        && csvExtractorKeys.getIsValidOutputSchema()) {
      // use the header and schema to generate column projection, then filter
      String[] headerRow = csvExtractorKeys.getHeaderRow();
      for (JsonElement column: schemaArray) {
        for (int i = 0; i < headerRow.length; i++) {
          if (headerRow[i].equalsIgnoreCase(column.getAsJsonObject().get(KEY_WORD_COLUMN_NAME).getAsString())) {
            columnProjection.add(i);
          }
        }
      }
      csvExtractorKeys.setColumnProjection(columnProjection);
    }

    if (columnProjection.size() > 0) {
      return filter(input, columnProjection);
    } else {
      LOG.info("Defaulting to project first N columns");
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
  String[] filter(String[] input, List<Integer> columnProjection) {
    if (columnProjection.size() == 0) {
      return null;
    }

    String[] output = new String[columnProjection.size()];
    for (int i = 0; i < output.length; i++) {
      if (columnProjection.get(i) < input.length) {
        output[i] = input[columnProjection.get(i)];
      } else {
        output[i] = StringUtils.EMPTY;
      }
    }
    return output;
  }
}

// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.configuration;

import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import com.linkedin.cdi.util.CsvUtils;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.configuration.State;

import static com.linkedin.cdi.configuration.StaticConstants.*;


/**
 * CSV Parameters
 */
public class CsvProperties extends JsonObjectProperties{
  final private static String DEFAULT_FIELD_TYPE = "defaultFieldType";
  final private static String ESCAPE_CHARACTER = "escapeCharacter";
  final private static String ESCAPE_CHARACTER_DEFAULT = "u005C";
  final private static String QUOTE_CHARACTER = "quoteCharacter";
  final private static String QUOTE_CHARACTER_DEFAULT = "\"";
  final private static String FIELD_SEPARATOR = "fieldSeparator";
  final private static String FIELD_SEPARATOR_DEFAULT = KEY_WORD_COMMA;
  final private static String RECORD_SEPARATOR = "recordSeparator";
  final private static String RECORD_SEPARATOR_DEFAULT = System.lineSeparator();
  final private static String LINES_TO_SKIP = "linesToSkip";
  final private static String COLUMN_HEADER_INDEX = "columnHeaderIndex";
  final private static String COLUMN_PROJECTION = "columnProjection";
  final private static String MAX_FAILURES = "maxFailures";
  final private static String KEEP_NULL_STRING = "keepNullString";

  final private static IntegerProperties linesToSkip =  new IntegerProperties(LINES_TO_SKIP);
  final private static IntegerProperties columnHeaderIndex =  new IntegerProperties(COLUMN_HEADER_INDEX, -1, Integer.MAX_VALUE, -1);
  final private static LongProperties maxFailures =  new LongProperties(MAX_FAILURES);
  final private static BooleanProperties keepNullString =  new BooleanProperties(KEEP_NULL_STRING, Boolean.FALSE);

  final private static List<String> csvAttributes = Lists.newArrayList(
      DEFAULT_FIELD_TYPE,
      ESCAPE_CHARACTER, QUOTE_CHARACTER, FIELD_SEPARATOR, RECORD_SEPARATOR,
      LINES_TO_SKIP, COLUMN_HEADER_INDEX, COLUMN_PROJECTION,
      MAX_FAILURES, KEEP_NULL_STRING
  );

  @Override
  public boolean isValid(State state) {
    if (super.isValid(state) && !super.isBlank(state)) {
      JsonObject value = GSON.fromJson(state.getProp(getConfig()), JsonObject.class);
      if (!value.entrySet().stream().allMatch(p -> csvAttributes.contains(p.getKey()))) {
        return false;
      }

      if (value.has(COLUMN_PROJECTION)) {
        String columnProjections = value.get(COLUMN_PROJECTION).getAsString();
        if (columnProjections.trim().isEmpty()) {
          return false;
        }

        if (expandColumnProjection(columnProjections).size() == 0) {
          return false;
        }
      }

      State tmpState = new State();
      if (value.has(COLUMN_HEADER_INDEX)) {
        tmpState.setProp(COLUMN_HEADER_INDEX, value.get(COLUMN_HEADER_INDEX).getAsString());
        if (!columnHeaderIndex.isValid(tmpState)) {
          return false;
        }
      }
      if (value.has(LINES_TO_SKIP)) {
        tmpState.setProp(LINES_TO_SKIP, value.get(LINES_TO_SKIP).getAsString());
        if (!linesToSkip.isValid(tmpState)) {
          return false;
        }

        if (linesToSkip.get(tmpState) < columnHeaderIndex.get(tmpState) + 1) {
          return false;
        }
      }
      if (value.has(MAX_FAILURES)) {
        tmpState.setProp(MAX_FAILURES, value.get(MAX_FAILURES).getAsString());
        if (!maxFailures.isValid(tmpState)) {
          return false;
        }
      }

      if (value.has(KEEP_NULL_STRING)) {
        tmpState.setProp(KEEP_NULL_STRING, value.get(KEEP_NULL_STRING).getAsString());
        if (!keepNullString.isValid(tmpState)) {
          return false;
        }
      }
    }
    return super.isValid(state);
  }

  /**
   * Constructor with implicit default value
   * @param config property name
   */
  CsvProperties(String config) {
    super(config);
  }

  public String getDefaultFieldType(State state) {
    JsonObject value = get(state);
    if (value.has(DEFAULT_FIELD_TYPE)) {
      return value.get(DEFAULT_FIELD_TYPE).getAsString();
    }
    return StringUtils.EMPTY;
  }

  public String getEscapeCharacter(State state) {
    JsonObject value = get(state);
    if (value.has(ESCAPE_CHARACTER)) {
      return CsvUtils.unescape(value.get(ESCAPE_CHARACTER).getAsString().trim());
    }
    return CsvUtils.unescape(ESCAPE_CHARACTER_DEFAULT);
  }

  public String getQuoteCharacter(State state) {
    JsonObject value = get(state);
    if (value.has(QUOTE_CHARACTER)) {
      return CsvUtils.unescape(value.get(QUOTE_CHARACTER).getAsString().trim());
    }
    return QUOTE_CHARACTER_DEFAULT;
  }

  public String getFieldSeparator(State state) {
    JsonObject value = get(state);
    if (value.has(FIELD_SEPARATOR)) {
      return CsvUtils.unescape(value.get(FIELD_SEPARATOR).getAsString().trim());
    }
    return FIELD_SEPARATOR_DEFAULT;
  }

  public String getRecordSeparator(State state) {
    JsonObject value = get(state);
    if (value.has(RECORD_SEPARATOR)) {
      return CsvUtils.unescape(value.get(RECORD_SEPARATOR).getAsString().trim());
    }
    return RECORD_SEPARATOR_DEFAULT;
  }

  public Integer getLinesToSkip(State state) {
    JsonObject value = get(state);
    int skip = 0;
    if (value.has(LINES_TO_SKIP) && StringUtils.isNotBlank(value.get(LINES_TO_SKIP).getAsString())) {
      skip = value.get(LINES_TO_SKIP).getAsInt();
    }
    return Math.max(skip, getColumnHeaderIndex(state) + 1);
  }

  public Integer getColumnHeaderIndex(State state) {
    JsonObject value = get(state);
    if (value.has(COLUMN_HEADER_INDEX) && StringUtils.isNotBlank(value.get(COLUMN_HEADER_INDEX).getAsString())) {
      return value.get(COLUMN_HEADER_INDEX).getAsInt();
    }
    return -1;
  }

  public List<Integer> getColumnProjection(State state) {
    JsonObject value = get(state);
    if (value.has(COLUMN_PROJECTION)) {
      return expandColumnProjection(value.get(COLUMN_PROJECTION).getAsString());
    }
    return new ArrayList<>();
  }

  public Long getMaxFailures(State state) {
    JsonObject value = get(state);
    if (value.has(MAX_FAILURES) && StringUtils.isNotBlank(value.get(MAX_FAILURES).getAsString())) {
      return value.get(MAX_FAILURES).getAsLong();
    }
    return 0L;
  }

  public Boolean getKeepNullString(State state) {
    JsonObject value = get(state);
    if (value.has(KEEP_NULL_STRING) && StringUtils.isNotBlank(value.get(KEEP_NULL_STRING).getAsString())) {
      return value.get(KEEP_NULL_STRING).getAsBoolean();
    }
    return false;
  }

  /**
   * Expand a column projection string into a list of indices
   * @param columnProjection columns to project
   * @return a list of column indices
   */
  private List<Integer> expandColumnProjection(String columnProjection) {
    List<Integer> expandedColumnProjection = new ArrayList<>();
    if (StringUtils.isNotBlank(columnProjection)) {
      for (String val : columnProjection.split(",")) {
        if (val.matches("^(\\d+)-(\\d+)$")) {  // range
          try {
            int left = Integer.parseInt(val.split("-")[0]);
            int right = Integer.parseInt(val.split("-")[1]);
            if (left < 0 || right < 0 || left >= right) {
              return Lists.newArrayList();
            } else {
              for (int i = left; i <= right; i++) {
                expandedColumnProjection.add(i);
              }
            }
          } catch (Exception e) {
            return Lists.newArrayList();
          }
        } else if (val.matches("^\\d+$")) {  // single number
          try {
            int col = Integer.parseInt(val);
            if (col < 0) {
              return Lists.newArrayList();
            } else {
              expandedColumnProjection.add(col);
            }
          } catch (Exception e) {
            return Lists.newArrayList();
          }
        } else {  // unknown patterns
          return Lists.newArrayList();
        }
      }
    }
    return expandedColumnProjection;
  }
}

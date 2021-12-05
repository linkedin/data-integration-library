// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.List;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.util.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

import static com.linkedin.cdi.configuration.StaticConstants.*;


/**
 * This class encapsulates Watermark definitions, and provide function to manage
 * features generate  milli-seconds or date time ranges
 *
 * @author chrli
 */
public class WatermarkDefinition {
  private static final Logger LOG = LoggerFactory.getLogger(WatermarkDefinition.class);
  final private static String DEFAULT_TIMEZONE = "America/Los_Angeles";

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public WatermarkTypes getType() {
    return type;
  }

  public void setType(WatermarkTypes type) {
    this.type = type;
  }

  public Pair<String, String> getRange() {
    return range;
  }

  public void setRange(Pair<String, String> range) {
    this.range = range;
  }

  public WorkUnitPartitionTypes getWorkUnitPartitionType() {
    return workUnitPartitionType;
  }

  public void setWorkUnitPartitionType(WorkUnitPartitionTypes workUnitPartitionType) {
    this.workUnitPartitionType = workUnitPartitionType;
  }

  public void setUnits(String units) {
    this.units = units;
  }

  public enum WatermarkTypes {
    DATETIME("datetime"),
    UNIT("unit");

    private final String name;

    WatermarkTypes(String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  private String name;
  private WatermarkTypes type;
  private Pair<String, String> range;
  private WorkUnitPartitionTypes workUnitPartitionType = null;

  // units is the internal storage of work units string, it should be
  // a JsonArray formatted as String
  private String units;

  /**
   * A constructor that creates a Unit watermark definition
   * <p>
   * A Unit watermark has a list of String values
   * @param name the name or the watermark
   * @param units the units in a JsonArray
   */
  public WatermarkDefinition(String name, JsonArray units) {
    this.setName(name);
    this.setType(WatermarkTypes.UNIT);
    this.setUnits(units.toString());
  }

  /**
   * A constructor that creates a Unit watermark definition from a units string
   *
   * <p>
   * A Unit watermark has a list of name : value pairs coded as a JsonArray of JsonObjects
   * @param name the name or the watermark
   * @param commaSeparatedUnits the comma separated units, either a JsonArray or a simple String list
   */
  public WatermarkDefinition(String name, String commaSeparatedUnits) {
    setUnits(name, commaSeparatedUnits);
  }

  /**
   * If the string is JsonArray, it will be stored as is.
   * Otherwise, the string is broken down as a list of values. And then the values
   * will be combined with the unit watermark name as name : value pairs.
   * @param name the name or the watermark
   * @param commaSeparatedUnits the comma separated units, either a JsonArray or a simple String list
   * @return the watermark definition object
   */
  public WatermarkDefinition setUnits(String name, String commaSeparatedUnits) {
    boolean isJsonArrayUnits = true;
    this.setName(name);
    this.setType(WatermarkTypes.UNIT);
    try {
        GSON.fromJson(commaSeparatedUnits, JsonArray.class);
    } catch (Exception e) {
      LOG.info("Assuming simple Unit Watermark definition as the unit watermark cannot be converted to JsonArray");
      LOG.info("Origin unit watermark definition: {} : {}", name, commaSeparatedUnits);
      isJsonArrayUnits = false;
    }

    if (isJsonArrayUnits) {
      this.setUnits(commaSeparatedUnits);
    } else {
      JsonArray unitArray = new JsonArray();
      List<String> units = Lists.newArrayList(commaSeparatedUnits.split(StringUtils.COMMA_STR));
      for (String unit: units) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty(name, unit.trim());
        unitArray.add(jsonObject);
      }
      this.setUnits(unitArray.toString());
    }
    return this;
  }

  /**
   * A constructor that creates a Datetime watermark definition
   * <p>
   * A Datetime watermark has a date range
   * @param name the name of the watermark
   * @param startDate the start date string in yyyy-MM-dd format
   * @param endDate the end date string in yyyy-MM-dd format or - for current date
   */
  public WatermarkDefinition(String name, String startDate, String endDate) {
    this(name, startDate, endDate, false);
  }

  public WatermarkDefinition(String name, String startDate, String endDate, boolean isPartialPartition) {
    this.setName(name);
    this.setType(WatermarkTypes.DATETIME);
    this.setRange(new ImmutablePair<>(startDate, endDate));
  }

  public WatermarkDefinition(JsonObject definition, boolean isPartialPartition) {
    this(definition, isPartialPartition, null);
  }

  public WatermarkDefinition(JsonObject definition, boolean isPartialPartition,
      WorkUnitPartitionTypes workUnitPartitionType) {
    Assert.assertTrue(definition.has(KEY_WORD_NAME));
    Assert.assertTrue(definition.has(KEY_WORD_TYPE));
    Assert.assertNotNull(WatermarkTypes.valueOf(definition.get(KEY_WORD_TYPE).getAsString().toUpperCase()));
    this.setName(definition.get(KEY_WORD_NAME).getAsString());
    if (definition.get(KEY_WORD_TYPE).getAsString().equalsIgnoreCase(WatermarkTypes.DATETIME.name)) {
      this.setType(WatermarkTypes.DATETIME);
      this.setRange(new ImmutablePair<>(
          definition.get(KEY_WORD_RANGE).getAsJsonObject().get(KEY_WORD_FROM).getAsString(),
          definition.get(KEY_WORD_RANGE).getAsJsonObject().get(KEY_WORD_TO).getAsString()));
      this.setWorkUnitPartitionType(workUnitPartitionType);
    } else if (definition.get(KEY_WORD_TYPE).getAsString().equalsIgnoreCase(WatermarkTypes.UNIT.name)) {
      this.setType(WatermarkTypes.UNIT);
      this.setUnits(definition.get(KEY_WORD_NAME).getAsString(), definition.get(KEY_WORD_UNITS).getAsString());
    }
  }

  /**
   * Weekly/Monthly partitioned jobs/sources expect the fromDate to be less than toDate.
   * Keeping the precision at day level for Weekly and Monthly partitioned watermarks.
   *
   * If partial partition is set to true, we don't floor the watermark for a given
   * partition type.
   * For daily partition type, 2019-01-01T12:31:00 will be rounded to 2019-01-01T00:00:00,
   * if partial partition is false.
   *
   * @param input the data time formula or string
   * @return DateTime object
   */
  @VisibleForTesting
  DateTime getDateTime(final String input) {
    String dtString = input;
    DateTimeZone timeZone = DateTimeZone.forID(TZ_LOS_ANGELES);
    if (input.equals("-")) {
      dtString = "P0DT0H0M";
    } else if (input.contains("\\.")) {
      timeZone = DateTimeZone.forID(input.split("\\.")[1]);
      dtString = input.split("\\.")[0];
    }

    // The standard ISO format - PyYmMwWdDThHmMsS, supporting DAY and HOUR only with DAY component being mandatory.
    // e.g.P1D, P2DT5H, P0DT7H
    if (dtString.matches(REGEXP_TIME_DURATION_PATTERN)) {
      Period period = Period.parse(dtString);
      if (dtString.matches(REGEXP_DAY_ONLY_DURATION_PATTERN)) {
        return DateTime.now().withZone(timeZone).dayOfMonth().roundFloorCopy().minus(period);
      } else if (dtString.matches(REGEXP_HOUR_ONLY_DURATION_PATTERN)) {
        return DateTime.now().withZone(timeZone).hourOfDay().roundFloorCopy().minus(period);
      } else {
        return DateTime.now().withZone(timeZone).minuteOfHour().roundFloorCopy().minus(period);
      }
    } else {
      return DateTimeUtils.parse(dtString, TZ_LOS_ANGELES);
    }
  }

  private Long getMillis(String input) {
    return getDateTime(input).getMillis();
  }

  public ImmutablePair<DateTime, DateTime> getRangeInDateTime() {
    return new ImmutablePair<>(getDateTime(range.getKey()), getDateTime(range.getValue()));
  }

  public ImmutablePair<Long, Long> getRangeInMillis() {
    return new ImmutablePair<>(getMillis(range.getKey()), getMillis(range.getValue()));
  }

  /**
   * get a list of work units, with each coded as a name : value pair.
   *
   * The internal storage of work units string should be a JsonArray string
   * @return list of work units
   */
  public List<String> getUnits() {
    List<String> unitList =  Lists.newArrayList();
    JsonArray unitArray = GSON.fromJson(units, JsonArray.class);
    for (JsonElement unit: unitArray) {
      unitList.add(unit.toString());
    }
    return unitList;
  }

  public String getLongName() {
    return "watermark." + name;
  }
}

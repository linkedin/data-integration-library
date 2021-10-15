// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.configuration;

import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.configuration.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.cdi.configuration.StaticConstants.*;


/**
 * A Long type of property has default defaultValue of 0L
 */
public class LongProperties extends MultistageProperties<Long> {
  private static final Logger LOG = LoggerFactory.getLogger(LongProperties.class);

  LongProperties(String config) {
    super(config, Long.class, Long.MAX_VALUE, 0L, 0L);
  }

  LongProperties(String config, Long maxValue) {
    super(config, Long.class, maxValue, 0L, 0L);
  }

  LongProperties(String config, Long maxValue, Long minValue) {
    super(config, Long.class, maxValue, minValue, 0L);
  }

  LongProperties(String config, Long maxValue, Long minValue, Long defaultValue) {
    super(config, Long.class, maxValue, minValue, defaultValue);
  }

  /**
   * Validates the value when it is blank
   * - No configuration is considered blank
   * - A blank string is considered blank
   *
   * @param state state
   * @return true if blank
   */
  @Override
  public boolean isBlank(State state) {
    return !state.contains(getConfig())
        || StringUtils.isBlank(state.getProp(getConfig()));
  }

  /**
   * Validates the value when it is non-blank and accepts blank value
   * - A blank configuration is considered valid
   * - Any properly formed Long is considered valid
   * @param state state
   * @return true if blank or non-blank and valid
   */
  @Override
  public boolean isValid(State state) {
    if (!isBlank(state)) try {
      // Properly formed Long string is valid
      long value = Long.parseLong(state.getProp(getConfig()));
      return value >= getMinValue() && value <= getMaxValue();
    } catch (Exception e) {
      LOG.error(alertMessage(state), e.getMessage());
      return false;
    }
    return true;
  }

  /**
   * Validates the value when it is non-blank and rejects blank value
   * - only properly formed Long string is considered valid
   *
   * @param state source state
   * @return true when the configuration is non-blank and valid
   */
  public boolean isValidNonblank(State state) {
    return !isBlank(state) && isValid(state);
  }

  /**
   * Retrieves property value from state object if valid and not blank
   * otherwise, return default value
   *
   * @param state state
   * @return property value if non-blank and valid, otherwise the default value
   * @see #get(State)
   */
  protected Long getValidNonblankWithDefault(State state) {
    if (isValidNonblank(state)) {
      return Long.parseLong(state.getProp(getConfig()));
    }
    return getDefaultValue();
  }

  /**
   * The default millisecond value is the property value itself
   * @param state state
   * @return the configured value
   */
  @Override
  public Long getMillis(State state) {
    return getValidNonblankWithDefault(state);
  }
}

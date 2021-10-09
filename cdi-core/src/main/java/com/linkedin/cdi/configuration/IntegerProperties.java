// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.configuration;

import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.configuration.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Integer type of property has default defaultValue of 0
 */
public class IntegerProperties extends MultistageProperties<Integer> {
  private static final Logger LOG = LoggerFactory.getLogger(IntegerProperties.class);

  IntegerProperties(String config) {
    super(config, Integer.class, 0);
  }

  IntegerProperties(String config, Integer defaultValue) {
    super(config, Integer.class, defaultValue);
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
   * - Any properly formed Integer is considered valid
   * @param state state
   * @return true if blank or non-blank and valid
   */
  @Override
  public boolean isValid(State state) {
    if (!isBlank(state)) try {
      // Properly formed Integer string is valid
      Integer.parseInt(state.getProp(getConfig()));
    } catch (Exception e) {
      LOG.error(getConfig(), e.getMessage());
      return false;
    }
    return true;
  }

  /**
   * Validates the value when it is non-blank and rejects blank value
   * - only properly formed Integer string is considered valid
   *
   * @param state source state
   * @return true when the configuration is non-blank and valid
   */
  public boolean validateNonblank(State state) {
    return !isBlank(state) && isValid(state);
  }

  /**
   * Retrieves property value from state object if valid and not blank
   * otherwise, return default value
   *
   * This method raises an exception when a non-conforming value is configured.
   *
   * @param state state
   * @return property value if non-blank and valid, otherwise the default value
   */
  @Override
  public Integer getProp(State state) {
    if (validateNonblank(state)) {
      return Integer.parseInt(state.getProp(getConfig()));
    }
    return getDefaultValueWithCheck(state);
  }

  /**
   * Retrieves property value from state object if valid and not blank
   * otherwise, return default value
   *
   * This method will keep quiet if the configuration is incorrect
   *
   * @param state state
   * @return property value if non-blank and valid, otherwise the default value
   */
  public Integer getValidNonblankWithDefault(State state) {
    if (validateNonblank(state)) {
      return Integer.parseInt(state.getProp(getConfig()));
    }
    return getDefaultValue();
  }
}

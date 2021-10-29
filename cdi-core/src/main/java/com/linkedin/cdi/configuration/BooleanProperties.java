// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.configuration;

import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.configuration.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A Boolean type of property has no default defaultValue, and each property
 * has to supply a default value, true or false
 */
public class BooleanProperties extends MultistageProperties<Boolean> {
  private static final Logger LOG = LoggerFactory.getLogger(BooleanProperties.class);

  /**
   * Constructor with explicit default value
   * @param config property name
   * @param defaultValue default value
   */
  BooleanProperties(String config, Boolean defaultValue) {
    super(config, Boolean.class, defaultValue);
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
   * - Any properly formed Boolean is considered valid
   * @param state state
   * @return true if blank or non-blank and valid
   */
  @Override
  public boolean isValid(State state) {
    if (!isBlank(state)) try {
      String value = state.getProp(getConfig());
      if (!value.toLowerCase().matches("true|false")) {
        LOG.error(errorMessage(state));
        return false;
      }
      // Properly formed Boolean string is valid
      Boolean.parseBoolean(state.getProp(getConfig()));
    } catch (Exception e) {
      LOG.error(errorMessage(state), e.getMessage());
      return false;
    }
    return true;
  }

  /**
   * Validates the value when it is non-blank and rejects blank value
   * - only properly formed Boolean string is considered valid
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
   */
  protected Boolean getValidNonblankWithDefault(State state) {
    if (isValidNonblank(state)) {
      return Boolean.parseBoolean(state.getProp(getConfig()));
    }
    return getDefaultValue();
  }
}

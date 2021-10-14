// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.configuration;

import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.configuration.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A String type of property has default defaultValue of ""
 */
public class StringProperties extends MultistageProperties<String> {
  private static final Logger LOG = LoggerFactory.getLogger(StringProperties.class);

  StringProperties(String config) {
    super(config, String.class, StringUtils.EMPTY);
  }

  StringProperties(String config, String defaultValue) {
    super(config, String.class, defaultValue);
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
   * Strings blank or not are always valid
   * @param state state
   * @return true
   */
  @Override
  public boolean isValid(State state) {
    return true;
  }

  /**
   * Validates the value when it is non-blank and rejects blank value
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
   * @param state state
   * @return property value if non-blank and valid, otherwise the default value
   * @see #getProp(State)
   */
  public String getValidNonblankWithDefault(State state) {
    if (validateNonblank(state)) {
      return state.getProp(getConfig());
    }
    return getDefaultValue();
  }
}

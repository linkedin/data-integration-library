// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.configuration;

import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.configuration.State;

import static com.linkedin.cdi.configuration.StaticConstants.*;


/**
 * Define a structure for MultistageProperties
 * @author chrli
 */
public abstract class MultistageProperties<T> {
  private final String config;
  private final Class<T> className;
  private final T defaultValue;
  private final T maxValue;
  private final T minValue;

  final public String getDocUrl() {
    return DOC_BASE_URL + "/parameters/" + getConfig() + ".md";
  }

  final public String getSummaryUrl() {
    return DOC_BASE_URL + "/parameters/summary.md";
  }

  final public String errorMessage(State state) {
    return String.format(EXCEPTION_INCORRECT_CONFIGURATION,
        getConfig(), state.getProp(getConfig(), StringUtils.EMPTY), getDocUrl());
  }

  final public String info(State state) {
    if (get(state).equals(getDefaultValue())) {
      return String.format("Property %s has default value %s", getConfig(), get(state));
    }
    return String.format("Property %s has non-default value %s", getConfig(), get(state));
  }

  final public String getConfig() {
    return config;
  }

  final public Class<?> getClassName() {
    return className;
  }

  public T getDefaultValue() {
    return defaultValue;
  }

  public T getMaxValue() {
    return maxValue;
  }

  public T getMinValue() {
    return minValue;
  }

  /**
   * Constructor with explicit default, and implicit max and min
   *
   * All subclasses don't need to have a max or min
   *
   * @param config property name
   * @param defaultValue default value
   */
  MultistageProperties(String config, Class<T> className, T defaultValue) {
    this.config = config;
    this.className = className;
    this.defaultValue = defaultValue;
    this.maxValue = null;
    this.minValue = null;
  }

  /**
   * Constructor with explicit default, max, and min for subclasses using min/max control
   *
   * @param config property name
   * @param defaultValue default value
   * @param maxValue max value
   * @param minValue min value
   */
  MultistageProperties(String config, Class<T> className, T defaultValue, T maxValue, T minValue) {
    this.config = config;
    this.className = className;
    this.defaultValue = defaultValue;
    this.maxValue = maxValue;
    this.minValue = minValue;
  }

  @Override
  public String toString() {
    return config;
  }

  /**
   * Converts configured value to a millisecond value if supported
   * @param state state
   * @return milliseconds value if supported
   */
  public Long getMillis(State state) {
    throw new RuntimeException("Not Supported");
  }

  /**
   * Validates the value when it is non-blank and accepts blank value
   * @param state state
   * @return sub-classes should override
   */
  abstract public boolean isValid(State state);

  /**
   * Validates the value when it is blank
   * @param state state
   * @return sub-classes should override
   */
  abstract public boolean isBlank(State state);

  /**
   * Validates the value when it is non-blank and rejects blank value
   * @param state source state
   * @return true when the configuration is non-blank and valid
   */
  abstract public boolean isValidNonblank(State state);

  /**
   * Retrieves property value from state object if valid and not blank
   * otherwise, return default value of the property type
   *
   * @param state state
   * @return subclasses should override
   */
  public T get(State state) {
    return getValidNonblankWithDefault(state);
  }

  /**
   * Retrieves property value from state object if valid and not blank
   * otherwise, return default value
   * @param state state
   * @return subclasses should override
   */
  abstract protected T getValidNonblankWithDefault(State state);

  public boolean isDeprecated() {
    return false;
  }
}

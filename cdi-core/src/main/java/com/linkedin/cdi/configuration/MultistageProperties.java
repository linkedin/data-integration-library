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

  public String getConfig() {
    return config;
  }

  public Class<?> getClassName() {
    return className;
  }

  public T getDefaultValue() {
    return defaultValue;
  }

  MultistageProperties(String config, Class<T> className, T defaultValue) {
    this.config = config;
    this.className = className;
    this.defaultValue = defaultValue;
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


}

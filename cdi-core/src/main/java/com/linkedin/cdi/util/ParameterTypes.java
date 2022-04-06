// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.util;

/**
 *
 * ParameterTypes defines a list of acceptable types of parameters in
 * Gobblin configuration file for jobs using multi-stage connectors.
 *
 */

public enum ParameterTypes {
  PRIMITIVE("primitive"),
  LIST("list"),
  OBJECT("object"),
  WATERMARK("watermark"),
  SESSION("session"),
  PAGESTART("pagestart"),
  PAGESIZE("pagesize"),
  PAGENO("pageno"),
  JSONARRAY("jsonarray"),
  JSONOBJECT("jsonobject");

  private final String name;

  ParameterTypes(String name) {
    this.name = name;
  }

  @Override
  public String toString() {
    return name;
  }
}
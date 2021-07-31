// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.exception;

/**
 * An {@link Exception} thrown when it can be retried
 */
public class RetriableAuthenticationException extends Exception {
  public RetriableAuthenticationException(String message) {
    super(message);
  }
}
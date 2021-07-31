// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.connection;

import com.linkedin.cdi.exception.RetriableAuthenticationException;
import com.linkedin.cdi.util.WorkUnitStatus;

/**
 * The connection interface defines core methods that an Extractor would call
 * to establish a transmission channel with the data provider or data receiver.
 *
 * @author Chris Li
 */
public interface Connection {
  /**
   * The common method among all connections, read or write, is the execute(). This
   * method expects a work unit status object as input parameter, and it gives out
   * a new work unit object as output.
   * @param status the input WorkUnitStatus object
   * @return the output of the execution in a WorkUnitStatus object
   * @throws RetriableAuthenticationException exception to allow retry at higher level
   */
  WorkUnitStatus execute(final WorkUnitStatus status) throws RetriableAuthenticationException;
  /**
   * Close the connection and pool of connections if applicable
   * @param message the message to send to the other end of connection upon closing
   * @return true if connections are successfully closed, or false if connections are not
   * closed successfully
   */
  boolean closeAll(final String message);
  /**
   * Close the current cursor or stream if applicable
   * @return true if closeStream was successful, or false if not able to close the stream
   */
  boolean closeStream();
}

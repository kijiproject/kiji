/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.rest.core;

import javax.ws.rs.core.Response.Status;

/**
 * Class that encapsulates the exception information to be sent back to the client
 * (in JSON) so that it's both parseable by a JSON client as well as human readable in the
 * browser. The standard DropWizard exceptions aren't very informative where as the exceptions
 * that we throw in the resource classes have some details as to why they failed.
 *
 */
final class ExceptionWrapper {

  private Throwable mWrappedException;
  private Status mStatus;

  /**
   * Default constructor.
   *
   * @param status is the status to display
   * @param exception is the exception that was thrown
   */
  public ExceptionWrapper(Status status, Throwable exception) {
    super();
    this.mStatus = status;
    if (exception.getCause() != null) {
      this.mWrappedException = exception.getCause();
    } else {
      this.mWrappedException = exception;
    }
  }

  /**
   * Gets the name of the exception so that there is some context.
   *
   * @return the name of the exception for context.
   */
  public String getExceptionName() {
    return mWrappedException.getClass().getSimpleName();
  }

  /**
   * Returns the exception message string for some understanding of why things failed.
   *
   * @return the exception message string for some understanding of why things failed.
   */
  public String getException() {
    return mWrappedException.getMessage();
  }

  /**
   * Returns the HTTP status code.
   *
   * @return the status code
   */
  public int getStatus() {
    return mStatus.getStatusCode();
  }
}

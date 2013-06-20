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

package org.kiji.delegation;

import org.kiji.annotations.ApiAudience;

/**
 * Thrown by a {@link Lookup} object when we cannot find a provider
 * for the specified interface or abstract class.
 */
@ApiAudience.Public
public final class NoSuchProviderException extends RuntimeException {

  /**
   * Create a NoSuchProviderException with the specified message.
   *
   * @param message the message to display in the exception.
   */
  NoSuchProviderException(String message) {
    super(message);
  }

  /**
   * Create a NoSuchProviderException with the specified message and cause.
   *
   * @param message the message to display in the exception.
   * @param cause the underlying exception
   */
  NoSuchProviderException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Create a NoSuchProviderException suggesting that we cannot
   * provide the required class.
   *
   * @param clazz the class we cannot load an instance of.
   */
  NoSuchProviderException(Class<?> clazz) {
    super("No such provider for abstract interface [" + clazz.getName() + "]");
  }
}

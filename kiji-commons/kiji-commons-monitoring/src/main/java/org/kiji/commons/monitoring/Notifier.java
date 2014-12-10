/**
 * (c) Copyright 2014 WibiData, Inc.
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

package org.kiji.commons.monitoring;

import java.io.Closeable;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

import org.kiji.annotations.ApiStability;

/**
 * An interface that services can use to send notifications about error events. Notifications can
 * take a variety of forms, including logging or sending an email.
 *
 * In particular, services should use a notifier when encountering recoverable errors which would
 * otherwise not be signaled in any way (by, for example, returning a 500 HTTP error code).
 *
 * Notifier implementations must be thread safe.
 */
@ApiStability.Experimental
@ThreadSafe
public interface Notifier extends Closeable {

  /**
   * Notify of an error event while attempting an action.
   *
   * The event consists of an action, an attributes map of context-specific properties, and the
   * exception.
   *
   * Typically, the action should be statically known in the context of the notification location.
   * In particular, some notifier implementations may have trouble handling an unbounded number of
   * actions. By convention, actions should be period-separated paths, for instance:
   * "org.kiji.rest.put".
   *
   * Service authors may use this method to notify of an error occurrence.
   *
   * This method must be non-blocking.
   *
   * @param action The action which experienced an error.
   * @param attributes Important properties in the context of the error.
   * @param error The error.
   */
  void error(String action, Map<String, String> attributes, Throwable error);
}

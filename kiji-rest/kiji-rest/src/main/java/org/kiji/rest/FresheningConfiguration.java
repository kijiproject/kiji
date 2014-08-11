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

package org.kiji.rest;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.yammer.dropwizard.config.Configuration;

/**
 * The Java object which is deserialized from the YAML configuration file under 'freshening'.
 */
public class FresheningConfiguration extends Configuration {

  /** Whether to freshen rows by default. */
  @JsonProperty("freshen")
  private boolean mFreshen = true;

  /** Default timeout in ms for freshening. */
  @JsonProperty("timeout")
  private long mTimeout = 100;

  /**
   * Constructor for tests.
   * @param freshen Whether to freshen columns by default.
   * @param timeout Length of default freshening timeout in ms.
   */
  public FresheningConfiguration(boolean freshen, long timeout) {
    mFreshen = freshen;
    mTimeout = timeout;
  }

  /**
   * Default constructor.
   */
  public FresheningConfiguration() {
  }

  /**
   * Get whether to freshen by default.
   * @return Whether to freshen by default.
   **/
  public final boolean isFreshen() {
    return mFreshen;
  }

  /**
   * Get default freshening timeout.
   * @return Freshening timeout default.
   * */
  public long getTimeout() {
    return mTimeout;
  }
}

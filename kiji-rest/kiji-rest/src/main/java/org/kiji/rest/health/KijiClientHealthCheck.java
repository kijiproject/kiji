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

package org.kiji.rest.health;

import com.yammer.metrics.core.HealthCheck;

import org.kiji.rest.ManagedKijiClient;

/**
 * A HealthCheck for checking the health of a ManagedKijiClient.
 */
public class KijiClientHealthCheck extends HealthCheck {

  private final ManagedKijiClient mKijiClient;

  /**
   * Create a HealthCheck instance for the supplied ManagedKijiClient.
   *
   * @param kijiClient to check health of.
   */
  public KijiClientHealthCheck(ManagedKijiClient kijiClient) {
    super("KijiClientHealthCheck");
    this.mKijiClient = kijiClient;
  }

  /** {@inheritDoc} */
  @Override
  protected Result check() throws Exception {
   return mKijiClient.checkHealth();
  }
}

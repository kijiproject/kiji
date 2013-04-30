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

import java.io.IOException;

import com.yammer.metrics.core.HealthCheck;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiURI;

/**
 * A health check of whether the parametrized instances can be accessed.
 */
public class InstanceHealthCheck extends HealthCheck {
  /** The URI of the instance. */
  private final KijiURI mKijiURI;

  /**
   * Constructor parametrized by the URI of the instance which is available to REST clients.
   *
   * @param kijiURI The URI of the instance to check.
   */
  public InstanceHealthCheck(final KijiURI kijiURI) {
    super(kijiURI.toString());
    mKijiURI = kijiURI;
  }

  /**
   * Opens and closes the instance hopefully without exceptions,
   * otherwise an exception is trickled to the REST client.
   *
   * @return Healthy result upon successful open/release of instance.
   * @throws IOException if there is an error in checking for the instance.
   */
  @Override
  protected final Result check() throws IOException {
    // TODO shouldn't this catch the execption and return Result.unhealthy()?
    Kiji kiji = Kiji.Factory.open(mKijiURI);
    kiji.release();
    return Result.healthy();
  }
}

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

package org.kiji.chopsticks;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.KijiRowData;

/**
 * Acts as a wrapper around [[KijiRowData]]. Instances of this class can be reused in MapReduce
 * jobs to wrap [[KijiRowData]] read from a Kiji table.
 */
@ApiAudience.Private
@ApiStability.Unstable
final class KijiValue {
  /** The row data being wrapped by this instance. */
  private var currentValue: KijiRowData = null

  /**
   * @return the row data wrapped by this instance.
   */
  def get(): KijiRowData = currentValue

  /**
   * Sets the Kiji row data wrapped by this instance.
   *
   * @param value that will be wrapped by this instance.
   */
  def set(value: KijiRowData) {
    currentValue = value;
  }
}

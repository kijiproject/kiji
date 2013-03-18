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

package org.kiji.chopsticks

import java.io.Serializable

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.chopsticks.ColumnRequest.InputOptions
import org.kiji.schema.filter.KijiColumnFilter

/**
 * Represents a request for a column in a Kiji table.
 *
 * @param name The name of the column.
 * @param inputOptions Input options for requesting the column.
 */
@ApiAudience.Public
@ApiStability.Unstable
final case class ColumnRequest(
    name: String,
    inputOptions: InputOptions = InputOptions())
    extends Serializable

object ColumnRequest {
  /**
   * Provides the ability to specify InputOptions for a column.
   *
   * @param maxVersions Max versions to return.
   * @param KijiColumnFilter Filters columns to request.
   */
  @ApiAudience.Public
  @ApiStability.Unstable
  final case class InputOptions(
        maxVersions: Int = 1,
        filter: KijiColumnFilter = null) extends Serializable {
  }
}

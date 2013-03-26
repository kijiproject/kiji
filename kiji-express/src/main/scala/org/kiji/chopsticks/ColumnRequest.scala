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
import org.kiji.schema.util.KijiNameValidator

/**
 * Represents a request for a column in a Kiji table.
 */
@ApiAudience.Public
@ApiStability.Unstable
sealed trait ColumnRequest extends Serializable

/**
 * Represents a request for a fully-qualified column from a Kiji table.
 *
 * @param family of the column.
 * @param qualifier of the column.
 * @param inputOptions for requesting the column.
 */
final case class QualifiedColumn(
    family: String,
    qualifier: String,
    inputOptions: InputOptions = InputOptions())
    extends ColumnRequest {
  KijiNameValidator.validateLayoutName(family)
  KijiNameValidator.validateLayoutName(qualifier)
}

/**
 * Represents a request for a column family from a Kiji table.
 *
 * @param family of the column family.
 * @param inputOptions for requesting the column family.
 */
final case class ColumnFamily(
    family: String,
    inputOptions: InputOptions = InputOptions())
    extends ColumnRequest {
  KijiNameValidator.validateLayoutName(family)
}

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
      filter: KijiColumnFilter = null)
      extends Serializable {
  }
}

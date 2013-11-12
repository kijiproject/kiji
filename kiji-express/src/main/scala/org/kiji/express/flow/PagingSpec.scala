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

package org.kiji.express.flow

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance

/**
 * A specification of the type of paging to use.
 */
@ApiAudience.Public
@ApiStability.Stable
@Inheritance.Sealed
sealed trait PagingSpec {
  private[kiji] def cellsPerPage: Option[Int]
}

/**
 * Module to provide PagingSpec implementations.
 */
@ApiAudience.Public
@ApiStability.Stable
object PagingSpec {
  /**
   * Specifies that paging should not be used. Each row requested from Kiji tables will be fully
   * materialized into RAM.
   */
  @ApiAudience.Public
  @ApiStability.Stable
  case object Off extends PagingSpec {
    override val cellsPerPage: Option[Int] = None
  }

  /**
   * Specifies that paging should be enabled. Each page will contain the specified number of cells.
   *
   * Note: Cells may not all be the same size (in bytes).
   *
   * @param count of the cells per page.
   */
  @ApiAudience.Public
  @ApiStability.Stable
  @Inheritance.Sealed
  final case class Cells(count: Int) extends PagingSpec {
    override val cellsPerPage: Option[Int] = Some(count)
  }
}

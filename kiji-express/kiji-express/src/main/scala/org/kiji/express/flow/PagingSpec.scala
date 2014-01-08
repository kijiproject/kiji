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
 *
 * If paging is disabled, all cells from the specified column will be loaded into memory at once. If
 * the size of the loaded cells exceeds the capacity of the task machine's main memory, the Scalding
 * job will fail at runtime with an [[java.lang.OutOfMemoryError]].
 *
 * @note Defaults to [[org.kiji.express.flow.PagingSpec.Off PagingSpec.Off]].
 * @example
 *      - [[org.kiji.express.flow.PagingSpec.Cells PagingSpec.Cells]] - Paging by number of cells:
 *        {{{
 *          // Will read in 10 cells from Kiji at a time. The appropriate number of cells to be
 *          // paged in depends on the size of each cell. Users should try to retrieve as many cells
 *          // as possible without causing an OutOfMemoryError in order to increase performance.
 *          .withPagingSpec(PagingSpec.Cells(10))
 *        }}}
 *      - [[org.kiji.express.flow.PagingSpec.Off PagingSpec.Off]] - No paging:
 *        {{{
 *          // Will disable paging entirely.
 *          .withPagingSpec(PagingSpec.Off)
 *        }}}
 * @see [[org.kiji.express.flow.ColumnInputSpec]] for more PagingSpec usage information.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
sealed trait PagingSpec extends Serializable {
  private[kiji] def cellsPerPage: Option[Int]
}

/**
 * Provides [[org.kiji.express.flow.PagingSpec]] implementations.
 */
@ApiAudience.Public
@ApiStability.Experimental
object PagingSpec {
  /**
   * Specifies that paging should not be used. Each row requested from Kiji tables will be fully
   * materialized into RAM.
   *
   * @see [[org.kiji.express.flow.PagingSpec]] for more PagingSpec usage information.
   */
  @ApiAudience.Public
  @ApiStability.Experimental
  case object Off extends PagingSpec {
    override val cellsPerPage: Option[Int] = None
  }

  /**
   * Specifies that paging should be enabled. Each page will contain the specified number of cells.
   *
   * @note Cells may not all be the same size (in bytes).
   * @see [[org.kiji.express.flow.PagingSpec]] for more PagingSpec usage information.
   *
   * @param count of the cells per page.
   */
  @ApiAudience.Public
  @ApiStability.Experimental
  @Inheritance.Sealed
  final case class Cells(count: Int) extends PagingSpec {
    override val cellsPerPage: Option[Int] = Some(count)
  }
}

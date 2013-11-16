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
 * These can be used in [[org.kiji.express.flow.ColumnInputSpec]].  The default in
 * [[org.kiji.express.flow.ColumnInputSpec]] is [[org.kiji.express.flow.PagingSpec.Off]], which
 * disables all paging.  With paging disabled, all cells from the specified column will be loaded
 * into memory at once.  If the size of all of the loaded cells exceeds the capacity of the
 * receiving machine's main memory, the Scalding job will fail at runtime.  In these cases you can
 * specify how many cells should be paged into memory at a time with
 * [[org.kiji.express.flow.PagingSpec.Cells]]:
 *
 * {{{
 *   paging = PagingSpec.Cells(10)
 * }}}
 *
 * This will load only 10 cells at a time into memory.
 *
 * The appropriate number of cells to be paged in depends on the size of each cell. Users should
 * try to retrieve as many cells as possible (without causing OOME) in order to increase
 * performance.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
sealed trait PagingSpec {
  private[kiji] def cellsPerPage: Option[Int]
}

/**
 * Module to provide PagingSpec implementations.
 */
@ApiAudience.Public
@ApiStability.Experimental
object PagingSpec {
  /**
   * Specifies that paging should not be used. Each row requested from Kiji tables will be fully
   * materialized into RAM.
   */
  @ApiAudience.Public
  @ApiStability.Experimental
  case object Off extends PagingSpec {
    override val cellsPerPage: Option[Int] = None
  }

  /**
   * Specifies that paging should be enabled. Each page will contain the specified number of cells.
   *
   * Note: Cells may not all be the same size (in bytes).
   *
   * Note: There are known issues with paging in Express.  See https://jira.kiji.org/browse/EXP-326,
   *    [[org.kiji.express.flow.TransientSeq]] and [[org.kiji.express.flow.TransientSeqSuite]] for
   *    details and workarounds.
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

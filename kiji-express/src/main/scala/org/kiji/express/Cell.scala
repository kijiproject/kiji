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

package org.kiji.express

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.schema.KijiCell

/**
 * A cell from a Kiji table containing some datum, addressed by a family, qualifier,
 * and version timestamp.
 *
 * @param family of the Kiji table cell.
 * @param qualifier of the Kiji table cell.
 * @param version  of the Kiji table cell.
 * @param datum in the Kiji table cell.
 * @tparam T is the type of the datum in the cell.
 */
@ApiAudience.Public
@ApiStability.Experimental
case class Cell[T] private[express] (family: String, qualifier: String, version: Long, datum: T)

/**
* A factory for creating cells used in KijiExpress from cells used in the Kiji Java API.
*/
object Cell {
  /**
   * Creates a new cell (for use in KijiExpress) from the contents of a cell produced by the
   * Kiji Java API.
   *
   * @param cell from a Kiji table produced by the Java API.
   * @tparam T is the type of the datum that this cell contains.
   * @return a cell for use in KijiExpress, with the same family, qualifier, timestamp,
   *     and datum as cell produced by the Java API.
   */
  private[express] def apply[T](cell: KijiCell[T]): Cell[T] = {
    new Cell[T](
        cell.getFamily,
        cell.getQualifier,
        cell.getTimestamp.longValue,
        AvroUtil.decodeGenericFromJava(cell.getData).asInstanceOf[T])
  }
}

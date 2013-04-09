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
 * Cell represents a cell in a Kiji table. It contains the family, qualifier, and timestamp that
 * uniquely locates the cell within a table, as well as the datum itself.
 *
 * @param family of the Kiji table cell.
 * @param qualifier of the Kiji table cell.
 * @param version  of the Kiji table cell.
 * @param datum in the Kiji table cell.
 * @tparam T the type of the datum in the cell.
 */
@ApiAudience.Public
@ApiStability.Experimental
case class Cell[T] private[express] (family: String, qualifier: String, version: Long, datum: T)
/**
* Currently, this companion object is only a factory for creating cells.
*/
object Cell {
  import KijiScheme.convertJavaTypes
  /**
   * Creates a new Cell using the contents of the specified [[org.kiji.schema.KijiCell]].
   *
   * @param cell used to instantiate a Cell.
   * @tparam T The type of the datum that this Cell contains.
   * @return a Cell with the same family, qualifier, timestamp, and datum as the passed in KijiCell.
   */
  private[express] def apply[T](cell: KijiCell[T]): Cell[T] = {
    new Cell[T](convertJavaTypes(cell.getFamily).asInstanceOf[String],
        convertJavaTypes(cell.getQualifier).asInstanceOf[String],
        convertJavaTypes(cell.getTimestamp).asInstanceOf[Long],
        convertJavaTypes(cell.getData).asInstanceOf[T])
  }
}

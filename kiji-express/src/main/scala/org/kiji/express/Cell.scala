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

import scala.annotation.implicitNotFound

import org.apache.hadoop.hbase.HConstants

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.express.util.AvroUtil
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
@Inheritance.Sealed
case class Cell[T] (
    family: String,
    qualifier: String,
    version: Long = HConstants.LATEST_TIMESTAMP,
    datum: T)

/**
 * A factory for creating cells used in KijiExpress from cells used in the Kiji Java API.
 */
object Cell {
  /**
   * Creates a new cell (for use in KijiExpress) from the contents of a cell produced by the Kiji
   * Java API.
   *
   * @param cell from a Kiji table produced by the Java API.
   * @tparam T is the type of the datum that this cell contains.
   * @return a cell for use in KijiExpress, with the same family, qualifier, timestamp,
   *     and datum as cell produced by the Java API.
   */
  private[kiji] def apply[T](cell: KijiCell[T]): Cell[T] = {
    new Cell[T](
        cell.getFamily,
        cell.getQualifier,
        cell.getTimestamp.longValue,
        AvroUtil.avroToScala(cell.getData).asInstanceOf[T])
  }

  /**
   * Provides an implementation of the `scala.Ordering` trait that sorts [[org.kiji.express.Cell]]s
   * by value.
   *
   * @tparam T is the type of the datum in the [[org.kiji.express.Cell]].
   * @return an ordering that sorts cells by their value.
   */
  @implicitNotFound("type of the datum in the cells is not Orderable.")
  implicit def valueOrder[T](implicit order: Ordering[T]): Ordering[Cell[T]] = {
    Ordering.by { cell: Cell[T] => cell.datum }
  }

  /**
   * Provides an implementation of the `scala.Ordering` trait that sorts [[org.kiji.express.Cell]]s
   * by timestamp/version.
   *
   * @tparam T is the type of the datum in the [[org.kiji.express.Cell]].
   * @return an ordering that sorts cells by timestamp.
   */
  def versionOrder[T]: Ordering[Cell[T]] = {
    Ordering.by { cell: Cell[T] => cell.version }
  }

  /**
   * Provides an implementation of the `scala.Ordering` trait that sorts [[org.kiji.express.Cell]]s
   * by timestamp/version.
   *
   * @tparam T is the type of the datum in the [[org.kiji.express.Cell]].
   * @return an ordering that sorts cells by timestamp.
   */
  def timestampOrder[T]: Ordering[Cell[T]] = versionOrder[T]

  /**
   * Provides an implementation of the `scala.Ordering` trait that sorts [[org.kiji.express.Cell]]s
   * by qualifier.
   *
   * @tparam T is the type of the datum in the [[org.kiji.express.Cell]].
   * @return an ordering that sorts cells by qualifier.
   */
  def qualifierOrder[T]: Ordering[Cell[T]] = {
    Ordering.by { cell: Cell[T] =>
      (cell.family, cell.qualifier)
    }
  }
}

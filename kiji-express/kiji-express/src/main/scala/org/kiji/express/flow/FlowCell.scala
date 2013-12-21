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

import scala.annotation.implicitNotFound
import scala.collection.mutable.ListBuffer

import org.apache.hadoop.hbase.HConstants

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.express.flow.util.AvroUtil
import org.kiji.schema.KijiCell

/**
 * A container for data from a Kiji table. Contains some datum tagged with a column family, column
 * qualifier, and version. Flow cells are provided when requesting a Kiji table column or column
 * family.
 *
 * Example of accessing data stored within a flow cell:
 * {{{
 *   // Extracts the data stored within cell.
 *   val myData: T = cell.datum
 *
 *   // Extracts the family, qualifier, and version of the cell.
 *   val myFamily: String = cell.family
 *   val myQualifier: String = cell.qualifier
 *   val myVersion: Long = cell.version
 * }}}
 *
 * @tparam T is the type of the datum in the cell.
 * @param family of columns that this cell comes from.
 * @param qualifier of the column that this cell comes from.
 * @param version of the column data that this cell represents. Defaults to the latest timestamp.
 * @param datum stored in this cell.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
case class FlowCell[T] (
    family: String,
    qualifier: String,
    version: Long = HConstants.LATEST_TIMESTAMP,
    datum: T)

/**
 * Companion object containing factory methods for creating flow cells and orderings for sorting
 * cells.
 */
@ApiAudience.Public
@ApiStability.Experimental
object FlowCell {
  /**
   * Creates an object that contains the coordinates (family, qualifier, and version) of data in a
   * Kiji table along with the data itself.
   *
   * @tparam T is the type of the datum that this cell contains.
   * @param cell from a Kiji table produced by the Java API.
   * @return a FlowCell for use in KijiExpress containing the same family, qualifier, version, and
   *     datum as the provided KijiCell.
   */
  private[kiji] def apply[T](cell: KijiCell[T]): FlowCell[T] = {
    new FlowCell[T](
        cell.getFamily,
        cell.getQualifier,
        cell.getTimestamp.longValue,
        AvroUtil.avroToScala(cell.getData).asInstanceOf[T])
  }

  /**
   * Provides an implementation of the `scala.Ordering` trait that sorts
   * [[org.kiji.express.flow.FlowCell]]s by value.
   *
   * @tparam T is the type of the datum in the [[org.kiji.express.flow.FlowCell]].
   * @return an ordering that sorts cells by their value.
   */
  @implicitNotFound("The type of the datum in the cells is not Orderable. You may be trying to " +
      "order a cell that contains a complex type (such as an avro record).")
  implicit def valueOrder[T](implicit order: Ordering[T]): Ordering[FlowCell[T]] = {
    Ordering.by { cell: FlowCell[T] => cell.datum }
  }

  /**
   * Provides an implementation of the `scala.Ordering` trait that sorts
   * [[org.kiji.express.flow.FlowCell]]s by version.
   *
   * @tparam T is the type of the datum in the [[org.kiji.express.flow.FlowCell]].
   * @return an ordering that sorts cells by version.
   */
  def versionOrder[T]: Ordering[FlowCell[T]] = {
    Ordering.by { cell: FlowCell[T] => cell.version }
  }

  /**
   * Provides an implementation of the `scala.Ordering` trait that sorts
   * [[org.kiji.express.flow.FlowCell]]s first by the cell's family and then by it's qualifier.
   *
   * @tparam T is the type of the datum in the [[org.kiji.express.flow.FlowCell]].
   * @return an ordering that sorts cells by qualifier.
   */
  def qualifierOrder[T]: Ordering[FlowCell[T]] = {
    Ordering.by { cell: FlowCell[T] =>
      (cell.family, cell.qualifier)
    }
  }

  /**
   * Groups a Seq of flow cells by qualifier. The cells must belong to the same column family, and
   * must be in lexicographic sorted order by qualifier (otherwise behavior is undefined).  This is
   * the default order for scan results from a Kiji table.
   *
   * This method is functionally equivalent to, but more memory efficient than, calling
   * `cells.groupBy(_.qualifier).toSeq`.
   *
   * This method will realize each group into memory individually, so it is safe to iterate through
   * the groups as long as no individual group is larger than the heap. It is NOT safe to
   * hold onto the individual groups while iterating, or call methods that force the seq to fully
   * realize (such as .toList or .toMap) if the provided sequence of flow cells is larger than
   * memory.
   *
   * @param cells to be grouped by qualifier.
   * @tparam T type of datum in cells.
   * @return groups of FlowCells by qualifier.
   */
  def groupByQualifier[T](cells: Seq[FlowCell[T]]): Seq[(String, Seq[FlowCell[T]])] = {
    val itr = cells.iterator.buffered

    new Iterator[(String, Seq[FlowCell[T]])] {
      def hasNext: Boolean = itr.hasNext

      def next(): (String, Seq[FlowCell[T]]) = {
        val qualifier = itr.head.qualifier
        val builder = ListBuffer[FlowCell[T]]()
        while(itr.hasNext && itr.head.qualifier == qualifier) {
          builder += itr.head
          itr.next()
        }
        qualifier -> builder.toList
      }
    }.toStream
  }
}

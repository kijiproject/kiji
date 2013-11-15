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

import org.apache.hadoop.hbase.HConstants

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.express.flow.util.AvroUtil
import org.kiji.schema.KijiCell

/**
 * A `FlowCell` from a Kiji table containing some datum, addressed by a family, qualifier,
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
case class FlowCell[T] (
    family: String,
    qualifier: String,
    version: Long = HConstants.LATEST_TIMESTAMP,
    datum: T)

/**
 * A factory for creating cells used in KijiExpress from cells used in the Kiji Java API.
 */
@ApiAudience.Public
@ApiStability.Experimental
object FlowCell {
  /**
   * Creates an object that contains the coordinates (family, qualifier, and version/timestamp) of
   * data in a Kiji table along with the data itself.
   *
   * @tparam T is the type of the datum that this cell contains.
   * @param cell from a Kiji table produced by the Java API.
   * @return a FlowCell for use in KijiExpress containing the same family, qualifier, timestamp, and
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
   * [[org.kiji.express.flow.FlowCell]]s by timestamp/version.
   *
   * @tparam T is the type of the datum in the [[org.kiji.express.flow.FlowCell]].
   * @return an ordering that sorts cells by timestamp.
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
}

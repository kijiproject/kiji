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

package org.kiji.express.flow.framework.hfile

import java.lang.UnsupportedOperationException

import cascading.tap.Tap
import com.google.common.base.Objects
import com.twitter.scalding.AccessMode
import com.twitter.scalding.HadoopTest
import com.twitter.scalding.Hdfs
import com.twitter.scalding.Mode
import com.twitter.scalding.Source
import com.twitter.scalding.Write

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.express.flow.All
import org.kiji.express.flow.ColumnRequestOutput
import org.kiji.express.flow.TimeRange
import org.kiji.express.flow.framework.KijiScheme

/**
 * A read or write view of a Kiji table.
 *
 * A Scalding `Source` provides a view of a data source that can be read as Scalding tuples. It
 * is comprised of a Cascading tap [[cascading.tap.Tap]], which describes where the data is and how
 * to access it, and a Cascading Scheme [[cascading.scheme.Scheme]], which describes how to read
 * and interpret the data.
 *
 * When reading from a Kiji table, a `KijiSource` will provide a view of a Kiji table as a
 * collection of tuples that correspond to rows from the Kiji table. Which columns will be read
 * and how they are associated with tuple fields can be configured,
 * as well as the time span that cells retrieved must belong to.
 *
 * When writing to a Kiji table, a `KijiSource` views a Kiji table as a collection of tuples that
 * correspond to cells from the Kiji table. Each tuple to be written must provide a cell address
 * by specifying a Kiji `EntityID` in the tuple field `entityId`, a value to be written in a
 * configurable field, and (optionally) a timestamp in a configurable field.
 *
 * End-users cannot directly obtain instances of `KijiSource`. Instead,
 * they should use the factory methods provided as part of the [[org.kiji.express.flow]] module.
 *
 * @param tableAddress is a Kiji URI addressing the Kiji table to read or write to.
 * @param timeRange that cells read must belong to. Ignored when the source is used to write.
 * @param timestampField is the name of a tuple field that will contain cell timestamp when the
 *     source is used for writing. Specify `None` to write all
 *     cells at the current time.
 * @param loggingInterval The interval at which to log skipped rows.
 * @param columns is a one-to-one mapping from field names to Kiji columns. When reading,
 *     the columns in the map will be read into their associated tuple fields. When
 *     writing, values from the tuple fields will be written to their associated column.
 */
@ApiAudience.Framework
@ApiStability.Experimental
class HFileKijiSource private[express] (
    val tableAddress: String,
    val hFileOutput: String,
    val timeRange: TimeRange,
    val timestampField: Option[Symbol],
    val loggingInterval: Long,
    val columns: Map[Symbol, ColumnRequestOutput]
) extends Source {
  import org.kiji.express.flow.KijiSource._

  /**
   * Creates a Scheme that writes to/reads from a Kiji table for usage with
   * the hadoop runner.
   */
  override val hdfsScheme: KijiScheme.HadoopScheme =
    new HFileKijiScheme(timeRange, timestampField, loggingInterval, convertKeysToStrings(columns))
      // This cast is required due to Scheme being defined with invariant type parameters.
      .asInstanceOf[KijiScheme.HadoopScheme]

  /**
   * Create a connection to the physical data source (also known as a Tap in Cascading)
   * which, in this case, is a [[org.kiji.schema.KijiTable]].
   *
   * @param readOrWrite Specifies if this source is to be used for reading or writing.
   * @param mode Specifies which job runner/flow planner is being used.
   * @return A tap to use for this data source.
   */
  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = {
    val tap: Tap[_, _, _] = mode match {
      // Production taps.
      case Hdfs(_, _) => new HFileKijiTap(tableAddress, hdfsScheme, hFileOutput)

      // Test taps.
      case HadoopTest(conf, buffers) => {
        readOrWrite match {
          case Write => {
            new HFileKijiTap(tableAddress, hdfsScheme, hFileOutput)
          }
          case _ => throw new UnsupportedOperationException("Read unsupported")
        }
      }
      // Delegate any other tap types to Source's default behaviour.
      case _ => super.createTap(readOrWrite)(mode)
    }

    return tap
  }

  override def toString: String = {
    Objects
        .toStringHelper(this)
        .add("tableAddress", tableAddress)
        .add("timeRange", timeRange)
        .add("timestampField", timestampField)
        .add("loggingInterval", loggingInterval)
        .add("columns", columns)
        .toString
  }

  override def equals(other: Any): Boolean = {
    other match {
      case source: HFileKijiSource => {
        Objects.equal(tableAddress, source.tableAddress) &&
        Objects.equal(hFileOutput, source.hFileOutput) &&
        Objects.equal(columns, source.columns) &&
        Objects.equal(timestampField, source.timestampField) &&
        Objects.equal(timeRange, source.timeRange)
      }
      case _ => false
    }
  }

  override def hashCode(): Int =
      Objects.hashCode(tableAddress, hFileOutput, columns, timestampField, timeRange)
}

/**
 * Private Scalding source implementation whose scheme is the SemiNullScheme that
 * simply sinks tuples to an output for later writing to HFiles.
 */
private final class HFileSource(
    override val tableAddress: String,
    override val hFileOutput: String
) extends HFileKijiSource(tableAddress, hFileOutput, All, None, 0, Map()) {
  /**
   * Creates a Scheme that writes to/reads from a Kiji table for usage with
   * the hadoop runner.
   */
  override val hdfsScheme: KijiScheme.HadoopScheme =
      new SemiNullScheme().asInstanceOf[KijiScheme.HadoopScheme]
}

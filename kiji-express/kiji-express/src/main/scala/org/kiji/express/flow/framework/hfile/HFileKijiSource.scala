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

import cascading.scheme.Scheme
import cascading.tap.Tap
import com.twitter.scalding.AccessMode
import com.twitter.scalding.HadoopSchemeInstance
import com.twitter.scalding.HadoopTest
import com.twitter.scalding.Hdfs
import com.twitter.scalding.Mode
import com.twitter.scalding.Source
import com.twitter.scalding.Write
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.RecordReader

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.express.flow.ColumnOutputSpec

/**
 * A read or write view of a Kiji table.
 *
 * A Scalding `Source` provides a view of a data source that can be read as Scalding tuples. It
 * is comprised of a Cascading tap [[cascading.tap.Tap]], which describes where the data is and how
 * to access it, and a Cascading Scheme [[cascading.scheme.Scheme]], which describes how to read
 * and interpret the data.
 *
 * An `HFileKijiSource` should never be used for reading.  It is intended to be used for writing
 * to HFiles formatted for bulk-loading into Kiji.
 *
 * When writing to a Kiji table, a `HFileKijiSource` views a Kiji table as a collection of tuples
 * that correspond to cells from the Kiji table. Each tuple to be written must provide a cell
 * address by specifying a Kiji `EntityID` in the tuple field `entityId`, a value to be written in a
 * configurable field, and (optionally) a timestamp in a configurable field.
 *
 * End-users cannot directly obtain instances of `KijiSource`. Instead,
 * they should use the factory methods provided as part of the
 * [[org.kiji.express.flow.framework.hfile]] module.
 *
 * @param tableAddress is a Kiji URI addressing the Kiji table to read or write to.
 * @param timestampField is the name of a tuple field that will contain cell timestamp when the
 *     source is used for writing. Specify `None` to write all cells at the current time.
 * @param columns is a one-to-one mapping from field names to Kiji columns. When reading,
 *     the columns in the map will be read into their associated tuple fields. When
 *     writing, values from the tuple fields will be written to their associated column.
 */
@ApiAudience.Framework
@ApiStability.Stable
final case class HFileKijiSource private[express] (
    tableAddress: String,
    hFileOutput: String,
    timestampField: Option[Symbol],
    columns: Map[Symbol, ColumnOutputSpec]
) extends Source {
  import org.kiji.express.flow.KijiSource._

  private val hfileScheme = new HFileKijiScheme(timestampField, convertKeysToStrings(columns))

  /**
   * Creates a Scheme that writes to/reads from a Kiji table for usage with
   * the hadoop runner.
   */
  override val hdfsScheme: Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _] =
    HadoopSchemeInstance(hfileScheme)

  /**
   * Create a connection to the physical data source (also known as a Tap in Cascading)
   * which, in this case, is a [[org.kiji.schema.KijiTable]].
   *
   * @param readOrWrite Specifies if this source is to be used for reading or writing.
   * @param mode Specifies which job runner/flow planner is being used.
   * @return A tap to use for this data source.
   */
  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = {
    (readOrWrite, mode) match {
      case (Write, Hdfs(_, _)) =>  new HFileKijiTap(tableAddress, hfileScheme, hFileOutput)
      case (Write, HadoopTest(_, _)) => new HFileKijiTap(tableAddress, hfileScheme, hFileOutput)
      case (Write, _) => throw new UnsupportedOperationException("Cascading local mode unsupported")
      case (_, _) => throw new UnsupportedOperationException("Read unsupported")
    }
  }
}

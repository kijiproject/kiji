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

package org.kiji.chopsticks

import java.util.HashMap

import scala.collection.JavaConverters._

import cascading.scheme.Scheme
import cascading.tap.Tap
import com.twitter.scalding.AccessMode
import com.twitter.scalding.Hdfs
import com.twitter.scalding.Local
import com.twitter.scalding.Mode
import com.twitter.scalding.Source
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.RecordReader
import org.apache.hadoop.mapred.OutputCollector

import org.kiji.lang.Column
import org.kiji.lang.KijiScheme
import org.kiji.lang.KijiTap
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiURI
import org.kiji.schema.filter.KijiColumnFilter

/**
 * Facilitates writing to and reading from a Kiji table.
 */
class KijiSource(
    val tableURI: String,
    val columns: Map[Symbol, Column])
    extends Source {

  type HadoopScheme = Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _]

  /** Convert scala columns definition into its corresponding java variety. */
  private def convertColumnMap(columnMap: Map[Symbol, Column]): java.util.Map[String, Column] = {
    val wrapped = columnMap
        .map { case (symbol, column) => (symbol.name, column) }
        .asJava

    // Copy the map into a HashMap because scala's JavaConverters aren't serializable.
    new java.util.HashMap(wrapped);
  }

  /** Converts a string into a [[KijiURI]]. */
  private def uri(uriString: String): KijiURI = KijiURI.newBuilder(uriString).build()

  /** Creates a new [[KijiScheme]] instance. */
  def kijiScheme: KijiScheme = new KijiScheme(convertColumnMap(columns))

  /**
   * Creates a Scheme that writes to/reads from a Kiji table for usage with
   * the hadoop runner.
   */
  override def hdfsScheme: HadoopScheme = kijiScheme
      .asInstanceOf[HadoopScheme]

  /**
   * Create a connection to the physical data source (also known as a Tap in Cascading)
   * which, in this case, is a [[org.kiji.schema.KijiTable]].
   */
  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = {
    val tap = mode match {
      case Hdfs(_,_) => new KijiTap(uri(tableURI), kijiScheme).asInstanceOf[Tap[_, _, _]]

      // TODO(CHOP-30): Add a case for test modes. The KijiSource should ideally read from
      //     a fake kiji instance.
      case _ => super.createTap(readOrWrite)(mode)
    }

    return tap
  }
}

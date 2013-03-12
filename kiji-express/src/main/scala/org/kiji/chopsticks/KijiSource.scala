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

import scala.collection.JavaConverters._
import scala.collection.mutable.Buffer
import java.io.InputStream
import java.io.OutputStream
import java.util.NavigableMap
import java.util.Properties

import cascading.flow.FlowProcess
import cascading.scheme.ConcreteCall
import cascading.scheme.Scheme
import cascading.scheme.SinkCall
import cascading.tap.Tap
import cascading.tuple.Fields
import cascading.tuple.Tuple
import cascading.tuple.TupleEntry
import com.google.common.base.Objects
import com.twitter.scalding.AccessMode
import com.twitter.scalding.HadoopTest
import com.twitter.scalding.Hdfs
import com.twitter.scalding.Local
import com.twitter.scalding.Mode
import com.twitter.scalding.Read
import com.twitter.scalding.Source
import com.twitter.scalding.Test
import com.twitter.scalding.Write
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.RecordReader

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.chopsticks.DSL._
import org.kiji.lang.Column
import org.kiji.lang.KijiScheme
import org.kiji.lang.KijiTap
import org.kiji.lang.LocalKijiScheme
import org.kiji.lang.LocalKijiTap
import org.kiji.schema.EntityId
import org.kiji.schema.Kiji
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiRowData
import org.kiji.schema.KijiTableWriter
import org.kiji.schema.KijiURI

/**
 * Facilitates writing to and reading from a Kiji table.
 *
 * @param tableAddress Address of the target KijiTable. This should be provided as a
 *     [[org.kiji.schema.KijiURI]].
 * @param columns Mapping from field name to Kiji column name.
 */
@ApiAudience.Framework
@ApiStability.Unstable
final class KijiSource(
    val tableAddress: String,
    val columns: Map[Symbol, Column])
    extends Source {
  import KijiSource._

  private type HadoopScheme = Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _]

  /** The URI of the target Kiji table. */
  private val tableUri: KijiURI = KijiURI.newBuilder(tableAddress).build()
  /** A Kiji scheme intended to be used with Scalding/Cascading's hdfs mode. */
  private val kijiScheme: KijiScheme = new KijiScheme(convertColumnMap(columns))
  /** A Kiji scheme intended to be used with Scalding/Cascading's local mode. */
  private val localKijiScheme: LocalKijiScheme = new LocalKijiScheme(convertColumnMap(columns))

  /**
   * Convert scala columns definition into its corresponding java variety.
   *
   * @param columnMap Mapping from field name to Kiji column name.
   * @return Java map from field name to column definition.
   */
  private def convertColumnMap(columnMap: Map[Symbol, Column]): java.util.Map[String, Column] = {
    val wrapped = columnMap
        .map { case (symbol, column) => (symbol.name, column) }
        .asJava

    // Copy the map into a HashMap because scala's JavaConverters aren't serializable.
    new java.util.HashMap(wrapped)
  }

  /**
   * Takes a buffer containing rows and writes them to the table at the specified uri.
   *
   * @param rows Tuples to write to populate the table with.
   * @param fields Field names for elements in the tuple.
   */
  private def populateTestTable(rows: Buffer[Tuple], fields: Fields) {
    // Open a table writer.
    val writer =
        doAndRelease(Kiji.Factory.open(tableUri)) { kiji =>
          doAndRelease(kiji.openTable(tableUri.getTable())) { table =>
            table.openTableWriter()
          }
        }

    // Write the desired rows to the table.
    try {
      rows.foreach { row: Tuple =>
        val tupleEntry = new TupleEntry(fields, row)
        val iterator = fields.iterator()

        // Get the entity id field.
        val entityIdField = iterator.next().toString()
        val entityId = tupleEntry
            .getObject(entityIdField)
            .asInstanceOf[EntityId]

        // Iterate through fields in the tuple, adding each one.
        while (iterator.hasNext()) {
          val field = iterator.next().toString()
          val columnName = new KijiColumnName(columns(Symbol(field)).name())

          // Get the timeline to be written.
          val timeline: NavigableMap[Long, Any] = tupleEntry.getObject(field)
              .asInstanceOf[NavigableMap[Long, Any]]

          // Write the timeline to the table.
          for (entry <- timeline.asScala) {
            val (key, value) = entry
            writer.put(
                entityId,
                columnName.getFamily(),
                columnName.getQualifier(),
                key,
                value)
          }
        }
      }
    } finally {
      writer.close()
    }
  }

  /**
   * Creates a Scheme that writes to/reads from a Kiji table for usage with
   * the hadoop runner.
   */
  override val hdfsScheme: HadoopScheme = kijiScheme
      // This cast is required due to Scheme being defined with invariant type parameters.
      .asInstanceOf[HadoopScheme]

  /**
   * Creates a Scheme that writes to/reads from a Kiji table for usage with
   * the local runner.
   */
  override val localScheme: LocalScheme = localKijiScheme
      // This cast is required due to Scheme being defined with invariant type parameters.
      .asInstanceOf[LocalScheme]

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
      case Hdfs(_,_) => new KijiTap(tableUri, kijiScheme).asInstanceOf[Tap[_, _, _]]
      case Local(_) => new LocalKijiTap(tableUri, localKijiScheme).asInstanceOf[Tap[_, _, _]]

      // Test taps.
      // TODO(CHOP-38): Add support for Hadoop based integration tests.
      case HadoopTest(_, _) => sys.error("HadoopTest mode not supported")
      case Test(buffers) => {
        readOrWrite match {
          // Use Kiji's local tap and scheme when reading.
          case Read => {
            val scheme = localKijiScheme
            populateTestTable(buffers(this), scheme.getSourceFields())

            new LocalKijiTap(tableUri, scheme).asInstanceOf[Tap[_, _, _]]
          }

          // After performing a write, use TestKijiScheme to populate the output buffer.
          case Write => {
            val scheme = new TestKijiScheme(buffers(this), convertColumnMap(columns))

            new LocalKijiTap(tableUri, scheme).asInstanceOf[Tap[_, _, _]]
          }
        }
      }

      // Delegate any other tap types to Source's default behaviour.
      case _ => super.createTap(readOrWrite)(mode)
    }

    return tap
  }

  override def equals(other: Any): Boolean = {
    other match {
      case source: KijiSource =>
        Objects.equal(tableAddress, source.tableAddress) &&
        Objects.equal(columns, source.columns)
      case _ => false
    }
  }

  override def hashCode(): Int = Objects.hashCode(tableAddress, columns)
}

/**
 * Contains a private, inner class used by [[org.kiji.chopsticks.KijiSource]] when working with
 * tests.
 */
object KijiSource {
  /**
   * A LocalKijiScheme that loads rows in a table into the provided buffer. This class
   * should only be used during tests.
   *
   * @param buffer Buffer to fill with post-job table rows for tests.
   * @param columns Scalding field name to Kiji column name mapping.
   */
  private class TestKijiScheme(
      val buffer: Buffer[Tuple],
      val columns: java.util.Map[String, Column])
      extends LocalKijiScheme(columns) {
    override def sinkConfInit(
        process: FlowProcess[Properties],
        tap: Tap[Properties, InputStream, OutputStream],
        conf: Properties) {
      super.sinkConfInit(process, tap, conf)

      // Store output uri as input uri.
      tap.sourceConfInit(process, conf)
    }

    override def sinkCleanup(
        process: FlowProcess[Properties],
        sinkCall: SinkCall[KijiTableWriter, OutputStream]) {
      super.sink(process, sinkCall)

      // Read table into buffer.
      val sourceCall: ConcreteCall[java.util.Iterator[KijiRowData], InputStream] =
          new ConcreteCall()
      sourceCall.setIncomingEntry(new TupleEntry())
      sourcePrepare(process, sourceCall)
      while (source(process, sourceCall)) {
        buffer += sourceCall.getIncomingEntry().getTuple()
      }
      sourceCleanup(process, sourceCall)
    }
  }
}

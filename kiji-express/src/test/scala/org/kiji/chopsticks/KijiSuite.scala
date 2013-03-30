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

import java.io.InputStream
import java.util.NavigableMap
import java.util.TreeMap

import com.twitter.scalding.TupleConversions
import org.scalatest.FunSuite

import org.kiji.chopsticks.Resources._
import org.kiji.schema.EntityId
import org.kiji.schema.EntityIdFactory
import org.kiji.schema.Kiji
import org.kiji.schema.KijiTable
import org.kiji.schema.avro.RowKeyEncoding;
import org.kiji.schema.avro.RowKeyFormat2;
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.layout.KijiTableLayouts
import org.kiji.schema.shell.api.Client
import org.kiji.schema.util.InstanceBuilder

/** Contains convenience methods for writing tests that use Kiji. */
trait KijiSuite
    extends FunSuite
    with TupleConversions {
  /**
   * Builds a [[org.kiji.schema.RawEntityId]] with the provided string.
   *
   * @param identifier Desired contents of the entity id.
   * @return A [[org.kiji.schema.RawEntityId]].
   */
  def id(identifier: String): EntityId = {
    val rowKeyFmt: RowKeyFormat2 = RowKeyFormat2.newBuilder()
        .setEncoding(RowKeyEncoding.RAW)
        .build()

    val factory = EntityIdFactory.getFactory(rowKeyFmt)

    factory.getEntityId(identifier)
  }

  /**
   * Builds a timeline from a single value. This will assign the current time as the timestamp for
   * value.
   *
   * @tparam T Type of the values contained within desired timeline.
   * @param value Single value to put in the timeline.
   * @return A timeline containing the desired value.
   */
  def singleton[T](value: T): NavigableMap[Long, T] = {
    val timeline: NavigableMap[Long, T] = new TreeMap()
    timeline.put(Long.MaxValue, value)
    timeline
  }

  /**
   * Builds a timeline containing no values.  This can be used to test for behavior of missing
   * values.
   *
   * @tparam T type of the values in the returned timeline.
   * @return An empty timeline.
   */
  def missing[T](): NavigableMap[Long, T] = {
    new TreeMap[Long, T]()
  }

  /**
   * Builds a timeline from a list of timestamp, value pairs.
   *
   * @tparam T Type of the values contained within desired timeline.
   * @param values Timestamp value pairs to build the timeline with.
   * @return A timeline containing the specified timestamp value pairs.
   */
  def timeline[T](values: (Long, T)*): NavigableMap[Long, T] = {
    values.foldLeft(new TreeMap[Long, T]) { (tree, entry) =>
      val (timestamp, value) = entry

      tree.put(timestamp, value)
      tree
    }
  }

  /**
   * Constructs and starts a test Kiji instance that uses fake-hbase.
   *
   * @param instanceName Name of the test Kiji instance.
   * @return A handle to the Kiji instance that just got constructed. Note: This object must be
   *     {{{release()}}}'d once it is no longer needed.
   */
  def makeTestKiji(instanceName: String = "default"): Kiji = {
    new InstanceBuilder(instanceName).build()
  }

  /**
   * Constructs and starts a test Kiji instance and creates a Kiji table.
   *
   * @param layout Layout of the test table.
   * @param instanceName Name of the Kiji instance to create.
   * @return A handle to the Kiji table that just got constructed. Note: This object must be
   *     {{{release()}}}'d once it is no longer needed.
   */
  def makeTestKijiTable(
      layout: KijiTableLayout,
      instanceName: String = "default"): KijiTable = {
    val tableName = layout.getName()
    val kiji: Kiji = new InstanceBuilder(instanceName)
        .withTable(tableName, layout)
        .build()

    val table: KijiTable = kiji.openTable(tableName)
    kiji.release()
    table
  }

  /**
   * Loads a [[org.kiji.schema.layout.KijiTableLayout]] from the classpath. See
   * [[org.kiji.schema.layout.KijiTableLayouts]] for some layouts that get put on the classpath
   * by KijiSchema.
   *
   * @param resourcePath Path to the layout definition file.
   * @return The layout contained within the provided resource.
   */
  def layout(resourcePath: String): KijiTableLayout = {
    val tableLayoutDef = KijiTableLayouts.getLayout(resourcePath)
    KijiTableLayout.newLayout(tableLayoutDef)
  }

  /**
   * Executes a series of KijiSchema Shell DDL commands, separated by `;`.
   *
   * @param kiji to execute the commands against.
   * @param commands to execute against the Kiji instance.
   */
  def executeDDLString(kiji: Kiji, commands: String) {
    doAndClose(Client.newInstance(kiji.getURI)) { ddlClient =>
      ddlClient.executeUpdate(commands)
    }
  }

  /**
   * Executes a series of KijiSchema Shell DDL commands, separated by `;`.
   *
   * @param kiji to execute the commands against.
   * @param stream to read a series of commands to execute against the Kiji instance.
   */
  def executeDDLStream(kiji: Kiji, stream: InputStream) {
    doAndClose(Client.newInstance(kiji.getURI)) { ddlClient =>
      ddlClient.executeStream(stream)
    }
  }

  /**
   * Executes a series of KijiSchema Shell DDL commands, separated by `;`.
   *
   * @param kiji to execute the commands against.
   * @param resourcePath to the classpath resource that a series of commands to execute
   *     against the Kiji instance will be read from.
   */
  def executeDDLResource(kiji: Kiji, resourcePath: String) {
    executeDDLStream(kiji, getClass.getClassLoader.getResourceAsStream(resourcePath))
  }
}

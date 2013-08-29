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

import java.io.InputStream

import com.twitter.scalding.{Hdfs, TupleConversions}
import org.scalatest.FunSuite

import org.kiji.express.util.Resources._
import org.kiji.schema.{EntityId => JEntityId}
import org.kiji.schema.EntityIdFactory
import org.kiji.schema.Kiji
import org.kiji.schema.KijiTable
import org.kiji.schema.avro.RowKeyEncoding;
import org.kiji.schema.avro.RowKeyFormat2;
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.layout.KijiTableLayouts
import org.kiji.schema.shell.api.Client
import org.kiji.schema.util.InstanceBuilder
import org.apache.hadoop.hbase.HBaseConfiguration

/** Contains convenience methods for writing tests that use Kiji. */
trait KijiSuite
    extends FunSuite
    with TupleConversions {
  /**
   * Builds a slice containing no values.  This can be used to test for behavior of missing
   * values.
   *
   * @tparam T type of the values in the returned slice.
   * @return an empty slice.
   */
  def missing[T](): KijiSlice[T] = {
    val emptyList = List[Cell[T]]()
    new KijiSlice[T](emptyList)
  }

  /**
   * Builds a slice from a group type column name and list of version, value pairs.
   *
   * @tparam T type of the values contained within desired slice.
   * @param columnName for a group type family, of the form "family:qualifier"
   * @param values pairs of (version, value) to build the slice with.
   * @return a slice containing the specified cells.
   */
  def slice[T](columnName: String, values: (Long, T)*): KijiSlice[T] = {
    val columnComponents: Array[String] = columnName.split(":")
    require(columnComponents.length == 2, "The column name must be of the form" +
      " \"family:qualifier\", with no extra colons. ")
    val cells: Seq[Cell[T]] = values.toSeq.map {input: (Long, T) =>
      val (version, value) = input
      Cell(columnComponents(0), columnComponents(1), version, value)
    }.toSeq
    new KijiSlice[T](cells)
  }

  /**
   * Builds a slice from a map type column name and a list of qualifier, version, value triples.
   *
   * @tparam T type of the values contained within desired slice.
   * @param columnName for a map type family, of the form "family"
   * @param values are triples of (qualifier, version, value) to build the slice with.
   * @return a slice containing the specified cells.
   */
  def mapSlice[T](columnName: String, values: (String, Long, T)*): KijiSlice[T] = {
    val columnComponents: Array[String] = columnName.split(":")
    require(columnComponents.length == 1, "The column name must be of the form" +
      " \"family\", with no extra colons. ")
    val cells: Seq[Cell[T]] = values.map {input: (String, Long, T) =>
      val (qualifier, version, value) = input
      Cell(columnComponents(0), qualifier, version, value)
    }.toSeq
    new KijiSlice[T](cells)
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

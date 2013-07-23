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

package org.kiji.express.flow.framework

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.util.Properties
import java.util.UUID

import cascading.flow.FlowProcess
import cascading.flow.hadoop.util.HadoopUtil
import cascading.scheme.Scheme
import cascading.tap.Tap
import cascading.tuple.TupleEntryCollector
import cascading.tuple.TupleEntryIterator
import cascading.tuple.TupleEntrySchemeCollector
import cascading.tuple.TupleEntrySchemeIterator
import com.google.common.base.Objects
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.mapred.JobConf

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.express.util.Resources.doAndRelease
import org.kiji.mapreduce.framework.KijiConfKeys
import org.kiji.schema.Kiji
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiURI

/**
 * A Kiji-specific implementation of a Cascading `Tap`, which defines the location of a Kiji table.
 *
 * LocalKijiTap is responsible for configuring a local Cascading job with the settings necessary to
 * read from a Kiji table.
 *
 * LocalKijiTap must be used with [[org.kiji.express.flow.framework.LocalKijiScheme]] to perform
 * decoding of cells in a Kiji table. [[org.kiji.express.flow.KijiSource]] handles the creation
 * of both LocalKijiScheme and LocalKijiTap in KijiExpress.
 *
 * @param uri of the Kiji table to read or write from.
 * @param scheme that will convert data read from Kiji into Cascading's tuple model.
 */
@ApiAudience.Framework
@ApiStability.Experimental
private[express] class LocalKijiTap(
    uri: KijiURI,
    private val scheme: LocalKijiScheme)
    extends Tap[Properties, InputStream, OutputStream](
        scheme.asInstanceOf[Scheme[Properties, InputStream, OutputStream, _, _]]) {

  /** The URI of the table to be read through this tap. */
  private val tableUri: String = uri.toString()

  /** A unique identifier for this tap instance. */
  private val id: String = UUID.randomUUID().toString()

  /**
   * Sets any configuration options that are required for running a local job
   * that reads from a Kiji table. This method gets called on the client machine
   * during job setup.
   *
   * @param flow being built.
   * @param conf to which we will add the table uri.
   */
  override def sourceConfInit(
      flow: FlowProcess[Properties],
      conf: Properties) {
    // Store the input table.
    conf.setProperty(KijiConfKeys.KIJI_INPUT_TABLE_URI, tableUri);

    super.sourceConfInit(flow, conf);
  }

  /**
   * Sets any configuration options that are required for running a local job
   * that writes to a Kiji table. This method gets called on the client machine
   * during job setup.
   *
   * @param flow being built.
   * @param conf to which we will add the table uri.
   */
  override def sinkConfInit(
      flow: FlowProcess[Properties],
      conf: Properties) {
    // Store the output table.
    conf.setProperty(KijiConfKeys.KIJI_OUTPUT_TABLE_URI, tableUri);

    super.sinkConfInit(flow, conf);
  }

  /**
   * Provides a string representing the resource this `Tap` instance represents.
   *
   * @return a java UUID representing this KijiTap instance. Note: This does not return the uri of
   *     the Kiji table being used by this tap to allow jobs that read from or write to the same
   *     table to have different data request options.
   */
  override def getIdentifier(): String = id

  /**
   * Opens any resources required to read from a Kiji table.
   *
   * @param flow being run.
   * @param input stream that will read from the desired Kiji table.
   * @return an iterator that reads rows from the desired Kiji table.
   */
  override def openForRead(
      flow: FlowProcess[Properties],
      input: InputStream): TupleEntryIterator = {
    return new TupleEntrySchemeIterator[Properties, InputStream](
        flow,
        getScheme(),
        // scalastyle:off null
        if (null == input) new ByteArrayInputStream(Array()) else input,
        // scalastyle:on null
        getIdentifier());
  }

  /**
   * Opens any resources required to write from a Kiji table.
   *
   * @param flow being run.
   * @param output stream that will write to the desired Kiji table. Note: This is ignored
   *     currently since writing to a Kiji table is currently implemented without using an output
   *     format by writing to the table directly from
   *     [[org.kiji.express.flow.framework.KijiScheme]].
   * @return a collector that writes tuples to the desired Kiji table.
   */
  override def openForWrite(
      flow: FlowProcess[Properties],
      output: OutputStream): TupleEntryCollector = {
    return new TupleEntrySchemeCollector[Properties, OutputStream](
        flow,
        getScheme(),
        // scalastyle:off null
        if (null == output) new ByteArrayOutputStream() else output,
        // scalastyle:on null
        getIdentifier());
  }

  /**
   * Builds any resources required to read from or write to a Kiji table.
   *
   * Note: KijiExpress currently does not support automatic creation of Kiji tables.
   *
   * @param conf containing settings for this flow.
   * @return true if required resources were created successfully.
   * @throws UnsupportedOperationException always.
   */
  override def createResource(conf: Properties): Boolean = {
    throw new UnsupportedOperationException("KijiTap does not support creating tables for you.")
  }

  /**
   * Deletes any unnecessary resources used to read from or write to a Kiji table.
   *
   * Note: KijiExpress currently does not support automatic deletion of Kiji tables.
   *
   * @param conf containing settings for this flow.
   * @return true if superfluous resources were deleted successfully.
   * @throws UnsupportedOperationException always.
   */
  override def deleteResource(conf: Properties): Boolean = {
    throw new UnsupportedOperationException("KijiTap does not support deleting tables for you.")
  }

  /**
   * Determines if the Kiji table this `Tap` instance points to exists.
   *
   * @param conf containing settings for this flow.
   * @return true if the target Kiji table exists.
   */
  override def resourceExists(conf: Properties): Boolean = {
    val uri: KijiURI = KijiURI.newBuilder(tableUri).build()
    val jobConf: JobConf = HadoopUtil.createJobConf(conf,
        new JobConf(HBaseConfiguration.create()))
    doAndRelease(Kiji.Factory.open(uri, jobConf)) { kiji: Kiji =>
      kiji.getTableNames().contains(uri.getTable())
    }
  }

  /**
   * Gets the time that the target Kiji table was last modified.
   *
   * Note: This will always return the current timestamp.
   *
   * @param conf containing settings for this flow.
   * @return the current time.
   */
  override def getModifiedTime(conf: Properties): Long = System.currentTimeMillis()

  override def equals(other: Any): Boolean = {
    other match {
      case tap: LocalKijiTap => ((tableUri == tap.tableUri)
          && (scheme == tap.scheme)
          && (id == tap.id))
      case _ => false
    }
  }

  override def hashCode(): Int = Objects.hashCode(tableUri, scheme, id)

  /**
   * Checks whether the instance, tables, and columns this tap uses can be accessed.
   *
   * @throws KijiExpressValidationException if the tables and columns are not accessible when this
   *    is called.
   */
  private[express] def validate(conf: Properties): Unit = {
    val kijiUri: KijiURI = KijiURI.newBuilder(tableUri).build()
    val columnNames: List[KijiColumnName] =
        scheme.columns.values.map { column => column.getColumnName() }.toList
    KijiTap.validate(kijiUri, columnNames, HadoopUtil.createJobConf(conf,
        new JobConf(HBaseConfiguration.create())))
  }
}

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

import java.util.UUID

import cascading.flow.FlowProcess
import cascading.scheme.Scheme
import cascading.tap.Tap
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeCollector
import cascading.tuple.TupleEntryCollector
import cascading.tuple.TupleEntryIterator
import com.twitter.elephantbird.mapred.output.DeprecatedOutputFormatWrapper
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.FileOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.RecordReader

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.express.flow.framework.KijiKey
import org.kiji.express.flow.framework.KijiScheme
import org.kiji.express.flow.framework.KijiValue
import org.kiji.mapreduce.framework.KijiConfKeys
import org.kiji.mapreduce.impl.HFileWriterContext
import org.kiji.mapreduce.output.framework.KijiHFileOutputFormat

/**
 * A Kiji-specific implementation of a Cascading `Tap`, which defines how data is to be read from
 * and written to a particular endpoint. This implementation only handles writing to Kiji
 * formatted HFiles to be bulk loaded into HBase.
 *
 * HFileKijiTap must be used with [[org.kiji.express.flow.framework.hfile.HFileKijiScheme]]
 * to perform decoding of cells in a Kiji table. [[org.kiji.express.flow.KijiSource]] handles
 * the creation of both HFileKijiScheme and HFileKijiTap in KijiExpress.
 *
 * @param tableUri of the Kiji table to read or write from.
 * @param scheme that will convert data read from Kiji into Cascading's tuple model.
 * @param hFileOutput is the location where the HFiles will be written to.
 */
@ApiAudience.Framework
@ApiStability.Experimental
private[express] class HFileKijiTap(
  private val tableUri: String,
  private val scheme: KijiScheme.HadoopScheme,
  private val hFileOutput: String)
    extends Tap[JobConf, RecordReader[KijiKey, KijiValue], OutputCollector[_, _]](
        scheme.asInstanceOf[Scheme[JobConf, RecordReader[KijiKey, KijiValue],
            OutputCollector[_, _], _, _]]) {

  /** Unique identifier for this KijiTap instance. */
  private val id: String = UUID.randomUUID().toString()

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
   * @param recordReader that will read from the desired Kiji table.
   * @return an iterator that reads rows from the desired Kiji table.
   */
  override def openForRead(
      flow: FlowProcess[JobConf],
      recordReader: RecordReader[KijiKey, KijiValue]): TupleEntryIterator = {
    null
  }

  /**
   * Opens any resources required to write from a Kiji table.
   *
   * @param flow being run.
   * @param outputCollector that will write to the desired Kiji table.
   *
   * @return a collector that writes tuples to the desired Kiji table.
   */
  override def openForWrite(
      flow: FlowProcess[JobConf],
      outputCollector: OutputCollector[_, _]): TupleEntryCollector = {

    new HadoopTupleEntrySchemeCollector(
        flow,
        this.asInstanceOf[Tap[JobConf, RecordReader[_, _], OutputCollector[_, _]]],
        outputCollector)
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
  override def createResource(conf: JobConf): Boolean = {
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
  override def deleteResource(conf: JobConf): Boolean = {
    throw new UnsupportedOperationException("KijiTap does not support deleting tables for you.")
  }

  /**
   * Gets the time that the target Kiji table was last modified.
   *
   * Note: This will always return the current timestamp.
   *
   * @param conf containing settings for this flow.
   * @return the current time.
   */
  override def getModifiedTime(conf: JobConf): Long = System.currentTimeMillis()

  /**
   * Determines if the Kiji table this `Tap` instance points to exists.
   *
   * @param conf containing settings for this flow.
   * @return true if the target Kiji table exists.
   */
  override def resourceExists(conf: JobConf): Boolean = {
    true
  }

  /**
   * Sets any configuration options that are required for running a MapReduce job
   * that writes to a Kiji table. This method gets called on the client machine
   * during job setup.
   *
   * @param flow being built.
   * @param conf to which we will add the table uri.
   */
  override def sinkConfInit(flow: FlowProcess[JobConf], conf: JobConf) {
    FileOutputFormat.setOutputPath(conf, new Path(hFileOutput))
    DeprecatedOutputFormatWrapper.setOutputFormat(classOf[KijiHFileOutputFormat], conf)
    val hfContext = classOf[HFileWriterContext].getName()
    conf.set(KijiConfKeys.KIJI_TABLE_CONTEXT_CLASS, hfContext)
    // Store the output table.
    conf.set(KijiConfKeys.KIJI_OUTPUT_TABLE_URI, tableUri)

    super.sinkConfInit(flow, conf)
  }
}

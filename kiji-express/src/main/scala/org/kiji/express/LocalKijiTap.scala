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

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.util.Properties
import java.util.UUID

import cascading.flow.FlowProcess
import cascading.scheme.Scheme
import cascading.tap.Tap
import cascading.tuple.TupleEntryCollector
import cascading.tuple.TupleEntryIterator
import cascading.tuple.TupleEntrySchemeCollector
import cascading.tuple.TupleEntrySchemeIterator
import com.google.common.base.Objects

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.express.Resources.doAndRelease
import org.kiji.mapreduce.framework.KijiConfKeys
import org.kiji.schema.Kiji
import org.kiji.schema.KijiURI

/**
 * A Scalding `Tap` for reading data from a Kiji table.
 *
 * <p>
 *   This tap is responsible for configuring a local job to read from a Kiji table.
 * </p>
 * <p>
 *   Note: Warnings about a missing serialVersionUID are ignored here. When KijiTap is serialized,
 *   the result is not persisted anywhere making serialVersionUID unnecessary.
 * </p>
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
   * that reads from a Kiji table.
   *
   * @param process Current Cascading flow being built.
   * @param conf The job configuration object.
   */
  override def sourceConfInit(
      process: FlowProcess[Properties],
      conf: Properties) {
    // Store the input table.
    conf.setProperty(KijiConfKeys.KIJI_INPUT_TABLE_URI, tableUri);

    super.sourceConfInit(process, conf);
  }

  /**
   * Sets any configuration options that are required for running a local job
   * that writes to a Kiji table.
   *
   * @param process Current Cascading flow being built.
   * @param conf The job configuration object.
   */
  override def sinkConfInit(
      process: FlowProcess[Properties],
      conf: Properties) {
    // Store the output table.
    conf.setProperty(KijiConfKeys.KIJI_OUTPUT_TABLE_URI, tableUri);

    super.sinkConfInit(process, conf);
  }

  override def getIdentifier(): String = id

  override def openForRead(
      process: FlowProcess[Properties],
      input: InputStream): TupleEntryIterator = {
    return new TupleEntrySchemeIterator[Properties, InputStream](
        process,
        getScheme(),
        // scalastyle:off null
        if (null == input) new ByteArrayInputStream(Array()) else input,
        // scalastyle:on null
        getIdentifier());
  }

  override def openForWrite(
      process: FlowProcess[Properties],
      output: OutputStream): TupleEntryCollector = {
    return new TupleEntrySchemeCollector[Properties, OutputStream](
        process,
        getScheme(),
        // scalastyle:off null
        if (null == output) new ByteArrayOutputStream() else output,
        // scalastyle:on null
        getIdentifier());
  }

  override def createResource(jobConf: Properties): Boolean = {
    throw new UnsupportedOperationException("KijiTap does not support creating tables for you.")
  }

  override def deleteResource(jobConf: Properties): Boolean = {
    throw new UnsupportedOperationException("KijiTap does not support deleting tables for you.")
  }

  override def resourceExists(jobConf: Properties): Boolean = {
    val uri: KijiURI = KijiURI.newBuilder(tableUri).build()

    doAndRelease(Kiji.Factory.open(uri)) { kiji: Kiji =>
      kiji.getTableNames().contains(uri.getTable())
    }
  }

  // currently unable to find last mod time on a table.
  override def getModifiedTime(jobConf: Properties): Long = System.currentTimeMillis()

  override def equals(other: Any): Boolean = {
    other match {
      case tap: LocalKijiTap => ((tableUri == tap.tableUri)
          && (scheme == tap.scheme)
          && (id == tap.id))
      case _ => false
    }
  }

  override def hashCode(): Int = Objects.hashCode(tableUri, scheme, id)
}

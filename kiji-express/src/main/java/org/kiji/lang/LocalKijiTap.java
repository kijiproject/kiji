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

package org.kiji.lang;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;
import java.util.UUID;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.TupleEntrySchemeCollector;
import cascading.tuple.TupleEntrySchemeIterator;

import org.kiji.mapreduce.framework.KijiConfKeys;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiURI;

/**
 * A {@link Tap} for reading data from a Kiji table.
 * <p>This tap is responsible for configuring a local job to read from a Kiji table.</p>
 * <p>
 *   Note: Warnings about a missing serialVersionUID are ignored here. When KijiTap is serialized,
 *   the result is not persisted anywhere making serialVersionUID unnecessary.
 * </p>
 */
@SuppressWarnings("serial")
public class LocalKijiTap
    extends Tap<Properties, InputStream, OutputStream> {
  /** The URI of the table to be read through this tap. */
  private final String mTableURI;
  /** A unique identifier for this tap instance. */
  private final String mId;

  /**
   * Creates a new instance of this tap.
   *
   * @param tableURI for the Kiji table this tap will be used to read.
   * @param scheme to be used with this tap that will convert data read from Kiji into Cascading's
   *     tuple model. Note: You must use {@link LocalKijiScheme} with this tap.
   * @throws IOException if there is an error creating the tap.
   */
  public LocalKijiTap(KijiURI tableURI, LocalKijiScheme scheme) throws IOException {
    // Set the scheme for this tap.
    super(scheme);

    mTableURI = tableURI.toString();
    mId = UUID.randomUUID().toString();
  }

  /**
   * Sets any configuration options that are required for running a local job
   * that reads from a Kiji table.
   *
   * @param process Current Cascading flow being built.
   * @param conf The job configuration object.
   */
  @Override
  public void sourceConfInit(FlowProcess<Properties> process, Properties conf) {
    // Store the input table.
    conf.setProperty(KijiConfKeys.KIJI_INPUT_TABLE_URI, mTableURI);

    super.sourceConfInit(process, conf);
  }

  /**
   * Sets any configuration options that are required for running a local job
   * that writes to a Kiji table.
   *
   * @param process Current Cascading flow being built.
   * @param conf The job configuration object.
   */
  @Override
  public void sinkConfInit(FlowProcess<Properties> process, Properties conf) {
    // Store the output table.
    conf.setProperty(KijiConfKeys.KIJI_OUTPUT_TABLE_URI, mTableURI);

    super.sinkConfInit(process, conf);
  }

  /** {@inheritDoc} */
  @Override
  public String getIdentifier() {
    return mId;
  }

  /** {@inheritDoc} */
  @Override
  public TupleEntryIterator openForRead(
      FlowProcess<Properties> process,
      InputStream input) throws IOException {
    return new TupleEntrySchemeIterator<Properties, InputStream>(
        process,
        getScheme(),
        null == input ? new ByteArrayInputStream(new byte[0]) : input,
        getIdentifier());
  }

  /** {@inheritDoc} */
  @Override
  public TupleEntryCollector openForWrite(
      FlowProcess<Properties> process,
      OutputStream output) throws IOException {
    return new TupleEntrySchemeCollector<Properties, OutputStream>(
        process,
        getScheme(),
        null == output ? new ByteArrayOutputStream() : output,
        getIdentifier());
  }

  /** {@inheritDoc} */
  @Override
  public boolean deleteResource(Properties conf) throws IOException {
    throw new UnsupportedOperationException("KijiTap does not support deleting tables for you.");
  }

  /** {@inheritDoc} */
  @Override
  public boolean createResource(Properties conf) throws IOException {
    throw new UnsupportedOperationException("KijiTap does not support creating tables for you.");
  }

  /** {@inheritDoc} */
  @Override
  public boolean resourceExists(Properties conf) throws IOException {
    final KijiURI uri = KijiURI.newBuilder(mTableURI).build();
    final String tableName = uri.getTable();
    final Kiji kiji = Kiji.Factory.open(uri);

    return kiji.getTableNames().contains(tableName);
  }

  /** {@inheritDoc} */
  @Override
  public long getModifiedTime(Properties conf) throws IOException {
    return System.currentTimeMillis(); // currently unable to find last mod time on a table
  }
}

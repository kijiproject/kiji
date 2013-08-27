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
package org.kiji.scoring.impl;

import java.io.IOException;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.HConstants;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.mapreduce.kvstore.KeyValueStoreReader;
import org.kiji.mapreduce.kvstore.KeyValueStoreReaderFactory;
import org.kiji.mapreduce.produce.ProducerContext;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiColumnName;
import org.kiji.scoring.impl.MultiBufferedWriter.SingleBuffer;

/**
 * Producer context for freshening KijiProducers.  The context is responsible for providing access
 * to KVStores and processing writes during produce.
 */
@ApiAudience.Private
@ApiStability.Experimental
public final class KijiFreshProducerContext implements ProducerContext {

  private final KeyValueStoreReaderFactory mFactory;
  private final String mFamily;
  private final String mQualifier;
  private final SingleBuffer mWriter;
  private final EntityId mEntityId;

  /**
   * Set by InternalFreshKijiTableReader after Producer.producer() returns to indicate that there
   * will be no further writes to this context and that its buffer may now be flushed.
   */
  private boolean mFinished;

  /**
   * Set to true by calls to put(), never set back to false.  Is used to indicate whether a reread
   * from the table is appropriate after a producer finishes.
   */
  private boolean mHasReceivedWrites = false;

  /**
   * Private constructor, use {@link KijiFreshProducerContext#create
   * (org.kiji.schema.KijiColumnName, org.kiji.schema.EntityId,
   * org.kiji.mapreduce.kvstore.KeyValueStoreReaderFactory, org.kiji.schema.KijiBufferedWriter)}.
   *
   * @param outputColumn the target column.
   * @param eid the target EntityId.
   * @param factory a factory of kv-serialize readers.
   * @param writer the SingleBuffer to be used by this Context to perform put() calls.
   * @throws IOException in case of an error opening a connection to the underlying table.
   */
  private KijiFreshProducerContext(
      final KijiColumnName outputColumn,
      final EntityId eid,
      final KeyValueStoreReaderFactory factory,
      final SingleBuffer writer)
      throws IOException {
    mEntityId = eid;
    mWriter = writer;
    mFamily = Preconditions.checkNotNull(outputColumn.getFamily());
    mQualifier = outputColumn.getQualifier();
    mFactory = Preconditions.checkNotNull(factory);
    mFinished = false;
  }

  /**
   * Create a new KijiFreshProducerContext configured to write to a specific column and row using a
   * given MultiBufferedWriter.
   *
   * @param outputColumn the column to which to write.
   * @param eid the EntityId of the row to which to write.
   * @param factory a factory of kv-serialize readers.
   * @param writer the SingleBuffer to be used by this Context to perform put() calls.
   * @return a new KijiFreshProducerContext configured to write to a specific column and row using a
   * given SingleBuffer.
   * @throws IOException in case of an error opening a connection to the underlying table.
   */
  public static KijiFreshProducerContext create(
      final KijiColumnName outputColumn,
      final EntityId eid,
      final KeyValueStoreReaderFactory factory,
      final SingleBuffer writer)
      throws IOException {
    return new KijiFreshProducerContext(outputColumn, eid, factory, writer);
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(final T value) throws IOException {
    put(HConstants.LATEST_TIMESTAMP, value);
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(final long timestamp, final T value) throws IOException {
    if (mWriter == null) {
      throw new UnsupportedOperationException("Writing in producer setup and cleanup methods is "
          + "unsupported.");
    } else {
      mWriter.put(mEntityId, mFamily, Preconditions.checkNotNull(
          mQualifier, "Output column is a map type family, use put(qualifier, timestamp, value)"),
          timestamp, value);
      mHasReceivedWrites = true;
    }
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(final String qualifier, final T value) throws IOException {
    put(qualifier, HConstants.LATEST_TIMESTAMP, value);
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(final String qualifier, final long timestamp, final T value)
      throws IOException {
    Preconditions.checkArgument(mQualifier == null, "Qualifier is already set in the "
        + "ProducerContext, use ProducerContext.put(timestamp, value)");
    if (mWriter == null) {
      throw new UnsupportedOperationException("Writing in producer setup and cleanup methods is "
          + "unsupported.");
    } else {
      mWriter.put(mEntityId, mFamily, qualifier, timestamp, value);
      mHasReceivedWrites = true;
    }
  }

  /** {@inheritDoc} */
  @Override
  public <K, V> KeyValueStoreReader<K, V> getStore(final String s) throws IOException {
    return mFactory.openStore(s);
  }

  /** {@inheritDoc} */
  @Override
  public void flush() throws IOException {
    if (mWriter == null) {
      throw new UnsupportedOperationException("Flushing in producer setup and cleanup methods is "
          + "unsupported.");
    } else {
      mWriter.flush();
    }
  }

  /**
   * Whether this context has received at least one write.  Is used to indicate whether a reread
   * from the table is appropriate.
   * @return Whether this context has received at least one write.
   */
  public boolean hasReceivedWrites() {
    return mHasReceivedWrites;
  }

  /**
   * Called when the produce method using this context returns to indicate the buffer may be
   * flushed.
   */
  void finish() {
    mFinished = true;
  }

  /**
   * Whether the produce method using this context has returned.
   * @return Whether the produce method using this context has returned.
   */
  boolean isFinished() {
    return mFinished;
  }

  /** {@inheritDoc} */
  @Override
  public void incrementCounter(final Enum<?> anEnum) { }

  /** {@inheritDoc} */
  @Override
  public void incrementCounter(final Enum<?> anEnum, final long l) { }

  /** {@inheritDoc} */
  @Override
  public void progress() { }

  /** {@inheritDoc} */
  @Override
  public void setStatus(final String s) throws IOException { }

  /** {@inheritDoc} */
  @Override
  public String getStatus() {
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException { }
}

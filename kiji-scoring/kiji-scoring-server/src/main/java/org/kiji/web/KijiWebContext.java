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

package org.kiji.web;

import java.io.IOException;
import java.util.Map;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.HConstants;

import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.KeyValueStoreReader;
import org.kiji.mapreduce.kvstore.KeyValueStoreReaderFactory;
import org.kiji.mapreduce.produce.ProducerContext;
import org.kiji.schema.KijiColumnName;

/**
 * Concrete implementation of a ProducerContext suitable for sending data to a remote client.
 *
 * <p>
 *   Creates {@link KijiScoringServerCell}s from values written to {@link #put(Object)} methods so
 *   that they can be sent as JSON to a remove client.
 * </p>
 *
 * <p>
 *   Instance of this class are not thread safe and should only be exposed to one KijiProducer at a
 *   time.
 * </p>
 */
public class KijiWebContext implements ProducerContext {

  private final KeyValueStoreReaderFactory mKVStoreFactory;
  private final String mFamily;
  private final String mQualifier;

  /** Cell written by the Producer served by this context. May only be set once. */
  private KijiScoringServerCell mOutputCell = null;

  /**
   * Constructs a new KijiWebContext given the bound KV stores and the output column to "write" the
   * final results to upon completion of the producer.
   *
   * @param boundStores is the map of name to KVStore.
   * @param outputColumn is the name of the column to which to write the results.
   */
  public KijiWebContext(
      final Map<String, KeyValueStore<?, ?>> boundStores,
      final KijiColumnName outputColumn
  ) {
    mKVStoreFactory = KeyValueStoreReaderFactory.create(boundStores);
    mFamily = outputColumn.getFamily();
    mQualifier = outputColumn.getQualifier();
  }

  /**
   * Construct a new KijiWebContext with the given KeyValueStoreReaderFactory and output column.
   *
   * @param factory KeyValueStoreReaderFactory for providing KVStoreReaders with
   *     {@link #getStore(String)}.
   * @param outputColumn Column to which the output value will be written.
   */
  public KijiWebContext(
      final KeyValueStoreReaderFactory factory,
      final KijiColumnName outputColumn
  ) {
    mKVStoreFactory = factory;
    mFamily = outputColumn.getFamily();
    mQualifier = outputColumn.getQualifier();
  }

  /**
   * Get the cell created by this context from the producer's output.
   *
   * @return the cell created by this context from the producer's output.
   */
  public KijiScoringServerCell getWrittenCell() {
    return mOutputCell;
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    mKVStoreFactory.close();
  }

  /** {@inheritDoc} */
  @Override
  public void flush() throws IOException {
    throw new UnsupportedOperationException("KijiWebContext does not support flushing.");
  }

  /** {@inheritDoc} */
  @Override
  public <K, V> KeyValueStoreReader<K, V> getStore(String storeName) throws IOException {
    return mKVStoreFactory.openStore(storeName);
  }

  /** {@inheritDoc} */
  @Override
  public void incrementCounter(Enum<?> counter) {
    throw new UnsupportedOperationException(
        "KijiWebContext does not support incrementing counters.");
  }

  /** {@inheritDoc} */
  @Override
  public void incrementCounter(Enum<?> counter, long amount) {
    throw new UnsupportedOperationException(
        "KijiWebContext does not support incrementing counters.");
  }

  /** {@inheritDoc} */
  @Override
  public void progress() {
    throw new UnsupportedOperationException("KijiWebContext does not support progressing.");
  }

  /** {@inheritDoc} */
  @Override
  public void setStatus(String msg) throws IOException {
    throw new UnsupportedOperationException("KijiWebContext does not support setting status.");
  }

  /** {@inheritDoc} */
  @Override
  public String getStatus() {
    throw new UnsupportedOperationException("KijiWebContext does not support getting status.");
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(T value) throws IOException {
    Preconditions.checkState(null == mOutputCell,
        "KijiProducers using KijiWebContexts may not write more than one value.");
    mOutputCell =
        new KijiScoringServerCell(mFamily, mQualifier, HConstants.LATEST_TIMESTAMP, value);

  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(long timestamp, T value) throws IOException {
    Preconditions.checkState(null == mOutputCell,
        "KijiProducers using KijiWebContexts may not write more than one value.");
    mOutputCell = new KijiScoringServerCell(mFamily, mQualifier, timestamp, value);
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(String qualifier, T value) throws IOException {
    Preconditions.checkState(null == mOutputCell,
        "KijiProducers using KijiWebContexts may not write more than one value.");
    mOutputCell = new KijiScoringServerCell(mFamily, qualifier, System.currentTimeMillis(), value);
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(String qualifier, long timestamp, T value) throws IOException {
    Preconditions.checkState(null == mOutputCell,
        "KijiProducers using KijiWebContexts may not write more than one value.");
    mOutputCell = new KijiScoringServerCell(mFamily, qualifier, timestamp, value);
  }
}


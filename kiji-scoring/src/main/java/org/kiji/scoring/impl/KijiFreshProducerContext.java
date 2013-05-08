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
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableWriter;

/**
 * Producer context for freshening KijiProducers.  The context is responsible for providing access
 * to KVStores and processing writes during produce.
 */
@ApiAudience.Private
@ApiStability.Experimental
public final class KijiFreshProducerContext implements ProducerContext {

  private EntityId mEntityId;
  private KeyValueStoreReaderFactory mFactory;
  private String mFamily;
  private String mQualifier;
  private KijiTableWriter mWriter;

  /**
   * Private constructor, use {@link KijiFreshProducerContext#create
   * (org.kiji.schema.KijiTable, org.kiji.schema.KijiColumnName, org.kiji.schema.EntityId,
   * org.kiji.mapreduce.kvstore.KeyValueStoreReaderFactory)}.
   *
   * @param table the target table.
   * @param outputColumn the target column.
   * @param eid the target EntityId.
   * @param factory a factory of kv-serialize readers.
   */
  private KijiFreshProducerContext(KijiTable table, KijiColumnName outputColumn, EntityId eid,
      KeyValueStoreReaderFactory factory) {
    mEntityId = eid;
    mWriter = table.openTableWriter();
    mFamily = Preconditions.checkNotNull(outputColumn.getFamily());
    mQualifier = outputColumn.getQualifier();
    mFactory = Preconditions.checkNotNull(factory);
  }

  /**
   * Create a new KijiFreshProducerContext configured to write to a specific column and row in a
   * given KijiTable.
   *
   * @param table the table to write into.
   * @param outputColumn the column to which to write.
   * @param eid the EntityId of the row to which to write.
   * @param factory a factory of kv-serialize readers.
   * @return the new KijiFreshProducerContext.
   */
  public static KijiFreshProducerContext create(KijiTable table, KijiColumnName outputColumn,
      EntityId eid, KeyValueStoreReaderFactory factory) {
    return new KijiFreshProducerContext(table, outputColumn, eid, factory);
  }

  /**
   * Producer contexts used for freshening will be configured to write to the column where the
   * freshness policy is attached, rather than the column returned by
   * {@link KijiProducer#getOutputColumn()}.
   * {@inheritDoc}
   */
  @Override
  public <T> void put(final T value) throws IOException {
    put(HConstants.LATEST_TIMESTAMP, value);
  }

  /**
   * Producer contexts used for freshening will be configured to write to the column where the
   * freshness policy is attached, rather than the column returned by
   * {@link KijiProducer#getOutputColumn()}.
   * {@inheritDoc}
   */
  @Override
  public <T> void put(final long timestamp, final T value) throws IOException {
    mWriter.put(mEntityId, mFamily, Preconditions.checkNotNull(
        mQualifier, "Output column is a map type family, use put(qualifier, timestamp, value)"),
        timestamp, value);
  }

  /**
   * Producer contexts used for freshening will be configured to write to the column where the
   * freshness policy is attached, rather than the column returned by
   * {@link KijiProducer#getOutputColumn()}.
   * {@inheritDoc}
   */
  @Override
  public <T> void put(final String qualifier, final T value) throws IOException {
    put(qualifier, HConstants.LATEST_TIMESTAMP, value);
  }

  /**
   * Producer contexts used for freshening will be configured to write to the column where the
   * freshness policy is attached, rather than the column returned by
   * {@link KijiProducer#getOutputColumn()}.
   * {@inheritDoc}
   */
  @Override
  public <T> void put(final String qualifier, final long timestamp, final T value)
      throws IOException {
    Preconditions.checkArgument(mQualifier == null, "Qualifier is already set in the "
        + "ProducerContext, use KijiFreshProducerContext.put(timestamp, value)");
    mWriter.put(mEntityId, mFamily, qualifier, timestamp, value);
  }

  /** {@inheritDoc} */
  @Override
  public <K, V> KeyValueStoreReader<K, V> getStore(final String s) throws IOException {
    return mFactory.openStore(s);
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

  /** {@inheritDoc} */
  @Override
  public void flush() throws IOException { }
}

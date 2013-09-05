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
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.mapreduce.kvstore.KeyValueStoreReader;
import org.kiji.mapreduce.kvstore.KeyValueStoreReaderFactory;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.scoring.PolicyContext;

/**
 * Internal implementation of PolicyContext for providing KijiFreshnessPolicy instances access to
 * outside data.
 */
@ApiAudience.Private
@ApiStability.Experimental
public final class InternalPolicyContext implements PolicyContext {
  private final Configuration mConf;
  private final KijiColumnName mAttachedColumn;
  private final Map<String, String> mParameters = Maps.newHashMap();
  private KijiDataRequest mClientRequest;
  private KeyValueStoreReaderFactory mKeyValueStoreReaderFactory;
  private boolean mReinitializeProducer;

  /**
   * Creates a new InternalPolicyContext to give freshness policies access to outside data.
   *
   * @param clientRequest The client data request which necessitates a freshness check.
   * @param attachedColumn The column to which the freshness policy is attached.
   * @param conf The Configuration from the Kiji instance housing the KijiTable from which this
   *   FreshKijiTableReader reads.
   * @param factory a KeyValueStoreReaderFactory configured with KVStores from the FreshnessPolicy
   *   and producer.
   * @param parameters a key-value mapping of parameters retrieved from the
   *   KijiFreshnessPolicyRecord, may be overridden during isFresh.
   * @param reinitializeProducer default value for whether to reinitialize the KijiProducer object,
   *   retrieved from the KijiFreshnessPolicyRecord.
   */
  public InternalPolicyContext(
      final KijiDataRequest clientRequest,
      final KijiColumnName attachedColumn,
      final Configuration conf,
      final KeyValueStoreReaderFactory factory,
      final Map<String, String> parameters,
      final boolean reinitializeProducer
  ) {
    mClientRequest = clientRequest;
    mAttachedColumn = attachedColumn;
    mConf = conf;
    mKeyValueStoreReaderFactory = factory;
  }

  /** {@inheritDoc} */
  @Override
  public KijiDataRequest getClientRequest() {
    return mClientRequest;
  }

  /** {@inheritDoc} */
  @Override
  public KijiColumnName getAttachedColumn() {
    return mAttachedColumn;
  }

  /** {@inheritDoc} */
  @Override
  public Configuration getConfiguration() {
    return mConf;
  }

  /** {@inheritDoc} */
  @Override
  public <K, V> KeyValueStoreReader<K, V> getStore(
      final String storeName
  ) throws IOException {
    return mKeyValueStoreReaderFactory.openStore(storeName);
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, String> getParameters() {
    return mParameters;
  }

  /** {@inheritDoc} */
  @Override
  public void setParameter(
      final String key,
      final String value
  ) {
    mParameters.put(key, value);
  }

  /** {@inheritDoc} */
  @Override
  public void reinitializeProducer(
      final boolean reinitializeProducer
  ) {
    mReinitializeProducer = reinitializeProducer;
  }

  /**
   * Whether the FreshKijiTableReader which contains the FreshnessPolicy which this PolicyContext
   * serves should reinitialize the KijiProducer object to reflect changes made to the Parameters
   * map.
   *
   * @return whether to reinitialize the KijiProducer object.
   */
  public boolean shouldReinitializeProducer() {
    return mReinitializeProducer;
  }
}

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
import java.util.Collections;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.kvstore.KeyValueStoreReader;
import org.kiji.mapreduce.kvstore.KeyValueStoreReaderFactory;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.scoring.FreshenerContext;

/**
 * Private implementation of FreshenerContext used by InternalFreshKijiTableReader.
 * <p>
 *   Also implements {@link org.kiji.scoring.FreshenerSetupContext} and
 *   {@link org.kiji.scoring.FreshenerGetStoresContext}. These interfaces are provided as views on
 *   the same object at different times in its lifecycle.
 * </p>
 */
@ApiAudience.Private
public final class InternalFreshenerContext implements FreshenerContext {

  private static final Map<String, String> EMPTY_PARAMS = Collections.emptyMap();

  // -----------------------------------------------------------------------------------------------
  // Static methods
  // -----------------------------------------------------------------------------------------------

  /**
   * Union the parameters and overrides and return an immutable view of the map.
   *
   * @param parameters base parameters to merge.
   * @param overrides overrides to merge. Entries in this map will mask entries in parameters.
   * @return an immutable view of the union of parameters and overrides, favoring overrides.
   */
  private static Map<String, String> unionParameters(
      final Map<String, String> parameters,
      final Map<String, String> overrides
  ) {
    final Map<String, String> collectedParameters = Maps.newHashMap(parameters);
    collectedParameters.putAll(overrides);
    return ImmutableMap.copyOf(collectedParameters);
  }

  /**
   * Create a new InternalFreshenerContext appropriate for use as a FreshenerGetStoreContext. Client
   * request, parameter overrides, and the KVStore reader factory will all be empty. Context
   * returned by this method can be used as a FreshenerSetupContext after calling
   * {@link #setKeyValueStoreReaderFactory(org.kiji.mapreduce.kvstore.KeyValueStoreReaderFactory)}.
   *
   * @param attachedColumn the column to which the Freshener served by this context is attached.
   * @param parameters configuration parameters from the Freshener record which was used to create
   *     the Freshener served by this context.
   * @return a new InternalFreshenerContext.
   */
  public static InternalFreshenerContext create(
      final KijiColumnName attachedColumn,
      final Map<String, String> parameters
  ) {
    return new InternalFreshenerContext(
        null,
        attachedColumn,
        parameters,
        EMPTY_PARAMS);
  }

  /**
   * Create a new InternalFreshenerContext appropriate for use as a FreshenerSetupContext. Client
   * request and parameter overrides will be empty.
   *
   * @param attachedColumn the column to which the Freshener served by this context is attached.
   * @param parameters configuration parameters from the Freshener record which was used to create
   *     the Freshener served by this context.
   * @param factory a KeyValueStoreReaderFactory which provides KeyValueStoreReaders for this
   *     context.
   * @return a new InternalFreshenerContext.
   */
  public static InternalFreshenerContext create(
      final KijiColumnName attachedColumn,
      final Map<String, String> parameters,
      final KeyValueStoreReaderFactory factory
  ) {
    final InternalFreshenerContext ifc =
        new InternalFreshenerContext(null, attachedColumn, parameters, EMPTY_PARAMS);
    ifc.setKeyValueStoreReaderFactory(factory);
    return ifc;
  }

  /**
   * Create a new InternalFreshenerContext appropriate for use as a FreshenerContext. All fields
   * will be set.
   *
   * @param clientRequest the client data request which triggered the Freshener served by this
   * context.
   * @param attachedColumn the column to which the Freshener served by this context is attached.
   * @param parameters the configuration parameters recovered from the KijiFreshenerRecord from
   *     which the Freshener served by this context was built.
   * @param parameterOverrides configuration parameters passed to a FreshKijiTableReader at request
   *     time. These overrides take precedence over 'parameters'.
   * @param factory a KeyValueStoreReaderFactory which provides KeyValueStoreReaders for this
   *     context.
   * @return a new InternalFreshenerContext.
   */
  public static InternalFreshenerContext create(
      final KijiDataRequest clientRequest,
      final KijiColumnName attachedColumn,
      final Map<String, String> parameters,
      final Map<String, String> parameterOverrides,
      final KeyValueStoreReaderFactory factory
  ) {
    final InternalFreshenerContext ifc =
        new InternalFreshenerContext(clientRequest, attachedColumn, parameters, parameterOverrides);
    ifc.setKeyValueStoreReaderFactory(factory);
    return ifc;
  }

  // -----------------------------------------------------------------------------------------------
  // State
  // -----------------------------------------------------------------------------------------------

  private final KijiDataRequest mClientRequest;
  private final KijiColumnName mAttachedColumn;
  private final Map<String, String> mParameters;
  private KeyValueStoreReaderFactory mReaderFactory = null;

  /**
   * Initialize a new InternalFreshenerContext.
   *
   * @param clientRequest the client data request which triggered the Freshener served by this
   *     context.
   * @param attachedColumn the column to which the Freshener served by this context is attached.
   * @param parameters the configuration parameters recovered from the KijiFreshenerRecord from
   *     which the Freshener served by this context was built.
   * @param parameterOverrides configuration parameters passed to a FreshKijiTableReader at request
   *     time. These overrides take precedence over 'parameters'.
   */
  private InternalFreshenerContext(
      final KijiDataRequest clientRequest,
      final KijiColumnName attachedColumn,
      final Map<String, String> parameters,
      final Map<String, String> parameterOverrides
  ) {
    mClientRequest = clientRequest;
    mAttachedColumn = attachedColumn;
    mParameters = unionParameters(parameters, parameterOverrides);
  }

  /**
   * Sets the KeyValueStoreReaderFactory from which to provide KVStores for this context.
   * mReaderFactory will be null before {@link org.kiji.scoring.KijiFreshnessPolicy} and
   * {@link org.kiji.scoring.ScoreFunction} getRequiredStores() methods are
   * complete.
   *
   * @param factory the KVStoreReaderFactory to use to provide KVStores for this context.
   */
  public void setKeyValueStoreReaderFactory(
      final KeyValueStoreReaderFactory factory
  ) {
    Preconditions.checkState(null == mReaderFactory,
        "Cannot set KeyValueStoreReaderFactory which has already been set.");
    mReaderFactory = factory;
  }

  // -----------------------------------------------------------------------------------------------
  // Public interface
  // -----------------------------------------------------------------------------------------------

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
  public <K, V> KeyValueStoreReader<K, V> getStore(
      final String storeName
  ) throws IOException {
    Preconditions.checkState(null != mReaderFactory,
        "Cannot open KeyValueStores during calls to getRequiredStores()");
    return mReaderFactory.openStore(storeName);
  }

  /** {@inheritDoc} */
  @Override
  public String getParameter(
      final String key
  ) {
    return mParameters.get(key);
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, String> getParameters() {
    return mParameters;
  }
}

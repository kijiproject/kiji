/**
 * (c) Copyright 2012 WibiData, Inc.
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

package org.kiji.mapreduce.kvstore;

import java.util.Map;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;

/**
 * <p>A KeyValueStoreClient defines a mapping between store names and their
 * KeyValueStore implementations via the getRequiredStores() method.
 * When used in the Kiji framework, you may override these default implementations
 * at runtime with either MapReduceJobBuilder.withStore()
 * or MapReduceJobBuilder.withStoreBindingsFile().</p>
 *
 * <p>How the KeyValueStores are surfaced to a KeyValueStoreClient varies across
 * implementations; often {@link org.kiji.mapreduce.KijiContext#getStore(String)} is used within
 * methods that receive Context objects as arguments.
 * You <b>should not</b> open KeyValueStoreReaders directly by repeatedly calling {@link
 * #getRequiredStores()}. This will create a new store and reader each time, and the
 * reader may not be properly initialized to read, e.g., from the distributed cache.</p>
 *
 * <p> Look to the implementing class for details on how the class distinguishes between
 * defining the store configurations you require and opening an initialized
 * {@link KeyValueStoreReader} implementation. For example, KijiProducer and KijiGatherer
 * classes can use the <code>getStore(String storeName)</code> method of the Context
 * object passed in as an argument to <code>produce()</code>, <code>gather()</code>, etc.
 * You should refer to the documentation for the API you are implementing against.</p>
 *
 * <p>If you are implementing your own
 * handler for data using MapReduce or other means "from scratch", you may want to use a
 * {@link KeyValueStoreReaderFactory} to deserialize a set of KeyValueStores from a
 * Configuration object.</p>
 */
@ApiAudience.Public
@ApiStability.Stable
@Inheritance.Extensible
public interface KeyValueStoreClient {
  /**
   * <p>Returns a mapping that specifies the names of all key-value stores that must be loaded
   * to execute this component, and default {@link KeyValueStore} definitions that can be
   * used if the user does not specify alternate locations/implementations.
   * It is an error for any of these default implementations to be null.
   * If you want to defer KeyValueStore definition to runtime, bind a name
   * to the {@link org.kiji.mapreduce.kvstore.lib.UnconfiguredKeyValueStore} instead.<p>
   *
   * <p>Note that this method returns <em>default</em> mappings from store names to
   * concrete implementations. Users may override these mappings, e.g. in MapReduce job
   * configuration. You <em>should not</em> open a store returned by
   * <code>getRequiredStores()</code> directly; you should look to a
   * <code>Context</code> object or similar mechanism exposed by the Kiji framework to
   * determine the actual {@link KeyValueStoreReader} instance to use.</p>
   *
   * @return a map from store names to default KeyValueStore implementations.
   */
  Map<String, KeyValueStore<?, ?>> getRequiredStores();
}

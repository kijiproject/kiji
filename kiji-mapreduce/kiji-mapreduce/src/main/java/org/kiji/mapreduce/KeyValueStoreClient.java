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

package org.kiji.mapreduce;

import java.util.Map;

import org.kiji.annotations.ApiAudience;

/**
 * <p>KeyValueStoreClient defines a mapping between store names and their
 * KeyValueStore implementations via the getRequiredStores() method.
 * When used in the Kiji framework, you may override these default implementations
 * at runtime with either MapReduceJobBuilder.withStore()
 * or MapReduceJobBuilder.withStoreBindingsFile().</p>
 *
 * <p>There is not currently a way to bind stores within Fresheners.
 * As a result, if you use a Producer in a Freshener, you will need to specify
 * implementations for each KeyValueStore in the getRequiredStores() method.</p>
 *
 * <p>How the KeyValueStores are surfaced to a KeyValueStoreClient is undefined.
 * Look to the implementing class for details on how these are surfaced.</p>
 */
@ApiAudience.Public
public interface KeyValueStoreClient {
  /**
   * <p>Returns a mapping that specifies the names of all key-value stores that must be loaded
   * to execute this component, and default {@link KeyValueStore} definitions that can be
   * used if the user does not specify alternate locations/implementations.
   * It is an error for any of these default implementations to be null.
   * If you want to defer KeyValueStore definition to runtime, bind a name
   * to the {@link org.kiji.mapreduce.kvstore.UnconfiguredKeyValueStore} instead.<p>
   *
   * @return a map from store names to default KeyValueStore implementations.
   */
  Map<String, KeyValueStore<?, ?>> getRequiredStores();
}

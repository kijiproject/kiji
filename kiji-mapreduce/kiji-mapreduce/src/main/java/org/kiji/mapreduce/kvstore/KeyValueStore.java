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

import java.io.IOException;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.Inheritance;

/**
 * <p>A KeyValueStore specifies all the resources needed to surface
 * key-value pairs from some backing store.  This may be defined on
 * files, a Kiji table, or some other resource like a different NoSQL database.
 * See the {@link org.kiji.mapreduce.kvstore.lib} package for implementations.</p>
 *
 * <p>The {@link org.kiji.mapreduce.kvstore.KeyValueStoreReader} interface defines
 * an implementation that actually accesses these key-value pairs, given
 * sufficient configuration info from the KeyValueStore itself. KeyValueStoreReaders
 * are opened by the {@link #open()} method of KeyValueStore.</p>
 *
 * <p>Within the Kiji framework, a KeyValueStore may be used within a MapReduce job,
 * or outside of one, e.g. in a streaming process.  In a MapReduce job, it will have access
 * to the job Configuration. In other environments, it may not have access to the same
 * resources.
 * As a result, you should be careful that your KeyValueStore implementation
 * can be run with the resources available in any expected operating environment.
 * (For example, you should <i>not</i> use the DistributedCache
 * in <code>storeToConf()</code> when a KeyValueStore is used within a non-MapReduce process.)</p>
 *
 * <h1>Lifecycle in the Kiji ecosystem:</h1>
 * <p>A KeyValueStore is bound to a name by the KeyValueStoreClient.getRequiredStores() method.
 * At runtime, you may override these name-to-KeyValueStore bindings
 * by specifying XML configuration files, or specifying individual name-to-store bindings
 * programmatically by using a subclass of MapReduceJobBuilder (e.g., KijiProduceJobBuilder).
 * These KeyValueStores will be instantiated with the default constructor,
 * the XML will be parsed into configuration name-value pairs, and those will be passed to
 * <code>initFromConf()</code>.
 *
 * <p>In a MapReduce job, configuration will be serialized using
 * <code>storeToConf()</code>, which copies data into the job Configuration.  On the
 * mapper side, this KeyValueStore will be deserialized by instantiating a KeyValueStore
 * instance through reflection, which uses the default constructor then initialized with a
 * single call to <code>initFromConf()</code>.  <b>Thus, if you implement this interface,
 * it is required that you provide a public no-argument constructor so your KeyValueStore
 * can be used within MapReduce.</b> </p>
 *
 * <p>To actually read key-value pairs, get a KeyValueStoreReader with the
 * <code>open()</code> method.  Close it when you're finished using it.</p>
 *
 * <p>KeyValueStore implementations may disallow calls to <code>initFromConf()</code>
 * after calling <code>open()</code>; it's expected that an "opened" KeyValueStore
 * must be treated as immutable. Implementations may throw InvalidStateException
 * if you do this.</p>
 *
 * @param <K> the key type expected to be implemented by the keys to this store.
 * @param <V> the value type expected to be accessed by the keys to this store.
 */
@ApiAudience.Public
@Inheritance.Extensible
public interface KeyValueStore<K, V> {

  /**
   * Serializes the state of the KeyValueStore into
   * the provided KeyValueStoreConfiguration.
   *
   * @param conf the KeyValueStoreConfiguration to serialize state into.
   *     This will be placed in a unique namespace of the job Configuration,
   *     so it can write any key.
   * @throws IOException if there is an error writing state to the configuration.
   */
  void storeToConf(KeyValueStoreConfiguration conf) throws IOException;

  /**
   * Deserializes the state of this KeyValueStore from the
   * KeyValueStoreConfiguration.
   *
   * @param conf the KeyValueStoreConfiguration storing state for this KeyValueStore.
   * @throws IOException if there is an error reading from the configuration.
   */
  void initFromConf(KeyValueStoreConfiguration conf) throws IOException;

  /**
   * Opens a KeyValueStoreReader instance for access by clients to this store.
   * After calling this method, some implementations may deny subsequent calls to
   * initFromConf() by throwing InvalidStateException.
   *
   * @return the KeyValueStoreReader associated with this KeyValueStore.
   * @throws IOException if there is an error opening the underlying storage resource.
   */
  KeyValueStoreReader<K, V> open() throws IOException;
}

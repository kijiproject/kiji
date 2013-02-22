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

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.Inheritance;
import org.kiji.mapreduce.kvstore.KeyValueStoreReader;

/** Interface for contexts passed to KijiMR methods. */
@ApiAudience.Public
@Inheritance.Sealed
public interface KijiContext extends Closeable, Flushable {
  /**
   * Opens a KeyValueStore associated with storeName for read-access.
   *
   * <p>You should close() the store instance returned by this method
   * when you are done with it.</p>
   *
   * <p>Calling getStore() multiple times on the same name will reuse the same
   * reader unless it is closed.</p>
   *
   * @param <K> The key type for the KeyValueStore.
   * @param <V> The value type for the KeyValueStore.
   * @param storeName the name of the KeyValueStore to open.
   * @return A KeyValueStoreReader associated with this storeName, or null
   *     if there is no such KeyValueStore available.
   * @throws IOException if there is an error opening the underlying storage resource.
   * @throws InterruptedException if there is an interruption while connecting to
   *     the underlying storage resource.
   */
  <K, V> KeyValueStoreReader<K, V> getStore(String storeName)
      throws IOException, InterruptedException;

  /**
   * Increments a counter by 1.
   *
   * @param counter The counter to increment.
   */
  void incrementCounter(Enum<?> counter);

  /**
   * Increment a counter by an amount.
   *
   * @param counter The counter to increment.
   * @param amount The amount to increment the counter by.
   */
  void incrementCounter(Enum<?> counter, long amount);

  /**
   * Report progress to the kiji framework.
   *
   * <p>If called as part of a MapReduce job, this will indicate progress in the style of
   * org.apache.hadoop.util.Progressable, keeping the task from being marked as dead. It may also
   * be called safely from Fresheners, so a producer using this call can be used either as a
   * Freshener or part of a Hadoop job.
   */
  void progress();

  /**
   * Sets a status string for the kiji task.
   *
   * <p>If called as part of a MapReduce job, the msg will be used as the task status
   * string. It may also be called safely from Fresheners, so a producer using this call can be
   * used either as a Freshener or part of a Hadoop job.</p>
   *
   * @param msg The status string.
   * @throws IOException if there is an error when setting the status string of the task.
   */
  void setStatus(String msg) throws IOException;

  /**
   * Returns a previously set status message.
   *
   * @return The previously set status message.
   */
  String getStatus();
}

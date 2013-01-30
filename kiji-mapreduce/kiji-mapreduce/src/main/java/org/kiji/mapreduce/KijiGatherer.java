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

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.Inheritance;
import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.KeyValueStoreClient;
import org.kiji.schema.KijiRowData;

/**
 * <p>
 * Base class for all Kiji Gatherers.  Gatherers are jobs that scan a
 * kiji table and aggregate data into an external file.
 * </p>
 *
 * <h1>Lifecycle:</h1>
 *
 * <p>
 * Instance are created using ReflectionUtils, so the {@link
 * org.apache.hadoop.conf.Configuration} is automagically set immediately after
 * instantiation with a call to setConf().  In order to initialize internal state
 * before any other methods are called, override the setConf() method.
 * </p>
 *
 * <p>
 * As a KeyValueStoreClient, KijiGatherers will have access to all
 * stores defined by getRequiredStores().
 * These stores are surfaced in the setup(), gather(), and cleanup() methods
 * via the Context provided to each.
 * </p>
 *
 * <p>
 * Once the internal state is set, functions may be called in any order, except for
 * restrictions on setup(), gather(), and cleanup().
 * </p>
 *
 * <p>
 * setup() will get called once at the beginning of the map phase, followed by
 * a call to gather() for each input row.  Once all of these gather()
 * calls have completed, cleanup() will be called exactly once.  It is possible
 * that this setup-gather-cleanup cycle may repeat any number of times.
 * </p>
 *
 * @param <K> The type of the output key from the gatherer.
 * @param <V> The type of the output value from the gatherer.
 */
@ApiAudience.Public
@Inheritance.Extensible
public abstract class KijiGatherer<K, V>
    implements Configurable, KeyValueStoreClient, KijiDataRequester, KijiMapper {

  /** The configuration key used to store the name of the KijiGatherer class. */
  public static final String CONF_GATHERER_CLASS = "kiji.gatherer.class";

  /** The Configuration for this instance. */
  private Configuration mConf;

  /**
   * Sets the Configuration for this instance.
   * This method will be called immediately after instantiation.
   * Override this method to initialize internal state from a configuration.
   *
   * @param conf The Configuration to use.
   */
  @Override
  public void setConf(Configuration conf) {
    mConf = conf;
  }

  /** {@inheritDoc} */
  @Override
  public Configuration getConf() {
    return mConf;
  }

  /**
   * Called once to initialize the gatherer before any calls to gather().
   * You may override this to add any one-time setup operations to your gather job.
   *
   * @param context A gatherer context used to write key/value output, access stores, etc.
   * @throws IOException if there is an error.
   */
  public void setup(MapReduceContext<K, V> context) throws IOException {
    // By default, do nothing.
  }

  /**
   * Called once per row in the kiji table.
   *
   * @param input A single for of input from the Kiji table, filled with the data requested.
   * @param context A gatherer context, used to write key/value output.
   * @throws IOException If there is an error.
   */
  public abstract void gather(KijiRowData input, MapReduceContext<K, V> context)
      throws IOException;

  /**
   * Called once to dispose of any resources used by the gatherer after all calls to gather().
   * You may override this to add any final cleanup operations to your gather job.
   *
   * @param context A gatherer context used to write key/value output, access stores, etc.
   * @throws IOException if there is an error.
   */
  public void cleanup(MapReduceContext<K, V> context) throws IOException {
    // By default, do nothing.
  }

  /**
   * Returns a mapping that specifies the names of all key-value stores that must be loaded
   * to execute this gatherer, and default {@link KeyValueStore} definitions that can be
   * used if the user does not specify alternate locations/implementations.
   * This method is called on the client once.
   *
   * <p>The default implementation of this method returns an empty map of Strings to
   * KeyValueStores.</p>
   *
   * @return a map from store names to default KeyValueStore implementations.
   */
  @Override
  public Map<String, KeyValueStore<?, ?>> getRequiredStores() {
    return Collections.emptyMap();
  }
}

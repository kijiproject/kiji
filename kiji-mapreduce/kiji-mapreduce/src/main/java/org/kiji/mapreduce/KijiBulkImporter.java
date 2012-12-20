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

/**
 * <p>Base class for all Kiji bulk importers.  Subclasses of KijiBulkImporter can be
 * passed to the --importer flag of a <code>kiji bulk-import</code> command.</p>
 *
 * <p>To implement your own bulk importer, extend KijiBulkImporter and implement the
 * {@link org.kiji.mapreduce.KijiBulkImporter#produce(Object, Object,
 * org.kiji.mapreduce.KijiBulkImporter.Context)} method to process your input.  To write
 * data to Kiji, call the appropriate <code>write()</code> method of the {@link
 * org.kiji.mapreduce.KijiBulkImporter.Context}.</p>
 *
 * <h1>Lifecycle:</h1>
 *
 * <p>Internal state is set by a call to setConf().  Thus, KijiBulkImporters will be
 * automagically initialized by hadoop's ReflectionUtils.</p>
 *
 * <p>As a KeyValueStoreClient, KijiProducers will have access to all
 * stores defined by getRequiredStores().
 * These stores are surfaced in the setup(), produce(), and cleanup() methods
 * via the getStore() method of the Context provided to each.</p>
 *
 * <p>Once the internal state is set, functions may be called in any order, except for
 * restrictions on setup(), produce(), and cleanup().</p>
 *
 * <p>setup() will get called once at the beginning of the map phase, followed by
 * a call to produce() for each input key-value pair.  Once all of these produce()
 * calls have completed, cleanup() will be called exactly once.  It is possible
 * that this setup-produce-cleanup cycle may repeat any number of times.</p>
 *
 * <p>A final guarantee is that setup(), produce(), and cleanup() will be called after
 * getOutputColumn() has been called at least once.</p>
 *
 * @param <K> The type of the MapReduce input key, which will depend on the input format used.
 * @param <V> The type of the MapReduce input value, which will depend on the input format used.
 */
public abstract class KijiBulkImporter<K, V>
    implements Configurable, KeyValueStoreClient {

  /**
   * Configuration key associated with the name of the KijiBulkImporter class.
   * @see org.kiji.mapreduce.util.KijiBulkImporters.create(Configuration).
   */
  public static final String CONF_BULK_IMPORTER_CLASS = "kiji.bulk.importer.class";

  /** The Configuration of this producer. */
  private Configuration mConf;

  /**
   * Your subclass of KijiBulkImporter must have a default constructor if it is to be used
   * in a bulk import job.  The constructors should be lightweight, since the framework is
   * free to create KijiBulkImporters at any time.
   */
  public KijiBulkImporter() {
    mConf = new Configuration();
  }

  /** {@inheritDoc} */
  @Override
  public void setConf(Configuration conf) {
    mConf = conf;
  }

  /** {@inheritDoc} */
  @Override
  public Configuration getConf() {
    return mConf;
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, KeyValueStore<?, ?>> getRequiredStores() {
    return Collections.emptyMap();
  }

  /**
   * @return the locality group to write to.
   * @deprecated the need to specify an output locality group will disappear once the
   *     KijiHFileOutputFormat has been fixed so that it can write multiple locality groups.
   */
  @Deprecated
  public abstract String getLocalityGroup();

  /**
   * Called once to initialize this producer before any calls to
   * {@link #produce(Object, Object, Context)}.
   *
   * @param context A context you can use to generate EntityIds and commit writes.
   *     See {@link Context#getEntityId(String)}.
   * @throws IOException on I/O error.
   */
  public void setup(KijiTableContext context) throws IOException {
    // By default, nothing to cleanup.
  }

  /**
   * Produces data to be imported into Kiji.
   *
   * @param key The MapReduce input key (its type depends on the InputFormat you use).
   * @param value The MapReduce input value (its type depends on the InputFormat you use).
   * @param context A context you can use to generate EntityIds and commit writes.
   *     See {@link Context#getEntityId(String)}.
   * @throws IOException on I/O error.
   */
  public abstract void produce(K key, V value, KijiTableContext context)
      throws IOException;

  /**
   * Called once to clean up this producer after all
   * {@link #produce(Object, Object, Context)} calls are made.
   *
   * @param context A context you can use to generate EntityIds and commit writes.
   *     See {@link Context#getEntityId(String)}.
   * @throws IOException on I/O error.
   */
  public void cleanup(KijiTableContext context) throws IOException {
    // By default, nothing to cleanup.
  }
}

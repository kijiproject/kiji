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

package org.kiji.mapreduce.bulkimport;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.Inheritance;
import org.kiji.mapreduce.KijiTableContext;
import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.KeyValueStoreClient;

/**
 * <p>Base class for all Kiji bulk importers.  Subclasses of KijiBulkImporter can be
 * passed to the --importer flag of a <code>kiji bulk-import</code> command.</p>
 *
 * <p>To implement your own bulk importer, extend KijiBulkImporter and implement the
 * {@link #produce(Object, Object, org.kiji.mapreduce.KijiTableContext)} method to process
 * your input.  To write data to Kiji, call the appropriate <code>put()</code> method of the
 * {@link org.kiji.mapreduce.KijiTableContext}.</p>
 *
 * <h1>Lifecycle:</h1>
 *
 * <p>Internal state is set by a call to setConf().  Thus, KijiBulkImporters will be
 * automagically initialized by hadoop's ReflectionUtils.</p>
 *
 * <p>
 * As a {@link KeyValueStoreClient}, KijiBulkImporter will have access to all
 * stores defined by {@link KeyValueStoreClient#getRequiredStores()}. Readers for
 * these stores are surfaced in the setup(), produce(), and cleanup() methods
 * via the Context provided to each by calling
 * {@link org.kiji.mapreduce.KijiContext#getStore(String)}.
 * </p>
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
 * <h1>Skeleton:</h1>
 * <p>
 *   Any concrete implementation of a KijiBulkImporter must implement the {@link #produce} method.
 *   An example of a bulk importer that parses a colon delimited mappings of strings to integers:
 * </p>
 * <pre><code>
 *   public void produce(LongWritable filePos, Text value, KijiTableContext context)
 *       throws IOException {
 *     final String[] split = value.toString().split(":");
 *     final String rowKey = split[0];
 *     final int integerValue = Integer.parseInt(split[1]);

 *     final EntityId eid = context.getEntityId(rowKey);
 *     context.put(eid, "primitives", "int", integerValue);
 *   }
 * </code></pre>
 *
 * @param <K> The type of the MapReduce input key, which will depend on the input format used.
 * @param <V> The type of the MapReduce input value, which will depend on the input format used.
 */
@ApiAudience.Public
@Inheritance.Extensible
public abstract class KijiBulkImporter<K, V>
    implements Configurable, KeyValueStoreClient {

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

  /**
   * {@inheritDoc}
   * <p>If you override this method for your bulk importer, you must call super.setConf(); or the
   * configuration will not be saved properly.</p>
   */
  @Override
  public void setConf(Configuration conf) {
    mConf = conf;
  }

  /**
   * {@inheritDoc}
   * <p>Overriding this method without returning super.getConf() may cause undesired behavior.</p>
   */
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
   * Called once to initialize this bulk importer before any calls to
   * {@link #produce(Object, Object, org.kiji.mapreduce.KijiTableContext)}.
   *
   * @param context A context you can use to generate EntityIds and commit writes.
   *     See {@link org.kiji.mapreduce.KijiTableContext#getEntityId(Object...)}.
   * @throws IOException on I/O error.
   */
  public void setup(KijiTableContext context) throws IOException {
    // Nothing may be added here, because users may have implemented setup methods without
    // super.setup();
  }

  /**
   * Produces data to be imported into Kiji.
   *
   * <p>Produce is called once for each key-value pair record from the raw input data.  To import
   * this data to Kiji, use the context.put(...) methods inherited from
   * {@link org.kiji.schema.KijiPutter} to specify a cell address and value to write.  One
   * execution of produce may include multiple calls to put, which may span multiple rows, columns
   * and locality groups if desired. The context provides a
   * {@link KijiTableContext#getEntityId(Object...)} method for generating EntityIds from input
   * data.</p>
   *
   * @param key The MapReduce input key (its type depends on the InputFormat you use).
   * @param value The MapReduce input value (its type depends on the InputFormat you use).
   * @param context A context you can use to generate EntityIds and commit writes.
   *     See {@link org.kiji.mapreduce.KijiTableContext#getEntityId(Object...)} and
   *     {@link org.kiji.schema.KijiPutter#put(org.kiji.schema.EntityId, String, String, T)}.
   * @throws IOException on I/O error.
   */
  public abstract void produce(K key, V value, KijiTableContext context)
      throws IOException;

  /**
   * Called once to clean up this bulk importer after all
   * {@link #produce(Object, Object, org.kiji.mapreduce.KijiTableContext)} calls are made.
   *
   * @param context A context you can use to generate EntityIds and commit writes.
   *     See {@link org.kiji.mapreduce.KijiTableContext#getEntityId(Object...)}.
   * @throws IOException on I/O error.
   */
  public void cleanup(KijiTableContext context) throws IOException {
    // Nothing may be added here, because users may have implemented cleanup methods without
    // super.cleanup();
  }
}

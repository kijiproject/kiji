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

import org.kiji.mapreduce.KeyValueStoreConfiguration;
import org.kiji.mapreduce.KeyValueStoreReader;
import org.kiji.mapreduce.kvstore.SeqFileKeyValueArrayStore.AbstractOptions;

/**
 * KeyValueStore implementation that reads records from SequenceFiles.
 *
 * <p>When specifying a SeqFileKeyValueStore in a kvstores XML file, you may
 * use the properties listed in {@link FileKeyValueStore}. No further properties
 * are provided for this implementation.</p>
 *
 * @param <K> The type of the key field stored in the SequenceFile(s).
 * @param <V> The type of value field stored in the SequenceFile(s).
 */
public class SeqFileKeyValueStore<K, V> extends FileKeyValueStore<K, V> {

  private SeqFileKeyValueArrayStore<K, V> mStore;

  /** Class that represents the options available to configure a SeqFileKeyValueStore. */
  public static class Options extends AbstractOptions<Options> {}

  /** Default constructor. Used only for reflection. */
  public SeqFileKeyValueStore() {
    this(new Options());
  }

  /**
   * Main constructor; create a new SeqFileKeyValueStore to read SequenceFiles.
   *
   * @param options the options that configure the file store.
   */
  @SuppressWarnings("rawtypes")
  public SeqFileKeyValueStore(Options options) {
    super(new SeqFileKeyValueArrayStore<K, V>(new SeqFileKeyValueArrayStore.Options()
    .withConfiguration(options.getConfiguration())
    .withInputPaths(options.getInputPaths())
    .withMaxValues(1)
    .withDistributedCache(options.getUseDistributedCache())));
    mStore = (SeqFileKeyValueArrayStore<K, V>) super.getArrayStore();
  }

  /** {@inheritDoc} */
  @Override
  public void storeToConf(KeyValueStoreConfiguration conf) throws IOException {
    mStore.storeToConf(conf);
  }

  /** {@inheritDoc} */
  @Override
  public void initFromConf(KeyValueStoreConfiguration conf) throws IOException {
    mStore.initFromConf(conf);
  }

  /** {@inheritDoc} */
  @Override
  public KeyValueStoreReader<K, V> open() throws IOException, InterruptedException {
    return new Reader(new SeqFileKeyValueArrayStore.Reader<K, V>(
        getConf(), getExpandedInputPaths(), 1));
  }

  /**
   * Reads an entire Sequence file into memory, indexed
   * by "key."
   *
   * <p>Lookups for a key <i>K</i> will return the "value" field for the first record
   * in the file where the key field has value <i>K</i>.</p>
   */
  private class Reader extends AvroKVSingleValueReader<K, V> {
    /**
     * Constructs a key value reader over a Sequence file.
     *
     * @param reader The array reader to use for reading from the file.
     * @throws IOException If there is an error.
     */
    public Reader(SeqFileKeyValueArrayStore.Reader<K, V> reader) throws IOException {
      super(reader);
    }
  }
}

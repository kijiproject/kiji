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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.ReflectionUtils;

import org.kiji.mapreduce.KeyValueStoreReader;

/**
 * KeyValueStore implementation that reads records from SequenceFiles.
 *
 * @param <K> The type of the key field stored in the SequenceFile(s).
 * @param <V> The type of value field stored in the SequenceFile(s).
 */
public class SeqFileKeyValueArrayStore<K, V> extends FileKeyValueArrayStore<K, V> {

  /** An object to encapsulate the numerous options of an AvroKVRecordKeyValueStore. */
  public static class AbstractOptions<T extends AbstractOptions<?>>
      extends FileKeyValueStore.Options<T> {}

  /** Class that represents the options available to configure a SeqFileKeyValueStore. */
  public static class Options extends AbstractOptions<Options> {
    private long mMaxValues = Long.MAX_VALUE;

    /**
     * Sets the maximum values for each key.
     *
     * @param maxValues The maximum values for each key.
     * @return This options instance.
     */
    public Options withMaxValues(long maxValues) {
      mMaxValues = maxValues;
      return this;
    }

    /**
     * Gets the maximum values for each key.
     *
     * @return The maximum number of values for each key.
     */
    public long getMaxValues() {
      return mMaxValues;
    }
  }

  /** Default constructor. Used only for reflection. */
  public SeqFileKeyValueArrayStore() {
    this(new Options());
  }

  /**
   * Main constructor; create a new SeqFileKeyValueStore to read SequenceFiles.
   *
   * @param options the options that configure the file store.
   */
  public SeqFileKeyValueArrayStore(Options options) {
    super(options);
    setMaxValues(options.getMaxValues());
  }

  /** {@inheritDoc} */
  @Override
  public KeyValueStoreReader<K, List<V>> open() throws IOException, InterruptedException {
    return new Reader<K, V>(getConf(), getExpandedInputPaths(), getMaxValues());
  }

  /**
   * Reads an entire SequenceFile of records into memory, and indexes it by the key field.
   *
   * <p>Lookups for a key <i>K</i> will return the first record in the file where the key field
   * has value <i>K</i>.</p>
   */
  static class Reader<K, V> extends KeyValueStoreReader<K, List<V>> {
    /** A map from key field to its corresponding value in the SequenceFile. */
    private Map<K, List<V>> mMap;
    private long mMaxValues;
    /**
     * Constructs a key value reader over a SequenceFile.
     *
     * @param conf The Hadoop configuration.
     * @param paths The path to the Avro file(s).
     * @param maxValues The maximum number of values associated with a key.
     * @throws IOException If the seqfile cannot be read.
     */
    @SuppressWarnings("unchecked")
    public Reader(Configuration conf, List<Path> paths, long maxValues) throws IOException {
      mMap = new TreeMap<K, List<V>>();
      mMaxValues = maxValues;
      for (Path path : paths) {
        // Load the entire SequenceFile into the lookup map.
        FileSystem fs = path.getFileSystem(conf);
        SequenceFile.Reader seqReader = new SequenceFile.Reader(fs, path, conf);
        try {
          Class<? extends K> keyClass = (Class<? extends K>) seqReader.getKeyClass();
          Class<? extends V> valClass = (Class<? extends V>) seqReader.getValueClass();
          K key = ReflectionUtils.newInstance(keyClass, conf);
          V val = ReflectionUtils.newInstance(valClass, conf);

          key = (K) seqReader.next(key);
          while (key != null) {
            val = (V) seqReader.getCurrentValue(val);
            List<V> values;
            if (!mMap.containsKey(key)) {
              values = new ArrayList<V>();
              mMap.put(key, values);
            } else {
              values = mMap.get(key);
            }
            if (values.size() < mMaxValues) {
              values.add(val);
            }
            // Get new instances of key and val to populate.
            key = ReflectionUtils.newInstance(keyClass, conf);
            val = ReflectionUtils.newInstance(valClass, conf);

            // Load the next key; returns null if we're out of file.
            key = (K) seqReader.next(key);
          }
        } finally {
          seqReader.close();
        }
      }
    }

    /** {@inheritDoc} */
    @Override
    public boolean isOpen() {
      return null != mMap;
    }

    /** {@inheritDoc} */
    @Override
    public List<V> get(K key) throws IOException, InterruptedException {
      if (!isOpen()) {
        throw new IOException("Reader is closed");
      }
      return mMap.get(key);
    }

    /** {@inheritDoc} */
    @Override
    public boolean containsKey(K key) throws IOException, InterruptedException {
      if (!isOpen()) {
        throw new IOException("Reader is closed");
      }
      return mMap.containsKey(key);
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
      mMap = null;
    }
  }
}

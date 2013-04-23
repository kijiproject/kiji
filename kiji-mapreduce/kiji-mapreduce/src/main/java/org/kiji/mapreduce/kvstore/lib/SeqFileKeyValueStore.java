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

package org.kiji.mapreduce.kvstore.lib;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.ReflectionUtils;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.KeyValueStoreReader;
import org.kiji.mapreduce.kvstore.framework.KeyValueStoreConfiguration;

/**
 * KeyValueStore implementation that reads records from SequenceFiles.
 *
 * <h3>XML Configuration</h3>
 *
 * <p>When specifying a SeqFileKeyValueStore in a kvstores XML file, you may
 * specify the following properties:</p>
 * <ul>
 *   <li><tt>dcache</tt> - True if files should be accessed by jobs through the DistributedCache.
 *   <li><tt>paths</tt> - A comma-separated list of HDFS paths to files backing this store.
 * </ul>
 *
 * <h3>Default values</h3>
 *
 * <ul>
 *   <li>By default, use of the DistributedCache is enabled.</li>
 *   <li>You must specify the paths to read. It is an error to leave this unconfigured.</li>
 *   <li>Files will be read using a new <tt>Configuration</tt> object if you do not specify
 *       your own.</li>
 * </ul>
 *
 * @param <K> The type of the key field stored in the SequenceFile(s).
 * @param <V> The type of value field stored in the SequenceFile(s).
 */
@ApiAudience.Public
public final class SeqFileKeyValueStore<K, V> implements Configurable, KeyValueStore<K, V> {

  /** Helper object to manage backing files. */
  private final FileStoreHelper mFileHelper;

  /** true if the user has called open(); cannot call initFromConf() after that. */
  private boolean mOpened;

  /**
   * A Builder-pattern class that configures and creates new SeqFileKeyValueStore
   * instances. You should use this to specify the input to this KeyValueStore.
   * Call the build() method to return a new, configured SeqFileKeyValueStore instance.
   */
  @ApiAudience.Public
  public static final class Builder {
    private FileStoreHelper.Builder mFileBuilder;

    /**
     * Private, default constructor. Call the builder() method of this KeyValueStore
     * to get a new builder instance.
     */
    private Builder() {
      mFileBuilder = FileStoreHelper.builder();
    }

    /**
     * Sets the Hadoop configuration instance to use.
     *
     * @param conf The configuration.
     * @return This builder instance.
     */
    public Builder withConfiguration(Configuration conf) {
      mFileBuilder.withConfiguration(conf);
      return this;
    }

    /**
     * Adds a path to the list of files to load.
     *
     * @param path The input file/directory path.
     * @return This builder instance.
     */
    public Builder withInputPath(Path path) {
      mFileBuilder.withInputPath(path);
      return this;
    }

    /**
     * Replaces the current list of files to load with the set of files
     * specified as an argument.
     *
     * @param paths The input file/directory paths.
     * @return This builder instance.
     */
    public Builder withInputPaths(List<Path> paths) {
      mFileBuilder.withInputPaths(paths);
      return this;
    }

    /**
     * Sets a flag indicating the use of the DistributedCache to distribute
     * input files.
     *
     * @param enabled true if the DistributedCache should be used, false otherwise.
     * @return This builder instance.
     */
    public Builder withDistributedCache(boolean enabled) {
      mFileBuilder.withDistributedCache(enabled);
      return this;
    }

    /**
     * Build a new SeqFileKeyValueStore instance.
     *
     *
     * @param <K> The type of the key field stored in the SequenceFile(s).
     * @param <V> The type of value field stored in the SequenceFile(s).
     * @return the initialized KeyValueStore.
     */
    public <K, V> SeqFileKeyValueStore<K, V> build() {
      return new SeqFileKeyValueStore<K, V>(this);
    }
  }

  /**
   * Creates a new SeqFileKeyValueStore.Builder instance that can be used
   * to configure and create a new KeyValueStore.
   *
   * @return a new Builder instance.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Reflection-only constructor. Used only for reflection. You should create and configure
   * new SeqFileKeyValueStore instances by using a builder;
   * call SeqFileKeyValueStore.builder() to get a new builder instance.
   */
  public SeqFileKeyValueStore() {
    this(builder());
  }

  /**
   * Main constructor used by the builder; create a new SeqFileKeyValueStore to read SequenceFiles.
   *
   * @param builder the builder to configure from.
   */
  private SeqFileKeyValueStore(Builder builder) {
    mFileHelper = builder.mFileBuilder.build();
  }

  /** {@inheritDoc} */
  @Override
  public void setConf(Configuration conf) {
    if (mOpened) {
      // Don't allow mutation after we start using this store for reads.
      throw new IllegalStateException(
          "Cannot set the configuration after a reader has been opened");
    }

    mFileHelper.setConf(conf);
  }

  /** {@inheritDoc} */
  @Override
  public Configuration getConf() {
    return new Configuration(mFileHelper.getConf());
  }

  /** {@inheritDoc} */
  @Override
  public void storeToConf(KeyValueStoreConfiguration conf) throws IOException {
    mFileHelper.storeToConf(conf);
  }

  /** {@inheritDoc} */
  @Override
  public void initFromConf(KeyValueStoreConfiguration conf) throws IOException {
    if (mOpened) {
      throw new IllegalStateException("Cannot reinitialize; already opened a reader.");
    }

    mFileHelper.initFromConf(conf);
  }

  /** @return the raw input paths specified as input by the user. */
  public List<Path> getInputPaths() {
    // Visible chiefly for testing.
    return mFileHelper.getInputPaths();
  }

  /** {@inheritDoc} */
  @Override
  public KeyValueStoreReader<K, V> open() throws IOException {
    mOpened = true;
    return new Reader(mFileHelper.getConf(), mFileHelper.getExpandedInputPaths());
  }

  /**
   * Reads an entire SequenceFile of records into memory, and indexes it by the key field.
   *
   * <p>Lookups for a key <i>K</i> will return the first record in the file where the key field
   * has value <i>K</i>.</p>
   */
  @ApiAudience.Private
  private final class Reader implements KeyValueStoreReader<K, V> {
    /** A map from key field to its corresponding value in the SequenceFile. */
    private Map<K, V> mMap;

    /**
     * Constructs a key value reader over a SequenceFile.
     *
     * @param conf The Hadoop configuration.
     * @param paths The path to the sequencefile(s).
     * @throws IOException If the seqfile cannot be read.
     */
    @SuppressWarnings("unchecked")
    public Reader(Configuration conf, List<Path> paths) throws IOException {
      mMap = new HashMap<K, V>();

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
            if (!mMap.containsKey(key)) {
              mMap.put(key, val);
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
    public V get(K key) throws IOException {
      if (!isOpen()) {
        throw new IOException("Reader is closed");
      }
      return mMap.get(key);
    }

    /** {@inheritDoc} */
    @Override
    public boolean containsKey(K key) throws IOException {
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

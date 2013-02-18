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
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroKeyValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.KeyValueStoreReader;
import org.kiji.mapreduce.kvstore.framework.KeyValueStoreConfiguration;

/**
 * An interface for providing read access to Avro container files of (key, value)
 * records.
 *
 * <p>This KeyValueStore provides lookup access to an Avro container file by reading
 * the entire file into memory. The Avro file is assumed to contain records with (at
 * least) two fields, named "key" and "value." This store will decompose the top-level
 * record into its two fields, and index the "value" field by the key.</p>
 *
 * <p>A kvstores XML file may contain the following properties when specifying the
 * behavior of this class:</p>
 * <ul>
 *   <li><tt>dcache</tt> - True if files should be accessed by jobs through the DistributedCache.
 *   <li><tt>paths</tt> - A comma-separated list of HDFS paths to files backing this store.
 *   <li><tt>avro.reader.schema</tt> - The reader schema to apply to records in the
 *       input file(s).</li>
 * </ul>
 *
 * @param <K> The type of the key field.
 * @param <V> The type of the value field.
 */
@ApiAudience.Public
public final class AvroKVRecordKeyValueStore<K, V> implements KeyValueStore<K, V> {
  /** A wrapped store for looking up an Avro record by its 'key' field. */
  private final AvroRecordKeyValueStore<K, GenericRecord> mStore;

  /** true if the user has called open(); cannot call initFromConf() after that. */
  private boolean mOpened;

  /**
   * A Builder-pattern class that configures and creates new AvroKVRecordKeyValueStore
   * instances. You should use this to specify the input to this KeyValueStore.
   * Call the build() method to return a new, configured AvroKVRecordKeyValueStore instance.
   */
  @ApiAudience.Public
  public static final class Builder {
    private AvroRecordKeyValueStore.Builder mAvroRecordStoreBuilder;

    /**
     * Private, default constructor. Call the builder() method of this KeyValueStore
     * to get a new builder instance.
     */
    private Builder() {
      mAvroRecordStoreBuilder = AvroRecordKeyValueStore.builder();
      mAvroRecordStoreBuilder.withKeyFieldName(AvroKeyValue.KEY_FIELD);
    }

    /**
     * Sets the schema to read the records with.
     * This may be null; the schema used when writing the input files will be used directly.
     *
     * @param schema The reader schema.
     * @return This builder instance.
     */
    public Builder withReaderSchema(Schema schema) {
      mAvroRecordStoreBuilder.withReaderSchema(schema);
      return this;
    }

    /**
     * Sets the Hadoop configuration instance to use.
     *
     * @param conf The configuration.
     * @return This builder instance.
     */
    public Builder withConfiguration(Configuration conf) {
      mAvroRecordStoreBuilder.withConfiguration(conf);
      return this;
    }

    /**
     * Adds a path to the list of files to load.
     *
     * @param path The input file/directory path.
     * @return This builder instance.
     */
    public Builder withInputPath(Path path) {
      mAvroRecordStoreBuilder.withInputPath(path);
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
      mAvroRecordStoreBuilder.withInputPaths(paths);
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
      mAvroRecordStoreBuilder.withDistributedCache(enabled);
      return this;
    }

    /**
     * Build a new AvroKVRecordKeyValueStore instance.
     *
     * @param <K> the key type used to look up each record.
     * @param <V> the value type returned by each record.
     * @return the initialized KeyValueStore.
     */
    public <K, V> AvroKVRecordKeyValueStore<K, V> build() {
      return new AvroKVRecordKeyValueStore<K, V>(this);
    }
  }

  /**
   * Creates a new AvroKVRecordKeyValueStore.Builder instance that can be used
   * to configure and create a new KeyValueStore.
   *
   * @return a new Builder instance.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Constructs an AvroKVRecordKeyValueStore from a builder.
   *
   * @param builder the builder instance to configure from.
   */
  private AvroKVRecordKeyValueStore(Builder builder) {
    mStore = builder.mAvroRecordStoreBuilder.build();
  }

  /**
   * Reflection-only constructor. Used only for reflection. You should create and configure
   * AvroKVRecordKeyValueStore instances by using a builder;
   * call AvroKVRecordKeyValueStore.builder() to get a new builder instance.
   */
  public AvroKVRecordKeyValueStore() {
    this(builder());
  }

  /** {@inheritDoc} */
  @Override
  public void storeToConf(KeyValueStoreConfiguration conf) throws IOException {
    mStore.storeToConf(conf);
  }

  /** {@inheritDoc} */
  @Override
  public void initFromConf(KeyValueStoreConfiguration conf) throws IOException {
    if (mOpened) {
      throw new IllegalStateException("Cannot reinitialize; already opened a reader.");
    }

    mStore.initFromConf(conf);
  }

  /** {@inheritDoc} */
  @Override
  public KeyValueStoreReader<K, V> open() throws IOException {
    mOpened = true;
    return new Reader<K, V>(mStore);
  }

  /**
   * Reads an entire Avro container file of (key, value) records into memory, indexed
   * by "key."
   *
   * <p>Lookups for a key <i>K</i> will return the "value" field of the first record
   * in the file where the key field has value <i>K</i>.</p>
   */
  @ApiAudience.Private
  static final class Reader<K, V> implements KeyValueStoreReader<K, V> {
    /** A wrapped Avro store reader for looking up a record by its 'key' field. */
    private final KeyValueStoreReader<K, GenericRecord> mReader;

    /**
     * Constructs a key value reader over an Avro file.
     *
     * @param store An Avro file store that uses the 'key' field as the key, and
     *     the entire record as the value.
     * @throws IOException If there is an error.
     */
    public Reader(AvroRecordKeyValueStore<K, GenericRecord> store) throws IOException {
      mReader = store.open();
    }

    /** {@inheritDoc} */
    @Override
    public boolean isOpen() {
      return mReader.isOpen();
    }

    /** {@inheritDoc} */
    @Override
    @SuppressWarnings("unchecked")
    public V get(K key) throws IOException {
      GenericRecord record = mReader.get(key);
      if (null == record) {
        // No match;
        return null;
      }

      return (V) record.get(AvroKeyValue.VALUE_FIELD);
    }

    /** {@inheritDoc} */
    @Override
    public boolean containsKey(K key) throws IOException {
      return mReader.containsKey(key);
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
      mReader.close();
    }
  }
}

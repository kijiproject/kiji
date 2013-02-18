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
import java.util.Map;
import java.util.TreeMap;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.hadoop.util.AvroCharSequenceComparator;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.KeyValueStoreReader;
import org.kiji.mapreduce.kvstore.framework.KeyValueStoreConfiguration;

/**
 * An interface for providing read access to Avro container files of records.
 *
 * <p>This KeyValueStore provides lookup access to an Avro container file by reading the
 * entire file into memory and indexing the records by the <i>key field</i>.  The <i>key
 * field</i> must be specified when constructing the store. The value returned from a
 * lookup of key <i>K</i> will be the first record whose key field has the value
 * <i>K</i>.</p>
 *
 * <p>A kvstores XML file may contain the following properties when specifying the behavior of this
 * class:</p>
 * <ul>
 *   <li><tt>avro.reader.schema</tt> - The reader schema to apply to records in the
 *       input file(s).</li>
 *   <li><tt>dcache</tt> - True if files should be accessed by jobs through the DistributedCache.
 *   <li><tt>key.field</tt> - The name of the field of the input records to treat as
 *       the key in the KeyValueStore.</li>
 *   <li><tt>paths</tt> - A comma-separated list of HDFS paths to files backing this store.
 * </ul>
 *
 * @param <K> The type of the key field.
 * @param <V> The type of record in the Avro container file.
 */
@ApiAudience.Public
public final class AvroRecordKeyValueStore<K, V extends IndexedRecord>
    implements KeyValueStore<K, V> {

  /** The configuration variable for the Avro record reader schema. */
  private static final String CONF_READER_SCHEMA_KEY = "avro.reader.schema";

  /** The configuration variable for the name of the field to use as the lookup key. */
  private static final String CONF_KEY_FIELD_KEY = "key.field";

  /** The schema to use for reading the Avro records. */
  private Schema mReaderSchema;

  /** The name of the field to use as the lookup key for records. */
  private String mKeyFieldName;

  /** Helper object to manage backing files. */
  private final FileStoreHelper mFileHelper;

  /** true if the user has called open(); cannot call initFromConf() after that. */
  private boolean mOpened;

  /**
   * A Builder-pattern class that configures and creates new AvroRecordKeyValueStore
   * instances. You should use this to specify the input to this KeyValueStore.
   * Call the build() method to return a new, configured AvroRecordKeyValueStore instance.
   */
  @ApiAudience.Public
  public static final class Builder {
    private FileStoreHelper.Builder mFileBuilder;
    private Schema mReaderSchema;
    private String mKeyFieldName;

    /**
     * Private, default constructor. Call the builder() method of this KeyValueStore
     * to get a new builder instance.
     */
    private Builder() {
      mFileBuilder = FileStoreHelper.builder();
    }

    /**
     * Sets the schema to read the records with.
     * This may be null; the schema used when writing the input files will be used directly.
     *
     * @param schema The reader schema.
     * @return This builder instance.
     */
    public Builder withReaderSchema(Schema schema) {
      mReaderSchema = schema;
      return this;
    }

    /**
     * Sets the name of the record field to use as the lookup key.
     *
     * @param keyFieldName The name of the key field.
     * @return This builder instance.
     */
    public Builder withKeyFieldName(String keyFieldName) {
      if (null == keyFieldName || keyFieldName.isEmpty()) {
        throw new IllegalArgumentException("Must specify a non-empty key field name");
      }
      mKeyFieldName = keyFieldName;
      return this;
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
     * Build a new AvroRecordKeyValueStore instance.
     *
     * @param <K> The type of the key field.
     * @param <V> The type of record in the Avro container file.
     * @return the initialized KeyValueStore.
     */
    public <K, V extends IndexedRecord> AvroRecordKeyValueStore<K, V> build() {
      if (null == mKeyFieldName || mKeyFieldName.isEmpty()) {
        throw new IllegalArgumentException("Must specify a non-empty key field name");
      }
      return new AvroRecordKeyValueStore<K, V>(this);
    }
  }

  /**
   * Creates a new AvroRecordKeyValueStore.Builder instance that can be used
   * to configure and create a new KeyValueStore.
   *
   * @return a new Builder instance.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Constructs an AvroRecordKeyValueStore from a builder.
   *
   * @param builder the builder to configure from.
   */
  private AvroRecordKeyValueStore(Builder builder) {
    mFileHelper = builder.mFileBuilder.build();
    mReaderSchema = builder.mReaderSchema;
    mKeyFieldName = builder.mKeyFieldName;
  }

  /**
   * Reflection-only constructor. Used only for reflection. You should create and configure
   * AvroRecordKeyValueStore instances by using a builder; call AvroRecordKeyValueStore.builder()
   * to get a new builder instance.
   */
  public AvroRecordKeyValueStore() {
    this(builder());
  }

  /** {@inheritDoc} */
  @Override
  public void storeToConf(KeyValueStoreConfiguration conf) throws IOException {
    mFileHelper.storeToConf(conf);
    if (null != mReaderSchema) {
      conf.set(CONF_READER_SCHEMA_KEY, mReaderSchema.toString());
    }

    if (null == mKeyFieldName) {
      throw new IOException("Required attribute not set: keyField");
    }
    conf.set(CONF_KEY_FIELD_KEY, mKeyFieldName);
  }

  /** {@inheritDoc} */
  @Override
  public void initFromConf(KeyValueStoreConfiguration conf) throws IOException {
    if (mOpened) {
      throw new IllegalStateException("Cannot reinitialize; already opened a reader.");
    }

    mFileHelper.initFromConf(conf);
    String schemaStr = conf.get(CONF_READER_SCHEMA_KEY);
    if (null != schemaStr) {
      mReaderSchema = Schema.parse(schemaStr);
    } else {
      mReaderSchema = null;
    }

    mKeyFieldName = conf.get(CONF_KEY_FIELD_KEY);
  }

  /** {@inheritDoc} */
  @Override
  public KeyValueStoreReader<K, V> open() throws IOException {
    mOpened = true;
    return new Reader<K, V>(mFileHelper.getConf(),
        mFileHelper.getExpandedInputPaths(), mReaderSchema, mKeyFieldName);
  }

  /**
   * Reads an entire Avro container file of records into memory, and indexes it by the key field.
   *
   * <p>Lookups for a key <i>K</i> will return the first record in the file where the key field
   * has value <i>K</i>.</p>
   */
  @ApiAudience.Private
  static final class Reader<K, V extends IndexedRecord> implements KeyValueStoreReader<K, V> {
    /** A map from key field to its corresponding record in the Avro container file. */
    private Map<K, V> mMap;

    /**
     * Constructs a key value reader over an Avro file.
     *
     * @param conf The Hadoop configuration.
     * @param paths The path to the Avro file(s).
     * @param readerSchema The reader schema for the records within the Avro file.
     * @param keyFieldName The name of the field within the record to use as the lookup key.
     * @throws IOException If the Avro file cannot be read, or the key field is not found.
     */
    public Reader(Configuration conf, List<Path> paths, Schema readerSchema, String keyFieldName)
        throws IOException {
      for (Path path : paths) {
        // Load the entire Avro file into the lookup map.
        DataFileStream<V> avroFileReader = null;
        try {
          avroFileReader = new DataFileStream<V>(path.getFileSystem(conf).open(path),
              new SpecificDatumReader<V>(readerSchema));
          if (null == readerSchema) {
            // If the user has not specified a reader schema, grab the schema from the
            // first file we encounter.
            readerSchema = avroFileReader.getSchema();
          }

          // Check that the key field exists in the reader schema.
          Schema.Field avroKeyField = readerSchema.getField(keyFieldName);
          if (null == avroKeyField) {
            throw new IOException("Key field " + keyFieldName
                + " was not found in the record schema: " + readerSchema.toString());
          }

          if (null == mMap) {
            // Set up the in-memory lookup map for Avro records.
            if (avroKeyField.schema().equals(Schema.create(Schema.Type.STRING))) {
              // Special case Avro string comparison, since we want to allow comparison of
              // String objects with Utf8 objects.
              mMap = new TreeMap<K, V>(new AvroCharSequenceComparator<K>());
            } else {
              mMap = new TreeMap<K, V>();
            }
          }

          for (V record : avroFileReader) {
            // Here, we read the value of the key field by looking it up by its record
            // position (index).  In other words, to get the value of the 'key' field, we ask
            // the Schema what position the 'key' field has, and use that position to get the
            // value from the IndexedRecord.
            //
            // We have to do this because there is no common interface between GenericRecord
            // and SpecificRecord that allows us to read fields by name.  The unit test seems
            // to suggest that this approach works just fine, but if we find a bug later,
            // the safe fix is to sacrifice the ability to read SpecificRecords -- all
            // values of this store could just be GenericData.Record instances read by the
            // GenericDatumReader.
            @SuppressWarnings("unchecked")
            K key = (K) record.get(avroKeyField.pos());
            if (!mMap.containsKey(key)) {
              mMap.put(key, record);
            }
          }
        } finally {
          if (null != avroFileReader) {
            avroFileReader.close();
          }
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

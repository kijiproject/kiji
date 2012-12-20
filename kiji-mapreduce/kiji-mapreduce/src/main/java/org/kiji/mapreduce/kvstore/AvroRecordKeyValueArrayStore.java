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

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.hadoop.util.AvroCharSequenceComparator;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.kiji.mapreduce.KeyValueStoreConfiguration;
import org.kiji.mapreduce.KeyValueStoreReader;

/**
 * An interface for providing read access to Avro container files of records.
 *
 * <p>This KeyValueStore provides lookup access to an Avro container file by reading the
 * entire file into memory and indexing the records by the <i>key field</i>.  The <i>key
 * field</i> must be specified when constructing the store. The value returned from a
 * lookup of key <i>K</i> will be the first record whose key field has the value
 * <i>K</i>.</p>
 *
 * @param <K> The type of the key field.
 * @param <V> The type of record in the Avro container file.
 */
public class AvroRecordKeyValueArrayStore<K, V extends IndexedRecord>
    extends FileKeyValueArrayStore<K, V> {

  // TODO(WIBI-1546): make variables public and update documentation.
  /** The configuration variable for the Avro record reader schema. */
  private static final String CONF_READER_SCHEMA = "avro.reader.schema";

  /** The configuration variable for the name of the field to use as the lookup key. */
  private static final String CONF_KEY_FIELD = "key.field";

  /** The schema to use for reading the Avro records. */
  private Schema mReaderSchema;

  /** The name of the field to use as the lookup key for records. */
  private String mKeyFieldName;

  /** An object to encapsulate the numerous options of an AvroKVRecordKeyValueStore. */
  public static class AbstractOptions<T extends AbstractOptions<?>>
      extends FileKeyValueStore.Options<T> {
    private Schema mReaderSchema;
    private String mKeyFieldName;

    /**
     * Sets the name of the record field to use as the lookup key.
     *
     * @param keyFieldName The name of the key field.
     * @return This options instance.
     */
    @SuppressWarnings("unchecked")
    public T withKeyFieldName(String keyFieldName) {
      mKeyFieldName = keyFieldName;
      return (T) this;
    }

    /**
     * Gets the key field name of the record.
     *
     * @return The key field name of the record.
     */
    public String getKeyFieldName() {
      return mKeyFieldName;
    }
    /**
     * Sets the schema to read the records with.
     *
     * @param schema The reader schema.
     * @return This options instance.
     */
    @SuppressWarnings("unchecked")
    public T withReaderSchema(Schema schema) {
      mReaderSchema = schema;
      return (T) this;
    }

    /**
     * Gets the schema used to read the records.
     *
     * @return The Avro reader schema.
     */
    public Schema getReaderSchema() {
      return mReaderSchema;
    }
  }

  /** An object to encapsulate the numerous options of an AvroRecordKeyValueStore. */
  public static class Options extends AvroRecordKeyValueArrayStore.AbstractOptions<Options> {
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

  /**
   * Constructs an AvroRecordKeyValueStore.
   *
   * @param options The options for configuring the store.
   */
  public AvroRecordKeyValueArrayStore(Options options) {
    super(options, options.getMaxValues());
    setConf(options.getConfiguration());
    setReaderSchema(options.getReaderSchema());
    setKeyFieldName(options.getKeyFieldName());
    this.setMaxValues(options.getMaxValues());
  }

  /**
   * Constructs an unconfigured AvroRecordKeyValueStore.
   *
   * <p>Do not use this constructor. It is for instantiation via ReflectionUtils.newInstance().</p>
   */
  public AvroRecordKeyValueArrayStore() {
    this(new Options());
  }

  /**
   * Sets the reader schema to use for the avro container file records.
   *
   * @param readerSchema The reader schema.
   */
  public void setReaderSchema(Schema readerSchema) {
    mReaderSchema = readerSchema;
  }

  /**
   * Gets the reader schema to use for the avro container file records.
   *
   * @return The reader schema.
   */
  public Schema getReaderSchema() {
    return mReaderSchema;
  }

  /**
   * Sets the field to use as the lookup key for the records.
   *
   * @param keyFieldName The key field.
   */
  public void setKeyFieldName(String keyFieldName) {
    mKeyFieldName = keyFieldName;
  }

  /**
   * Gets the key field name of the record.
   *
   * @return The key field name of the record.
   */
  public String getKeyFieldName() {
    return mKeyFieldName;
  }

  /** {@inheritDoc} */
  @Override
  public void storeToConf(KeyValueStoreConfiguration conf) throws IOException {
    storeToConf(conf, true);
  }

  /**
   * Implements storeToConf() but optionally suppresses storing the actual
   * FileSystem state associated with this KeyValueStore. This is used by the
   * AvroKVRecordKeyValueStore when wrapping an instance of this KeyValueStore;
   * the wrapper manages the file data, whereas this KeyValueStore persists only
   * reader schema, key field, etc.
   *
   * @param conf the KeyValueStoreConfiguration to persist to.
   * @param persistFsState true if the filenames, etc. associated with this store should
   *     be written to the KeyValueStoreConfiguration, false if this is externally managed.
   * @throws IOException if there is an error communicating with the FileSystem.
   */
  void storeToConf(KeyValueStoreConfiguration conf, boolean persistFsState)
      throws IOException {

    if (persistFsState) {
      super.storeToConf(conf);
    }

    if (null != mReaderSchema) {
      conf.set(CONF_READER_SCHEMA, mReaderSchema.toString());
    }

    if (null == mKeyFieldName) {
      throw new IOException("Required attribute not set: keyField");
    }
    conf.set(CONF_KEY_FIELD, mKeyFieldName);
  }

  /** {@inheritDoc} */
  @Override
  public void initFromConf(KeyValueStoreConfiguration conf) throws IOException {
    super.initFromConf(conf);

    String schema = conf.get(CONF_READER_SCHEMA);
    if (null != schema) {
      setReaderSchema(new Schema.Parser().parse(schema));
    }

    setKeyFieldName(conf.get(CONF_KEY_FIELD));
  }

  /**
   * Opens an instance of this KeyValueStore for access by clients.  This reader
   * returns an array of records containing all records that match a given key.
   *
   * @return the KeyValueStoreReader associated with this KeyValueStore.
   * @throws IOException if there is an error opening the underlying storage resource.
   * @throws InterruptedException if there is an interruption while communicating with
   *     the underlying storage resource.
   */
  public KeyValueStoreReader<K, List<V>> open()
      throws IOException, InterruptedException {
    return new Reader<K, V>(getConf(), getExpandedInputPaths(),
        mReaderSchema, mKeyFieldName, getMaxValues());
  }

    /**
   * Reads an entire Avro container file of records into memory, and indexes it by the key field.
   *
   * <p>Lookups for a key <i>K</i> will return a list of records in the file where the key field
   * has value <i>K</i>.</p>
   */
  static class Reader<K, V extends IndexedRecord> extends KeyValueStoreReader<K, List<V>> {
    /** A map from key field to its corresponding record in the Avro container file. */
    private Map<K, List<V>> mMap;
    private long mMaxValues;
    /**
     * Constructs a key value reader over an Avro file.
     *
     * @param conf The Hadoop configuration.
     * @param paths The path to the Avro file(s).
     * @param readerSchema The reader schema for the records within the Avro file.
     * @param keyFieldName The name of the field within the record to use as the lookup key.
     * @param maxValues The maximum number of values associated with a key.
     * @throws IOException If the Avro file cannot be read, or the key field is not found.
     */
    public Reader(Configuration conf, List<Path> paths,
        Schema readerSchema, String keyFieldName, long maxValues) throws IOException {
      mMaxValues = maxValues;
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
              mMap = new TreeMap<K, List<V>>(new AvroCharSequenceComparator<K>());
            } else {
              mMap = new TreeMap<K, List<V>>();
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
            List<V> values;
            if (!mMap.containsKey(key)) {
              values = new ArrayList<V>();
              mMap.put(key, values);
            } else {
              values = mMap.get(key);
            }
            if (values.size() < mMaxValues) {
              values.add(record);
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

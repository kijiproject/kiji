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

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

import org.kiji.mapreduce.KeyValueStoreConfiguration;
import org.kiji.mapreduce.KeyValueStoreReader;
import org.kiji.mapreduce.kvstore.AvroRecordKeyValueArrayStore.AbstractOptions;

/**
 * An interface for providing read access to Avro container files of records.
 *
 * <p>This KeyValueStore provides lookup access to an Avro container file by reading the
 * entire file into memory and indexing the records by the <i>key field</i>.  The <i>key
 * field</i> must be specified when constructing the store. The value returned from a
 * lookup of key <i>K</i> will be the first record whose key field has the value
 * <i>K</i>.</p>
 *
 * <p>In addition to the properties listed in {@link FileKeyValueStore}, a kvstores
 * XML file may contain the following properties when specifying the behavior of this
 * class:</p>
 * <ul>
 *   <li><tt>avro.reader.schema</tt> - The reader schema to apply to records in the
 *       input file(s).</li>
 *   <li><tt>key.field</tt> - The name of the field of the input records to treat as
 *       the key in the KeyValueStore.</li>
 * </ul>
 *
 * @param <K> The type of the key field.
 * @param <V> The type of record in the Avro container file.
 */
public class AvroRecordKeyValueStore<K, V extends IndexedRecord>
    extends FileKeyValueStore<K, V> {

  private AvroRecordKeyValueArrayStore<K, V> mStore;

  /**
   * Abstract class that represents the options available to configure an
   * AvroRecordKeyValueStore.
   */
  public static class Options extends AbstractOptions<Options> {}

  /**
   * Constructs an AvroRecordKeyValueStore.
   *
   * @param options The options for configuring the store.
   */
  public AvroRecordKeyValueStore(Options options) {
    super(new AvroRecordKeyValueArrayStore<K, V>(new AvroRecordKeyValueArrayStore.Options()
    .withConfiguration(options.getConfiguration())
    .withInputPaths(options.getInputPaths())
    .withDistributedCache(options.getUseDistributedCache())
    .withReaderSchema(options.getReaderSchema())
    .withMaxValues(1)
    .withKeyFieldName(options.getKeyFieldName())));
    mStore = (AvroRecordKeyValueArrayStore<K, V>) super.getArrayStore();
  }

  /**
   * Constructs an unconfigured AvroRecordKeyValueStore.
   *
   * <p>Do not use this constructor. It is for instantiation via ReflectionUtils.newInstance().</p>
   */
  public AvroRecordKeyValueStore() {
    this(new Options());
  }

  /**
   * Sets the reader schema to use for the avro container file records.
   *
   * @param readerSchema The reader schema.
   */
  public void setReaderSchema(Schema readerSchema) {
    mStore.setReaderSchema(readerSchema);
  }

  /**
   * Sets the field to use as the lookup key for the records.
   *
   * @param keyFieldName The key field.
   */
  public void setKeyFieldName(String keyFieldName) {
    mStore.setKeyFieldName(keyFieldName);
  }

  /** {@inheritDoc} */
  @Override
  public void storeToConf(KeyValueStoreConfiguration conf) throws IOException {
    mStore.storeToConf(conf, true);
  }

  /** {@inheritDoc} */
  @Override
  public void initFromConf(KeyValueStoreConfiguration conf) throws IOException {
    mStore.initFromConf(conf);
  }

  /** {@inheritDoc} */
  @Override
  public KeyValueStoreReader<K, V> open() throws IOException, InterruptedException {
    return new Reader<K, V>(new AvroRecordKeyValueArrayStore.Reader<K, V>(
        getConf(), getExpandedInputPaths(), mStore.getReaderSchema(),
        mStore.getKeyFieldName(), 1));
  }

  /**
   * Reads an entire Avro container file of (key, value) records into memory, indexed
   * by "key."
   *
   * <p>Lookups for a key <i>K</i> will return the first record
   * in the file where the key field has value <i>K</i>.</p>
   * @param <K> the key type expected to be implemented by the keys to this reader.
   * @param <V> the value type expected to be accessed by keys in the reader.
   */
  static class Reader<K, V extends IndexedRecord> extends AvroKVSingleValueReader<K, V> {
    /**
     * Constructs a key/record reader over an Avro file.
     *
     * @param reader The array reader to use for reading from the file.
     * @throws IOException If there is an error.
     */
    public Reader(AvroRecordKeyValueArrayStore.Reader<K, V> reader) throws IOException {
      super(reader);
    }
  }
}

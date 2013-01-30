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
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.KeyValueStoreConfiguration;
import org.kiji.mapreduce.kvstore.KeyValueStoreReader;
import org.kiji.mapreduce.util.LruCache;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiURI;
import org.kiji.schema.KijiURIException;
import org.kiji.schema.util.ReferenceCountableUtils;

/**
 * KeyValueStore lookup implementation based on a Kiji table.
 *
 * <p>Allows you to use a single fully-qualified column of a Kiji table as a
 * key-value store (using the EntityId associated with each row as the key).</p>
 *
 * <p>This operates over the most recent timestamped value (within the optionally-configured
 * timestamp range associated with the Store).</p>
 *
 * <p>This implementation assumes that the column is immutable while being used in this
 * fashion. It may cache values to improve repeated read performance. You can set the
 * cache size with {@link KijiTableKeyValueStore.Builder#withCacheLimit(int)}.</p>
 *
 * <p>When configuring a KijiTableKeyValueStore from a kvstores XML file, the following
 * properties may be used to specify the behavior of this class:</p>
 * <ul>
 *   <li><tt>table.uri</tt> - The Kiji URI for the table backing this store.</li>
 *   <li><tt>column</tt> - The family and qualifier of the column representing the values
 *       in this store. e.g., <tt>info:name</tt></li>
 *   <li><tt>min.ts</tt> - A <tt>long</tt> value representing the minimum timestamp to
 *       include in the results for this KeyValueStore.</li>
 *   <li><tt>max.ts</tt> - A <tt>long</tt> value representing the maximum timestamp to
 *       include in the results for this KeyValueStore.</li>
 *   <li><tt>cache.size</tt> - An <tt>int</tt> value representing the number of results
 *       to cache locally. (Default is 100; set to 0 to disable caching.)</li>
 *   <li><tt>avro.reader.schema</tt> - The common Avro reader schema used to
 *       deserialize values from
 *       the value column to return them to the client.</li>
 * </ul>
 *
 * @param <V> the value type returned by this key-value store.
 */
@ApiAudience.Public
public final class KijiTableKeyValueStore<V> implements Configurable, KeyValueStore<String, V> {
  // TODO(WIBI-1652): Add a flag that allows users to specify hex-strings (pre-hashed entity ids)
  // as keys instead of "vanilla" key strings.

  /** Cache the most recent 100 lookups in memory. */
  private static final int DEFAULT_MAX_OBJECTS_TO_CACHE = 100;

  // See javadoc for this class to understand the definitions of these configuration keys.

  private static final String CONF_TABLE_URI = "table.uri";
  private static final String CONF_COLUMN = "column";
  private static final String CONF_MIN_TS = "min.ts";
  private static final String CONF_MAX_TS = "max.ts";
  private static final String CONF_CACHE_SIZE = "cache.size";
  private static final String CONF_READER_SCHEMA = "avro.reader.schema";

  private KijiURI mTableUri;
  private KijiColumnName mColumn;
  private long mMinTs;
  private long mMaxTs;
  private int mMaxObjectsToCache = DEFAULT_MAX_OBJECTS_TO_CACHE;
  private Schema mReaderSchema;
  private Configuration mConf;

  /** true if the user has called open() on this object. */
  private boolean mOpened;

  /**
   * A Builder-pattern class that configures and creates new KijiTableKeyValueStore
   * instances. You should use this to specify the input to this KeyValueStore.
   * Call the build() method to return a new KijiTableKeyValueStore instance.
   */
  @ApiAudience.Public
  public static final class Builder {
    private KijiURI mTableUri;
    private KijiColumnName mColumn;
    private long mMinTs;
    private long mMaxTs;
    private int mMaxObjectsToCache = DEFAULT_MAX_OBJECTS_TO_CACHE;
    private Schema mReaderSchema;
    private Configuration mConf;

    /**
     * Private, default constructor. Call the builder() method of this KeyValueStore
     * to get a new builder instance.
     */
    private Builder() {
      mMaxObjectsToCache = DEFAULT_MAX_OBJECTS_TO_CACHE;
      mMinTs = 0;
      mMaxTs = HConstants.LATEST_TIMESTAMP;
      mConf = new Configuration();
    }

    /**
     * Sets the Configuration to use to connect to Kiji.
     *
     * @param conf the Configuration to use.
     * @return this builder instance.
     */
    public Builder withConfiguration(Configuration conf) {
      mConf = conf;
      return this;
    }

    /**
     * Sets the table to use as the backing store.
     *
     * @param tableUri the Kiji table URI to use.
     * @return this builder instance.
     */
    public Builder withTable(KijiURI tableUri) {
      checkTableUri(tableUri);
      mTableUri = tableUri;
      return this;
    }

    /**
     * Sets the column to retrieve values from.
     *
     * @param colName the column to use.
     * @return this builder instance.
     */
    public Builder withColumn(KijiColumnName colName) {
      if (!colName.isFullyQualified()) {
        throw new IllegalArgumentException("Must specify a fully-qualified column, not a map.");
      }
      mColumn = colName;
      return this;
    }

    /**
     * Sets the column to retrieve values from.
     *
     * @param family the column family to use.
     * @param qualifier the column qualifier to use.
     * @return this builder instance.
     */
    public Builder withColumn(String family, String qualifier) {
      return withColumn(new KijiColumnName(family, qualifier));
    }

    /**
     * Sets the oldest timestamp to retrieve values from in the column.
     *
     * @param timestamp the oldest timestamp to consider.
     * @return this builder instance.
     */
    public Builder withMinTimestamp(long timestamp) {
      mMinTs = timestamp;
      return this;
    }

    /**
     * Sets the newest timestamp to retrieve values from in the column.
     *
     * @param timestamp the newest timestamp to consider.
     * @return this builder instance.
     */
    public Builder withMaxTimestamp(long timestamp) {
      mMaxTs = timestamp;
      return this;
    }

    /**
     * Sets the maximum number of lookups to cache in memory.
     *
     * <p>Defaults to 100.</p>
     *
     * @param numValues the maximum number of values to keep in the cache.
     * @return this builder instance.
     */
    public Builder withCacheLimit(int numValues) {
      mMaxObjectsToCache = numValues;
      return this;
    }

    /**
     * Sets the reader schema to use when deserializing values from the value column.
     * If set to null, will use the common reader schema associated with the column.
     *
     * @param schema the reader schema to use.
     * @return this builder instance.
     */
    public Builder withReaderSchema(Schema schema) {
      mReaderSchema = schema;
      return this;
    }

    /**
     * Throws an IllegalArgumentException if tableUri is invalid. The URI
     * is valid iff it is not null, specifies a Kiji instance, and specifies
     * a Kiji table within that instance to read.
     *
     * @param tableUri a Kiji URI that must specify an instance and table.
     * @throws IllegalArgumentException if the uri is invalid.
     */
    private void checkTableUri(KijiURI tableUri) {
      if (null == tableUri) {
        throw new IllegalArgumentException("Must specify non-null table URI");
      }

      String tableName = tableUri.getTable();
      if (null == tableName || tableName.isEmpty()) {
        throw new IllegalArgumentException("Must specify a non-empty table name");
      }
    }

    /**
     * Build a new KijiTableKeyValueStore instance.
     *
     * @param <V> The value type for the KeyValueStore.
     * @return the initialized KeyValueStore.
     */
    public <V> KijiTableKeyValueStore<V> build() {
      checkTableUri(mTableUri);
      if (null == mColumn || !mColumn.isFullyQualified()) {
        throw new IllegalArgumentException("Must specify a fully-qualified column");
      }
      if (mMinTs > mMaxTs) {
        throw new IllegalArgumentException("Minimum timestamp must be less than max timestamp");
      }

      // Check that the table exists, so users can fail fast before finding this
      // IO error after the MapReduce job has started.
      Kiji kiji = null;
      KijiTable kijiTable = null;
      try {
        kiji = Kiji.Factory.open(mTableUri, mConf);
        kijiTable = kiji.openTable(mTableUri.getTable());
      } catch (IOException ioe) {
        throw new IllegalArgumentException("Could not open table: " + mTableUri, ioe);
      } finally {
        IOUtils.closeQuietly(kijiTable);
        ReferenceCountableUtils.releaseQuietly(kiji);
      }

      return new KijiTableKeyValueStore<V>(this);
    }
  }

  /**
   * Creates a new KijiTableKeyValueStore.Builder instance that can be used
   * to configure and create a new KeyValueStore.
   *
   * @return a new Builder instance.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Reflection-only constructor. Used only for reflection. You should create and configure
   * KijiTableKeyValueStore instances by using a builder;
   * call KijiTableKeyValueStore.builder() to get a new builder instance.
   */
  public KijiTableKeyValueStore() {
    this(builder());
  }

  /**
   * Constructor that up this KeyValueStore using a builder.
   *
   * @param builder the builder instance to read configuration from.
   */
  private KijiTableKeyValueStore(Builder builder) {
    mTableUri = builder.mTableUri;
    mColumn = builder.mColumn;
    mMinTs = builder.mMinTs;
    mMaxTs = builder.mMaxTs;
    mMaxObjectsToCache = builder.mMaxObjectsToCache;
    mReaderSchema = builder.mReaderSchema;
    mConf = builder.mConf;
  }

  /**
   * Set the configuration object to refer to.
   *
   * @param conf the Configuration object to use.
   */
  @Override
  public void setConf(Configuration conf) {
    if (mOpened) {
      // Don't allow mutation after we start using this store for reads.
      throw new IllegalStateException(
          "Cannot set the configuration after a reader has been opened");
    }
    mConf = conf;
  }

  /** @return a copy of the Configuration object we are using. */
  @Override
  public Configuration getConf() {
    return new Configuration(mConf);
  }

  /** {@inheritDoc} */
  @Override
  public void storeToConf(KeyValueStoreConfiguration conf) throws IOException {
    if (null == mTableUri) {
      throw new IOException("Required attribute not set: table URI");
    }
    if (null == mColumn) {
      throw new IOException("Required attribute not set: column");
    }
    if (!mColumn.isFullyQualified()) {
      throw new IOException("Column must be fully qualified");
    }

    conf.set(CONF_TABLE_URI, mTableUri.toString());
    conf.set(CONF_COLUMN, mColumn.toString());
    conf.setLong(CONF_MIN_TS, mMinTs);
    conf.setLong(CONF_MAX_TS, mMaxTs);
    conf.setInt(CONF_CACHE_SIZE, mMaxObjectsToCache);

    if (null != mReaderSchema) {
      conf.set(CONF_READER_SCHEMA, mReaderSchema.toString());
    }
  }

  /** {@inheritDoc} */
  @Override
  public void initFromConf(KeyValueStoreConfiguration conf) throws IOException {
    if (mOpened) {
      // Don't allow mutation after we start using this store for reads.
      throw new IllegalStateException(
          "Cannot set the configuration after a reader has been opened");
    }

    try {
      mTableUri = KijiURI.parse(conf.get(CONF_TABLE_URI));
    } catch (KijiURIException kue) {
      throw new IOException("Error parsing input URI: " + kue.getMessage(), kue);
    }

    mColumn = new KijiColumnName(conf.get(CONF_COLUMN));
    mMinTs = conf.getLong(CONF_MIN_TS, 0);
    mMaxTs = conf.getLong(CONF_MAX_TS, Long.MAX_VALUE);
    mMaxObjectsToCache = conf.getInt(CONF_CACHE_SIZE, DEFAULT_MAX_OBJECTS_TO_CACHE);

    String schemaStr = conf.get(CONF_READER_SCHEMA);
    if (null != schemaStr) {
      mReaderSchema = new Schema.Parser().parse(schemaStr);
    } else {
      mReaderSchema = null; // Just use whatever's in the cell directly.
    }

    // Set the job Configuration so that we get connection parameters to the Kiji table.
    setConf(conf.getDelegate());
  }

  /** {@inheritDoc} */
  @Override
  public KeyValueStoreReader<String, V> open() throws IOException {
    mOpened = true;
    return new TableKVReader();
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object otherObj) {
    if (otherObj == this) {
      return true;
    } else if (null == otherObj) {
      return false;
    } else if (!otherObj.getClass().equals(getClass())) {
      return false;
    }

    @SuppressWarnings("unchecked")
    KijiTableKeyValueStore<V> other = (KijiTableKeyValueStore<V>) otherObj;

    if (null == mTableUri) {
      if (other.mTableUri != null) {
        return false;
      }
    } else if (!mTableUri.equals(other.mTableUri)) {
      return false;
    }

    if (null == mColumn) {
      if (other.mColumn != null) {
        return false;
      }
    } else if (!mColumn.equals(other.mColumn)) {
      return false;
    }

    if (mMinTs != other.mMinTs) {
      return false;
    } else if (mMaxTs != other.mMaxTs) {
      return false;
    } else if (mMaxObjectsToCache != other.mMaxObjectsToCache) {
      return false;
    }

    if (null == mReaderSchema) {
      if (other.mReaderSchema != null) {
        return false;
      }
    } else if (!mReaderSchema.equals(other.mReaderSchema)) {
      return false;
    }

    // We don't care about mConf.

    return true;
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    int hash = 17;
    hash = hash + (null == mTableUri ? 0 : mTableUri.hashCode()) * 31;
    hash = hash + (null == mColumn ? 0 : mColumn.hashCode()) * 31;
    return hash;
  }

  /** KeyValueStoreReader implementation that reads from a Kiji table. */
  @ApiAudience.Private
  private final class TableKVReader implements KeyValueStoreReader<String, V> {
    /** Kiji instance to use. */
    private Kiji mKiji;
    /** Kiji Table instance to open. */
    private KijiTable mKijiTable;
    /** KijiTableReader to read the table. */
    private KijiTableReader mTableReader;
    /** Data request to use for all lookups. */
    private final KijiDataRequest mDataReq;
    /** If the user has requested result caching, do this here. */
    private final Map<EntityId, V> mResultCache;

    /**
     * Creates a new TableKVReader.
     *
     * @throws IOException if there's an error opening the Kiji table.
     */
    private TableKVReader() throws IOException {
      Configuration conf = getConf();
      mKiji = Kiji.Factory.open(mTableUri, conf);
      mKijiTable = mKiji.openTable(mTableUri.getTable());
      mTableReader = mKijiTable.openTableReader();

      mDataReq = new KijiDataRequest()
          .addColumn(new KijiDataRequest.Column(mColumn.getFamily(), mColumn.getQualifier())
              .withMaxVersions(1))
          .withTimeRange(mMinTs, mMaxTs);

      if (mMaxObjectsToCache > 1) {
        mResultCache = LruCache.create(mMaxObjectsToCache);
      } else {
        mResultCache = null;
      }
    }

    /** {@inheritDoc} */
    @Override
    public boolean isOpen() {
      return null != mKijiTable;
    }

    /** {@inheritDoc} */
    @Override
    public V get(String key) throws IOException {
      if (!isOpen()) {
        throw new IOException("Closed");
      }

      EntityId entityId = mKijiTable.getEntityId(key);
      // Check the cache first.
      if (null != mResultCache && mResultCache.containsKey(entityId)) {
        return mResultCache.get(entityId);
      }

      // Now do a full lookup.
      KijiRowData rowData = mTableReader.get(entityId, mDataReq);

      if (null == rowData) {
        return null;
      }

      if (rowData.containsColumn(mColumn.getFamily(), mColumn.getQualifier())) {
        // If mReaderSchema is null, that's ok; it uses the cell writer schema.
        // TODO: But we must actually use it if it's not null!
        V val = rowData.<V>getMostRecentValue(mColumn.getFamily(), mColumn.getQualifier());
        if (null != mResultCache) {
          mResultCache.put(entityId, val);
        }

        return val;
      } else {
        if (null != mResultCache) {
          mResultCache.put(entityId, null);
        }
        return null;
      }
    }

    /** {@inheritDoc} */
    @Override
    public boolean containsKey(String key) throws IOException {
      if (!isOpen()) {
        throw new IOException("Closed");
      }

      EntityId entityId = mKijiTable.getEntityId(key);
      if (null != mResultCache && mResultCache.containsKey(entityId)) {
        return true; // Cache hit.
      }

      KijiRowData rowData = mTableReader.get(entityId, mDataReq);

      if (null == rowData) {
        return false;
      }

      return rowData.containsColumn(mColumn.getFamily(), mColumn.getQualifier());
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
      try {
        IOUtils.closeQuietly(mTableReader);
        IOUtils.closeQuietly(mKijiTable);
        ReferenceCountableUtils.releaseQuietly(mKiji);
      } finally {
        mTableReader = null;
        mKijiTable = null;
        mKiji = null;
      }
    }
  }
}

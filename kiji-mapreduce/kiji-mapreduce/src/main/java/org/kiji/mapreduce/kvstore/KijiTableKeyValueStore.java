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
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.KeyValueStore;
import org.kiji.mapreduce.KeyValueStoreConfiguration;
import org.kiji.mapreduce.KeyValueStoreReader;
import org.kiji.mapreduce.KijiMapReduceJob;
import org.kiji.mapreduce.util.LruCache;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiConfiguration;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;

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
 * cache size with {@link #setCacheLimit(int)}.</p>
 *
 * <p>When configuring a KijiTableKeyValueStore from a kvstores XML file, the following
 * properties may be used to specify the behavior of this class:</p>
 * <ul>
 *   <li><tt>table</tt> - The name of the table backing this store.</li>
 *   <li><tt>col</tt> - The family and qualifier of the column representing the values
 *       in this store.</li>
 *   <li><tt>min.ts</tt> - A <tt>long</tt> value representing the minimum timestamp to
 *       include in the results for this KeyValueStore.</li>
 *   <li><tt>max.ts</tt> - A <tt>long</tt> value representing the maximum timestamp to
 *       include in the results for this KeyValueStore.</li>
 *   <li><tt>cache.size</tt> - An <tt>int</tt> value representing the number of results
 *       to cache locally. (Default is 100; set to 0 to disable caching.)</li>
 *   <li><tt>schema</tt> - The common Avro reader schema used to deserialize values from
 *       the value column to return them to the client.</li>
 * </ul>
 *
 * @param <V> the value type returned by this key-value store.
 */
@ApiAudience.Public
public class KijiTableKeyValueStore<V> extends KeyValueStore<String, V> implements Configurable {
  // TODO(WIBI-1652): Add a flag that allows users to specify hex-strings (pre-hashed entity ids)
  // as keys instead of "vanilla" key strings.

  /** Cache the most recent 100 lookups in memory. */
  private static final int DEFAULT_MAX_OBJECTS_TO_CACHE = 100;

  // TODO(WIBI-1546): Make these public, and document what these variables are for.
  private static final String CONF_TABLE = "table";
  private static final String CONF_COLUMN = "col";
  private static final String CONF_MIN_TS = "min.ts";
  private static final String CONF_MAX_TS = "max.ts";
  private static final String CONF_CACHE_SIZE = "cache.size";
  private static final String CONF_READER_SCHEMA = "schema";

  private String mTableName;
  private KijiColumnName mColumn;
  private long mMinTs;
  private long mMaxTs;
  private int mMaxObjectsToCache = DEFAULT_MAX_OBJECTS_TO_CACHE;
  private Schema mReaderSchema;
  private Configuration mConf;

  /** Constructs an unconfigured KeyValueStore. */
  public KijiTableKeyValueStore() {
    this(null, null);
  }

  /**
   * Constructor that sets that name of the KeyValueStore, and the table and column to
   * retrieve.
   *
   * @param tableName the name of the Kiji table to retrieve.
   * @param column the fully-qualified column name to retrieve.
   */
  public KijiTableKeyValueStore(String tableName, KijiColumnName column) {
    this(tableName, column, 0L, Long.MAX_VALUE);
  }

  /**
   * Constructor that sets that name of the KeyValueStore, and the table and column to
   * retrieve.
   *
   * @param tableName the name of the Kiji table to retrieve.
   * @param column the fully-qualified column name to retrieve.
   * @param minTimestamp the oldest timestamp to consider when retrieving data.
   * @param maxTimestamp the newest timestamp to consider when retrieving data.
   */
  public KijiTableKeyValueStore(String tableName, KijiColumnName column,
      long minTimestamp, long maxTimestamp) {
    mTableName = tableName;
    mColumn = column;
    mMinTs = minTimestamp;
    mMaxTs = maxTimestamp;
  }

  /**
   * Sets the table name to retrieve.
   *
   * @param tableName the table name to use for lookups.
   */
  public void setTableName(String tableName) {
    mTableName = tableName;
  }

  /**
   * Sets the column to use for values in the lookup store.
   *
   * @param column the column to retrieve. Must be fully-qualified (cannot be a map-type family).
   */
  public void setColumn(KijiColumnName column) {
    mColumn = column;
  }

  /**
   * Sets the minimum timestamp to use for lookups.
   *
   * @param minTs the oldest timestamp value to consider when doing lookups.
   */
  public void setMinTimestamp(long minTs) {
    mMinTs = minTs;
  }

  /**
   * Sets the maximum timestamp to use for lookups.
   *
   * @param maxTs the newest timestamp value to consider when doing lookups.
   */
  public void setMaxTimestamp(long maxTs) {
    mMaxTs = maxTs;
  }

  /**
   * Sets the maximum number of lookups to cache in memory.
   *
   * <p>Defaults to 100.</p>
   *
   * @param numValues the maximum number of values to keep in the cache.
   */
  public void setCacheLimit(int numValues) {
    mMaxObjectsToCache = numValues;
  }

  /**
   * Sets the reader schema to use when deserializing values from the value column.
   *
   * @param schema the reader schema to use.
   */
  public void setReaderSchema(Schema schema) {
    mReaderSchema = schema;
  }

  /**
   * Set the configuration object to refer to.
   *
   * @param conf the Configuration object to use.
   */
  @Override
  public void setConf(Configuration conf) {
    mConf = conf;
  }

  /** @return the Configuration object to use. */
  @Override
  public Configuration getConf() {
    return mConf;
  }

  /** {@inheritDoc} */
  @Override
  public void storeToConf(KeyValueStoreConfiguration conf) throws IOException {
    if (null == mTableName) {
      throw new IOException("Required attribute not set: tableName");
    }
    if (null == mColumn) {
      throw new IOException("Required attribute not set: column");
    }

    if (!mColumn.isFullyQualified()) {
      throw new IOException("Column must be fully qualified");
    }

    conf.set(CONF_TABLE, mTableName);
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
    mTableName = conf.get(CONF_TABLE);
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
  public KeyValueStoreReader<String, V> open() throws IOException, InterruptedException {
    return new TableKVReader();
  }

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

    if (mTableName == null) {
      if (other.mTableName != null) {
        return false;
      }
    } else if (!mTableName.equals(other.mTableName)) {
      return false;
    }

    if (mColumn == null) {
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

    if (mReaderSchema == null) {
      if (other.mReaderSchema != null) {
        return false;
      }
    } else if (!mReaderSchema.equals(other.mReaderSchema)) {
      return false;
    }

    // We don't care about mConf.

    return true;
  }

  @Override
  public int hashCode() {
    int tableHash = null == mTableName ? 0 : mTableName.hashCode();
    int colHash = null == mColumn ? 0 : mColumn.hashCode();
    return tableHash ^ colHash;
  }

  /** KeyValueStoreReader implementation that reads from a Kiji table. */
  private final class TableKVReader extends KeyValueStoreReader<String, V> {
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
      String instanceName = KijiMapReduceJob.getInstanceName(conf);
      mKiji = Kiji.Factory.open(new KijiConfiguration(conf, instanceName));
      mKijiTable = mKiji.openTable(mTableName);
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
    public V get(String key) throws IOException, InterruptedException {
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
    public boolean containsKey(String key) throws IOException, InterruptedException {
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
        if (null != mTableReader) {
          mTableReader.close();
        }
        if (null != mKijiTable) {
          mKijiTable.close();
        }
        if (null != mKiji) {
          mKiji.close();
        }
      } finally {
        mTableReader = null;
        mKijiTable = null;
        mKiji = null;
      }
    }
  }
}

/**
 * (c) Copyright 2014 WibiData, Inc.
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

package org.kiji.schema.impl.cassandra;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.delete;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.QueryBuilder.ttl;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequest.Column;
import org.kiji.schema.avro.RowKeyComponent;
import org.kiji.schema.avro.RowKeyFormat2;
import org.kiji.schema.cassandra.CassandraColumnName;
import org.kiji.schema.cassandra.CassandraTableName;

/**
 * A cache for prepared CQL statements scoped to a single Kiji row key format.
 *
 * Responsibilities:
 *
 * <ul>
 *   <li>Generating and preparing CQL statements</li>
 *   <li>Caching prepared CQL statements</li>
 *   <li>Binding parameters to prepared CQL statements</li>
 * </ul>
 *
 * <p>
 *   Only the {@link RowKeyFormat2} is needed in order to construct queries, therefore a single
 *   instance of this class can be shared among all open Kiji tables which share a row key format.
 *   Kiji tables will often have a unique row key format in an instance, but this is not a
 *   requirement. When tables share a row key format, a single instance of this class can be shared.
 * </p>
 */
@ThreadSafe
public class CQLStatementCache {
  private static final Logger LOG = LoggerFactory.getLogger(CQLStatementCache.class);

  /**
   * Prefix added to Kiji entity ID component names to make the Cassandra column name. Adding a
   * prefix is necessary to avoid name conflicts between the entity ID component names and other
   * Cassandra column names.
   */
  private static final String ENTITY_ID_PREFIX = "eid_";

  private static final int ENTITY_ID_BATCH_SIZE = 250;

  private final RowKeyFormat2 mRowKeyFormat;

  /** Cassandra primary key columns belonging to the Kiji Entity ID. */
  private final List<String> mEntityIDColumns;

  /** Cassandra partition key columns. */
  private final List<String> mPartitionKeyColumns;

  /** Cassandra clustering columns belonging to the Kiji Entity ID. */
  private final List<String> mEntityIDClusteringColumns;

  private final Session mSession;

  /**
   * Marker interface for statement 'keys'.  These keys are used to lookup the statement in the
   * cache. Statement keys must override {@code hashCode} and {@code equals}, and must take into
   * account the type of the key when doing these operations. Unfortunately there is not way to
   * encode this requirement in the type system.
   */
  private interface StatementKey {
    /**
     * Create an unprepared CQL statement corresponding to the key. The unprepared statement will
     * automatically be prepared and cached by {@code mCache}.
     *
     * @return An unprepared CQL statement corresponding to the key.
     */
    RegularStatement createUnpreparedStatement();
  }

  private final LoadingCache<StatementKey, PreparedStatement> mCache =
      CacheBuilder
          .newBuilder()
          .expireAfterAccess(60, TimeUnit.MINUTES) // Avoid caching one-off queries forever
          .build(
              new CacheLoader<StatementKey, PreparedStatement>() {
                /** {@inheritDoc} */
                @Override
                public PreparedStatement load(final StatementKey key) {
                  final RegularStatement statement = key.createUnpreparedStatement();
                  LOG.debug("Preparing statement: {}.", statement);
                  return mSession.prepare(statement);
                }
              });

  /**
   * Create a new CQL statement cache for a Kiji table.
   *
   * @param session The Cassandra connection.
   * @param rowKeyFormat The table's row key format.
   */
  public CQLStatementCache(final Session session, final RowKeyFormat2 rowKeyFormat) {
    mSession = session;
    mRowKeyFormat = rowKeyFormat;
    // Cassandra Kiji only supports RowKeyFormat2
    switch (rowKeyFormat.getEncoding()) {
      case RAW: {
        mEntityIDColumns = ImmutableList.of(CQLUtils.RAW_KEY_COL);
        break;
      }
      case FORMATTED: {
        final ImmutableList.Builder<String> entityIDColumns = ImmutableList.builder();
        for (final RowKeyComponent component : mRowKeyFormat.getComponents()) {
          entityIDColumns.add(ENTITY_ID_PREFIX + component.getName());
        }
        mEntityIDColumns = entityIDColumns.build();
        break;
      }
      default:
        throw new IllegalArgumentException(
            String.format("Unknown row key encoding %s.", mRowKeyFormat.getEncoding()));
    }

    mPartitionKeyColumns =
        mEntityIDColumns.subList(0, rowKeyFormat.getRangeScanStartIndex());

    mEntityIDClusteringColumns =
        mEntityIDColumns.subList(rowKeyFormat.getRangeScanStartIndex(),  mEntityIDColumns.size());
  }

  /**
   * Get the entity ID component values from an Entity ID.
   *
   * @param entityID The entity ID.
   * @return The entity ID's component values.
   */
  private List<Object> getEntityIDComponents(
      final EntityId entityID
  ) {
    switch (mRowKeyFormat.getEncoding()) {
      case RAW: {
        return ImmutableList.<Object>of(ByteBuffer.wrap(entityID.getHBaseRowKey()));
      }
      case FORMATTED: {
        return entityID.getComponents();
      }
      default: throw new IllegalArgumentException(
          String.format("Unknown row key encoding %s.", mRowKeyFormat.getEncoding()));
    }
  }

  /*************************************************************************************************
   * Get Statement
   ************************************************************************************************/

  /**
   * Create a statement for retrieving a column in single row of a Cassandra Kiji table.
   *
   * @param tableName The Cassandra locality group table.
   * @param entityId The Kiji entity ID.
   * @param column The translated Kiji column name.
   * @param dataRequest The data request defining overall query parameters.
   * @param columnRequest The column request defining the column to retrieve.
   * @return A statement for querying the column.
   */
  public Statement createGetStatement(
      final CassandraTableName tableName,
      final EntityId entityId,
      final CassandraColumnName column,
      final KijiDataRequest dataRequest,
      final Column columnRequest
  ) {
    Preconditions.checkArgument(entityId.getComponents().size() == mEntityIDColumns.size(),
        "Entity ID components mismatch. entity ID components: {}, entity ID columns: {}",
        entityId.getComponents(), mEntityIDColumns);

    // Retrieve the prepared statement from the cache

    final boolean isQualifiedGet = column.containsQualifier();
    final boolean hasMaxTimestamp =
        column.containsQualifier() && dataRequest.getMaxTimestamp() != Long.MAX_VALUE;
    final boolean hasMinTimestamp =
        column.containsQualifier() && dataRequest.getMinTimestamp() != 0;

    final GetStatementKey key =
        new GetStatementKey(tableName, isQualifiedGet, hasMaxTimestamp, hasMinTimestamp);

    final PreparedStatement statement = mCache.getUnchecked(key);

    // Bind the parameters to the prepared statement

    // The extra 5 slots are for the family, qualifier, min/max timestamps, and max versions
    final List<Object> values = Lists.newArrayListWithCapacity(mEntityIDColumns.size() + 5);

    values.addAll(getEntityIDComponents(entityId));
    values.add(column.getFamilyBuffer());

    if (column.containsQualifier()) {
      values.add(column.getQualifierBuffer());
    }

    if (hasMaxTimestamp) {
      values.add(dataRequest.getMaxTimestamp());
    }

    if (hasMinTimestamp) {
      values.add(dataRequest.getMinTimestamp());
    }

    // Only limit the number of versions if this is a qualified get. Family gets will need to limit
    // versions on the client side.
    if (isQualifiedGet) {
      values.add(columnRequest.getMaxVersions());
    }

    final Statement boundStatement = statement.bind(values.toArray());

    if (columnRequest.getPageSize() != 0) {
      boundStatement.setFetchSize(columnRequest.getPageSize());
    }

    return boundStatement;
  }

  /**
   * A statement cache key containing all of the information necessary to create a get statement.
   */
  private final class GetStatementKey implements StatementKey {
    private final CassandraTableName mTable;
    private final boolean mIsQualifiedGet;
    private final boolean mHasMaxTimestamp;
    private final boolean mHasMinTimestamp;

    /**
     * Create a new get statement key. This key contains all of the information necessary to
     * create an unbound statement for the get.
     *
     * @param table The Cassandra table name.
     * @param isQualifiedGet Whether the get is fully qualified (true) or for an entire family
     *    (false).
     * @param hasMaxTimestamp Whether the includes a max timestamp. Only valid with qualified gets.
     * @param hasMinTimestamp Whether the get includes a min timestamp. Only valid with qualified
     *    gets.
     */
    private GetStatementKey(
        final CassandraTableName table,
        final boolean isQualifiedGet,
        final boolean hasMaxTimestamp,
        final boolean hasMinTimestamp
    ) {
      mTable = table;
      mIsQualifiedGet = isQualifiedGet;
      mHasMaxTimestamp = hasMaxTimestamp;
      mHasMinTimestamp = hasMinTimestamp;
      Preconditions.checkState(!mHasMaxTimestamp || mIsQualifiedGet,
          "Max timestamp may only be set on fully-qualified gets. Key: %s.", this);
      Preconditions.checkState(!mHasMinTimestamp || mIsQualifiedGet,
          "Min timestamp may only be set on fully-qualified gets. Key: %s.", this);
    }

    /**
     * Get the table name.
     *
     * @return The table name.
     */
    public CassandraTableName getTable() {
      return mTable;
    }

    /** {@inheritDoc} */
    @Override
    public RegularStatement createUnpreparedStatement() {
      final Select select =
          select()
              .all()
              .from(mTable.getKeyspace(), mTable.getTable());

      for (final String componentColumn : mEntityIDColumns) {
        select.where(eq(componentColumn, bindMarker()));
      }

      select.where(eq(CQLUtils.FAMILY_COL, bindMarker()));

      if (mIsQualifiedGet) {
        select.where(eq(CQLUtils.QUALIFIER_COL, bindMarker()));
        if (mHasMaxTimestamp) {
          select.where(lt(CQLUtils.VERSION_COL, bindMarker()));
        }
        if (mHasMinTimestamp) {
          select.where(gte(CQLUtils.VERSION_COL, bindMarker()));
        }
        select.limit(bindMarker());
      }

      return select;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("table", mTable)
          .add("isQualifiedGet", mIsQualifiedGet)
          .add("hasMaxTimestamp", mHasMaxTimestamp)
          .add("hashMinTimestamp", mHasMinTimestamp)
          .toString();
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
      return Objects.hashCode(
          this.getClass(),
          mTable,
          mIsQualifiedGet,
          mHasMaxTimestamp,
          mHasMinTimestamp);
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      final GetStatementKey other = (GetStatementKey) obj;
      return Objects.equal(mTable, other.mTable)
          && Objects.equal(mIsQualifiedGet, other.mIsQualifiedGet)
          && Objects.equal(mHasMaxTimestamp, other.mHasMaxTimestamp)
          && Objects.equal(mHasMinTimestamp, other.mHasMinTimestamp);
    }
  }

  /*************************************************************************************************
   * Entity ID Scan Statement
   ************************************************************************************************/

  /**
   * Create a CQL statement for selecting the columns that make up the Entity ID from a Cassandra
   * Kiji Table.
   *
   * @param table The translated Cassandra table name.
   * @param tokenRange A range of tokens to scan.
   * @return a statement that will get the single column.
   */
  public Statement createEntityIDScanStatement(
      final CassandraTableName table,
      final Range<Long> tokenRange
  ) {

    // Retrieve the prepared statement from the cache

    // We can only use the 'DISTINCT' optimization if all entity ID components are part of the
    // partition key. CQL does not allow DISTINCT over non partition-key columns.
    final boolean useDistinct = mEntityIDClusteringColumns.isEmpty();

    final Optional<BoundType> lowerBound =
        tokenRange.hasLowerBound()
            ? Optional.of(tokenRange.lowerBoundType())
            : Optional.<BoundType>absent();

    final Optional<BoundType> upperBound =
        tokenRange.hasUpperBound()
            ? Optional.of(tokenRange.upperBoundType())
            : Optional.<BoundType>absent();

    final EntityIDScanStatementKey key =
        new EntityIDScanStatementKey(table, lowerBound, upperBound, useDistinct);
    final PreparedStatement statement = mCache.getUnchecked(key);

    // Bind the parameters to the prepared statement

    // slots are for the min/max token
    final List<Object> values = Lists.newArrayListWithCapacity(2);

    if (tokenRange.hasLowerBound()) {
      values.add(tokenRange.lowerEndpoint());
    }

    if (tokenRange.hasUpperBound()) {
      values.add(tokenRange.upperEndpoint());
    }

    return statement.bind(values.toArray()).setFetchSize(ENTITY_ID_BATCH_SIZE);
  }

  /**
   * A statement cache key containing all of the information necessary to create an entity ID scan
   * statement.
   */
  private final class EntityIDScanStatementKey implements StatementKey {
    private final CassandraTableName mTable;
    private final Optional<BoundType> mLowerBound;
    private final Optional<BoundType> mUpperBound;
    private final boolean mUseDistinct;

    /**
     * Create a new entity ID scan statement key.
     *
     * @param table The Cassandra table name.
     * @param lowerBound The lower token bound type (open or closed), if present.
     * @param upperBound The upper token bound type (open or closed), if present.
     * @param useDistinct Whether the scan can use the 'DISTINCT' clause optimization.
     */
    private EntityIDScanStatementKey(
        final CassandraTableName table,
        final Optional<BoundType> lowerBound,
        final Optional<BoundType> upperBound,
        final boolean useDistinct
    ) {
      mTable = table;
      mLowerBound = lowerBound;
      mUpperBound = upperBound;
      mUseDistinct = useDistinct;
    }

    /** {@inheritDoc} */
    @Override
    public RegularStatement createUnpreparedStatement() {
      final String tokenColumn =
          String.format("token(%s)", CQLUtils.COMMA_JOINER.join(mPartitionKeyColumns));

      final Select.Selection selection = select();
      selection.column(tokenColumn);

      for (final String column : mEntityIDColumns) {
        selection.column(column);
      }

      if (mUseDistinct) {
        selection.distinct();
      }

      final Select select = selection.from(mTable.getKeyspace(), mTable.getTable());

      if (mLowerBound.isPresent()) {
        switch (mLowerBound.get()) {
          case OPEN: {
            select.where(gt(tokenColumn, bindMarker()));
            break;
          }
          case CLOSED: {
            select.where(gte(tokenColumn, bindMarker()));
            break;
          }
        }
      }

      if (mUpperBound.isPresent()) {
        switch (mUpperBound.get()) {
          case OPEN: {
            select.where(lt(tokenColumn, bindMarker()));
            break;
          }
          case CLOSED: {
            select.where(lte(tokenColumn, bindMarker()));
            break;
          }
        }
      }

      return select;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("table", mTable)
          .add("lowerBound", mLowerBound)
          .add("upperBound", mUpperBound)
          .toString();
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
      return Objects.hashCode(this.getClass(), mTable, mLowerBound, mUpperBound, mUseDistinct);
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      final EntityIDScanStatementKey other = (EntityIDScanStatementKey) obj;
      return Objects.equal(this.mTable, other.mTable)
          && Objects.equal(this.mLowerBound, other.mLowerBound)
          && Objects.equal(this.mUpperBound, other.mUpperBound)
          && Objects.equal(this.mUseDistinct, other.mUseDistinct);
    }
  }

  /*************************************************************************************************
   * Insert Statement
   ************************************************************************************************/

  /**
   * Create a CQL statement that executes a Kiji put.
   *
   * @param table The name of the Cassandra table to insert the cell into.
   * @param entityID The entity ID of the row to insert.
   * @param column The column of the cell to insert.
   * @param version The version of the cell to insert.
   * @param value The value to be written to the cell.
   * @param ttl The TTL of the inserted cell, or null if forever.
   * @return a Statement which will insert the cell.
   */
  public Statement createInsertStatment(
      final CassandraTableName table,
      final EntityId entityID,
      final CassandraColumnName column,
      final Long version,
      final ByteBuffer value,
      final Integer ttl
  ) {
    // Retrieve the prepared statement from the cache
    final boolean hasTTL = ttl != null && ttl < 630720000; // 630720000 is the maximum Cassandra TTL

    final InsertStatementKey key = new InsertStatementKey(table, hasTTL);
    final PreparedStatement statement = mCache.getUnchecked(key);

    // Bind the parameters to the prepared statement

    // The extra 5 slots are for the family, qualifier, version, value, and TTL
    final List<Object> values = Lists.newArrayListWithCapacity(mEntityIDColumns.size() + 5);

    values.addAll(getEntityIDComponents(entityID));
    values.add(column.getFamilyBuffer());
    values.add(column.getQualifierBuffer());
    values.add(version);
    values.add(value);

    if (hasTTL) {
      values.add(ttl);
    }

    return statement.bind(values.toArray());
  }

  /**
   * A statement cache key containing all of the information necessary to create an insert
   * statement.
   */
  private final class InsertStatementKey implements StatementKey {
    private final CassandraTableName mTable;
    private final boolean mHasTTL;

    /**
     * Create a new insert statement key.
     *
     * @param table The table that the cell will be inserted into.
     * @param hasTTL Whether the inserted cell has a TTL.
     */
    public InsertStatementKey(final CassandraTableName table, final boolean hasTTL) {
      mTable = table;
      mHasTTL = hasTTL;
    }

    /** {@inheritDoc} */
    @Override
    public RegularStatement createUnpreparedStatement() {

      final Insert insert = insertInto(mTable.getKeyspace(), mTable.getTable());

      for (final String column : mEntityIDColumns) {
        insert.value(column, bindMarker());
      }

      insert
          .value(CQLUtils.FAMILY_COL, bindMarker())
          .value(CQLUtils.QUALIFIER_COL, bindMarker())
          .value(CQLUtils.VERSION_COL, bindMarker())
          .value(CQLUtils.VALUE_COL, bindMarker());

      if (mHasTTL) {
        insert.using(ttl(bindMarker()));
      }

      return insert;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("table", mTable)
          .add("hasTTL", mHasTTL)
          .toString();
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
      return Objects.hashCode(this.getClass(), mTable, mHasTTL);
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      final InsertStatementKey other = (InsertStatementKey) obj;
      return Objects.equal(this.mTable, other.mTable)
          && Objects.equal(this.mHasTTL, other.mHasTTL);
    }
  }

  /*************************************************************************************************
   * Delete Statement
   ************************************************************************************************/

  /**
   * Create a CQL statement to delete a cell.
   *
   * @param table The name of the Cassandra table.
   * @param entityID The entity ID of the row to delete.
   * @param column The column to delete.
   * @param version The version of the cell.
   * @return A CQL statement to delete a cell.
   */
  public Statement createCellDeleteStatement(
      final CassandraTableName table,
      final EntityId entityID,
      final CassandraColumnName column,
      final long version
  ) {
    Preconditions.checkArgument(column.containsQualifier());
    return createDeleteStatement(table, entityID, column, version);
  }

  /**
   * Create a CQL statement to delete a Kiji column from a row.
   *
   * @param table The name of the Cassandra table.
   * @param entityID The entity ID of the row to delete.
   * @param column The column to delete.
   * @return A CQL statement to delete a column.
   */
  public Statement createColumnDeleteStatement(
      final CassandraTableName table,
      final EntityId entityID,
      final CassandraColumnName column
  ) {
    return createDeleteStatement(table, entityID, column, null);
  }

  /**
   * Create a CQL statement to delete all columns in a locality group from a row.
   *
   * @param table The name of the Cassandra table.
   * @param entityID The entity ID of the row to delete.
   * @return A CQL statement to delete a row.
   */
  public Statement createLocalityGroupDeleteStatement(
      final CassandraTableName table,
      final EntityId entityID
  ) {
    return createDeleteStatement(table, entityID, null, null);
  }

  /**
   * Create a CQL statement for deleting from a locality group in a row of a Cassandra Kiji table.
   *
   * @param table The name of the Cassandra table.
   * @param entityID The entity ID of the row to delete.
   * @param column The column to delete.
   * @param version The version of the cell to delete.
   * @return A statement which will delete the column.
   */
  private Statement createDeleteStatement(
      CassandraTableName table,
      EntityId entityID,
      CassandraColumnName column,
      Long version
  ) {
    // Retrieve the prepared statement from the cache
    final boolean hasFamily = column != null;
    final boolean hasQualifier = hasFamily && column.containsQualifier();
    final boolean hasVersion = hasQualifier && version != null;

    final DeleteStatementKey key =
        new DeleteStatementKey(table, hasFamily, hasQualifier, hasVersion);
    final PreparedStatement statement = mCache.getUnchecked(key);

    // Bind the parameters to the prepared statement

    // The extra 3 slots are for the family, qualifier, and version
    final List<Object> values = Lists.newArrayListWithCapacity(mEntityIDColumns.size() + 3);

    values.addAll(getEntityIDComponents(entityID));
    if (hasFamily) {
      values.add(column.getFamilyBuffer());
      if (hasQualifier) {
        values.add(column.getQualifierBuffer());
        if (hasVersion) {
          values.add(version);
        }
      }
    }

    return statement.bind(values.toArray());
  }

  /**
   * A statement cache key containing all of the information necessary to create a delete statement.
   */
  private final class DeleteStatementKey implements StatementKey {
    private final CassandraTableName mTable;
    private boolean mHasFamily;
    private boolean mHasQualifier;
    private boolean mHasVersion;

    /**
     * Create a new delete key.
     *
     * @param table The Cassandra Kiji table.
     * @param hasFamily Whether the delete is for a specific family.
     * @param hasQualifier Whether the delete is for a specific qualifier.
     * @param hasVersion Whether the delete is for a specific version.
     */
    private DeleteStatementKey(
        final CassandraTableName table,
        final boolean hasFamily,
        final boolean hasQualifier,
        final boolean hasVersion
    ) {
      mTable = table;
      mHasFamily = hasFamily;
      mHasQualifier = hasQualifier;
      mHasVersion = hasVersion;

      // preconditions are last so that the #toString is valid
      Preconditions.checkArgument(hasFamily || !hasQualifier,
          "Delete statement may not have a qualifier without a family. %s.", this);
      Preconditions.checkArgument(hasQualifier || !hasVersion,
          "Delete statement may not have a version without a qualifier. %s.", this);
    }

    /** {@inheritDoc} */
    @Override
    public RegularStatement createUnpreparedStatement() {
      final Delete delete = delete().all().from(mTable.getKeyspace(), mTable.getTable());
      for (String column : mEntityIDColumns) {
        delete.where(eq(column, bindMarker()));
      }
      if (mHasFamily) {
        delete.where(eq(CQLUtils.FAMILY_COL, bindMarker()));
        if (mHasQualifier) {
          delete.where(eq(CQLUtils.QUALIFIER_COL, bindMarker()));
          if (mHasVersion) {
            delete.where(eq(CQLUtils.VERSION_COL, bindMarker()));
          }
        }
      }
      return delete;
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
      return Objects.hashCode(
          this.getClass(),
          mTable,
          mHasFamily,
          mHasQualifier,
          mHasVersion);
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      final DeleteStatementKey other = (DeleteStatementKey) obj;
      return Objects.equal(this.mTable, other.mTable)
          && Objects.equal(this.mHasFamily, other.mHasFamily)
          && Objects.equal(this.mHasQualifier, other.mHasQualifier)
          && Objects.equal(this.mHasVersion, other.mHasVersion);
    }
  }
}

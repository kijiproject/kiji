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

package org.kiji.mapreduce.impl;

import java.io.IOException;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.KijiTableContext;
import org.kiji.mapreduce.framework.HFileKeyValue;
import org.kiji.mapreduce.framework.KijiConfKeys;
import org.kiji.schema.EntityId;
import org.kiji.schema.EntityIdFactory;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiCellEncoder;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiURI;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.filter.StripValueColumnFilter;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.impl.DefaultKijiCellEncoderFactory;
import org.kiji.schema.layout.CellSpec;
import org.kiji.schema.layout.HBaseColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout;
import org.kiji.schema.util.ResourceUtils;

/**
 * Kiji context that emits puts for the configured output table to HFiles.
 *
 * This is the recommended way for writing to an HBase table.
 *  <li> This context provides some level of atomicity and isolation
 *       (no partial writes to the table while the M/R job runs, or if the M/R job fails).
 *  <li> Region servers are not hammered but a sustained stream of puts while the M/R job.
 *  <li> After the M/R job completed successfully, the output is committed to the HBase table
 *       using the HFileLoader.
 */
@ApiAudience.Private
public final class HFileWriterContext
    extends InternalKijiContext
    implements KijiTableContext {

  /** NullWritable shortcut. */
  private static final NullWritable NULL = NullWritable.get();

  private final Kiji mKiji;
  private final KijiTable mTable;
  private final KijiTableReader mReader;
  private final HBaseColumnNameTranslator mColumnNameTranslator;
  private final EntityIdFactory mEntityIdFactory;

  /**
   * Constructs a new context that can write cells to an HFile that can be loaded into an HBase
   * table.
   *
   * @param hadoopContext is the Hadoop {@link TaskInputOutputContext} that will be used to perform
   *     the writes.
   * @throws IOException on I/O error.
   */
  public HFileWriterContext(TaskInputOutputContext<?, ?, ?, ?> hadoopContext)
      throws IOException {
    super(hadoopContext);
    final Configuration conf = new Configuration(hadoopContext.getConfiguration());
    final KijiURI outputURI =
        KijiURI.newBuilder(conf.get(KijiConfKeys.KIJI_OUTPUT_TABLE_URI)).build();
    mKiji = Kiji.Factory.open(outputURI, conf);
    mTable = mKiji.openTable(outputURI.getTable());
    mReader = mTable.openTableReader();
    mColumnNameTranslator = HBaseColumnNameTranslator.from(mTable.getLayout());
    mEntityIdFactory = EntityIdFactory.getFactory(mTable.getLayout());
  }

  /**
   * Creates a new context that can write cells to an HFile that can be loaded into an HBase table.
   *
   * @param hadoopContext is the Hadoop {@link TaskInputOutputContext} that will be used to perform
   *    the writes.
   * @return a new context that can write cells to an HFile that can be loaded into an HBase table.
   * @throws IOException if there is an I/O error.
   */
  public static HFileWriterContext create(TaskInputOutputContext<?, ?, ?, ?> hadoopContext)
      throws IOException {
    return new HFileWriterContext(hadoopContext);
  }

  /**
   * Write the given HFileKeyValue to the MR context.
   *
   * <p>The key value is written as the mapper key with a null writable value.</p>
   *
   * @param mrKey The HFileKeyValue to write out.
   * @throws IOException on I/O error or interruption.
   */
  private void write(final HFileKeyValue mrKey) throws IOException {
    try {
      getMapReduceContext().write(mrKey, NULL);
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(EntityId entityId, String family, String qualifier, T value)
      throws IOException {
    put(entityId, family, qualifier, HConstants.LATEST_TIMESTAMP, value);
  }

  /** {@inheritDoc} */
  @SuppressWarnings("unchecked")
  @Override
  public <T> void put(EntityId entityId, String family, String qualifier, long timestamp, T value)
      throws IOException {
    final KijiColumnName kijiColumn = new KijiColumnName(family, qualifier);
    final HBaseColumnName hbaseColumn = mColumnNameTranslator.toHBaseColumnName(kijiColumn);
    final CellSpec cellSpec = mTable.getLayout().getCellSpec(kijiColumn)
        .setSchemaTable(mKiji.getSchemaTable());
    final KijiCellEncoder encoder = DefaultKijiCellEncoderFactory.get().create(cellSpec);
    final HFileKeyValue mrKey = new HFileKeyValue(
        entityId.getHBaseRowKey(),
        hbaseColumn.getFamily(),
        hbaseColumn.getQualifier(),
        timestamp,
        encoder.encode(value));

    write(mrKey);
  }

  /**
   * Deletes an entire row.
   *
   * <p>Note HBase does not represent row deletions with individual (cross-family)
   * tombstones.  Instead, this method issues a family delete for each locality group
   * individually.</p>
   *
   * @param entityId Entity ID of the row to delete.
   * @throws IOException on I/O error.
   */
  @Override
  public void deleteRow(EntityId entityId) throws IOException {
    deleteRow(entityId, HConstants.LATEST_TIMESTAMP);
  }

  /**
   * Delete all cells from a row with a timestamp less than or equal to the specified timestamp.
   *
   * <p>Note HBase does not represent row deletions with individual (cross-family)
   * tombstones.  Instead, this method issues a family delete for each locality group
   * individually.</p>
   *
   * @param entityId Entity ID of the row to delete data from.
   * @param upToTimestamp Delete cells with a timestamp older or equal to this parameter.
   * @throws IOException on I/O error.
   */
  @Override
  public void deleteRow(EntityId entityId, long upToTimestamp) throws IOException {
    for (LocalityGroupLayout localityGroupLayout : mTable.getLayout().getLocalityGroups()) {
      final HFileKeyValue mrKey = new HFileKeyValue(
          entityId.getHBaseRowKey(),
          localityGroupLayout.getId().toByteArray(),
          HConstants.EMPTY_BYTE_ARRAY,
          upToTimestamp,
          HFileKeyValue.Type.DeleteFamily,
          HConstants.EMPTY_BYTE_ARRAY);

      write(mrKey);
    }
  }

  /**
   * Deletes all versions of all cells in a family.
   *
   * <p>When the deleted kiji family is the only one in the enclosing locality group, a
   * single (hbase) family delete is issued.</p>
   *
   * <p>However this won't work when there are other families within the locality group:
   * the delete would affect them too.  In this case, this method instead issues a
   * sequence of column deletes in way depending on the family type.</p>
   *
   * <p>Group-type families are handled by issuing a column delete for each of their
   * columns as declared by their fixed layout.</p>
   *
   * <p>Map-type families are handled by enumerating the existing cells of the family and
   * issuing columns deletes.  This strategy is susceptible to races with writers: a new
   * put occurring after MR job execution but before bulk load will not be deleted.</p>
   *
   * <P>Again, notice this limitation does not apply if the map-type family is the only
   * one in its locality group.  As stated above, in that case a single family delete is
   * issued and no leaks are possible.</p>
   *
   * @param entityId Entity ID of the row to delete data from.
   * @param family Column family.
   * @throws IOException on I/O error.
   */
  @Override
  public void deleteFamily(EntityId entityId, String family) throws IOException {
    deleteFamily(entityId, family, HConstants.LATEST_TIMESTAMP);
  }

  /**
   * Deletes all cells from a family with a timestamp less than or equal to the specified timestamp.
   *
   * <p>When the deleted kiji family is the only one in the enclosing locality group, a
   * single (hbase) family delete is issued.</p>
   *
   * <p>However this won't work when there are other families within the locality group:
   * the delete would affect them too.  In this case, this method instead issues a
   * sequence of column deletes in way depending on the family type.</p>
   *
   * <p>Group-type families are handled by issuing a column delete for each of their
   * columns as declared by their fixed layout.</p>
   *
   * <p>Map-type families are handled by enumerating the existing cells of the family and
   * issuing columns deletes.  This strategy is susceptible to races with writers: a new
   * put occurring after MR job execution but before bulk load will not be deleted.</p>
   *
   * <P>Again, notice this limitation does not apply if the map-type family is the only
   * one in its locality group.  As stated above, in that case a single family delete is
   * issued and no leaks are possible.</p>
   *
   * @param entityId Entity ID of the row to delete data from.
   * @param family Column family.
   * @param upToTimestamp Delete cells with a timestamp older or equal to this parameter.
   * @throws IOException on I/O error.
   */
  @Override
  public void deleteFamily(EntityId entityId, String family, long upToTimestamp)
      throws IOException {

    final FamilyLayout familyLayout = mTable.getLayout().getFamilyMap().get(family);
    if (null == familyLayout) {
      throw new NoSuchColumnException(String.format("Family '%s' not found.", family));
    }

    // Shamelessly stolen from HBaseKijiBufferedWriter#deleteFamily
    if (familyLayout.getLocalityGroup().getFamilyMap().size() > 1) {
      // There are multiple families within the locality group, so we need to be clever.
      if (familyLayout.isGroupType()) {
        deleteGroupFamily(entityId, familyLayout, upToTimestamp);
      } else if (familyLayout.isMapType()) {
        deleteMapFamily(entityId, familyLayout, upToTimestamp);
      } else {
        throw new RuntimeException("Internal error: family is neither map-type nor group-type.");
      }
      return;
    }

    // The only data in this HBase family is the one Kiji family, so we can delete everything.
    final KijiColumnName kijiColumn = new KijiColumnName(family, null);
    final HBaseColumnName hbaseColumn = mColumnNameTranslator.toHBaseColumnName(kijiColumn);
    final HFileKeyValue mrKey = new HFileKeyValue(
        entityId.getHBaseRowKey(),
        hbaseColumn.getFamily(),
        HConstants.EMPTY_BYTE_ARRAY,
        upToTimestamp,
        HFileKeyValue.Type.DeleteFamily,
        HConstants.EMPTY_BYTE_ARRAY);

    write(mrKey);
  }

  /**
   * Deletes all cells from a group-type family with a timestamp less than or equal to a
   * specified timestamp.
   *
   * @param entityId The entity (row) to delete from.
   * @param familyLayout The family layout.
   * @param upToTimestamp A timestamp.
   * @throws IOException If there is an IO error.
   */
  private void deleteGroupFamily(
      EntityId entityId,
      FamilyLayout familyLayout,
      long upToTimestamp)
      throws IOException {
    final String familyName = Preconditions.checkNotNull(familyLayout.getName());
    // Delete each column in the group according to the layout.
    for (ColumnLayout columnLayout : familyLayout.getColumnMap().values()) {
      final String qualifier = columnLayout.getName();
      deleteColumn(entityId, familyName, qualifier, upToTimestamp);
    }
  }

  /**
   * Deletes all cells from a map-type family with a timestamp less than or equal to a
   * specified timestamp.
   *
   * @param entityId The entity (row) to delete from.
   * @param familyLayout A family layout.
   * @param upToTimestamp A timestamp.
   * @throws IOException If there is an IO error.
   */
  private void deleteMapFamily(EntityId entityId, FamilyLayout familyLayout, long upToTimestamp)
      throws IOException {
    // Since multiple Kiji column families are mapped into a single HBase column family,
    // we have to do this delete in two steps:
    //
    // 1. Send a get() to retrieve the names of all HBase qualifiers within the HBase
    //    family that belong to the Kiji column family.
    // 2. Send a delete() for each of the qualifiers found in the previous step.

    // Step 1.
    final String familyName = familyLayout.getName();

    final KijiDataRequestBuilder dataRequestBuilder = KijiDataRequest.builder();
    dataRequestBuilder
      .withTimeRange(0, upToTimestamp)
      .newColumnsDef()
        .withFilter(new StripValueColumnFilter())
        .addFamily(familyName);
    final KijiDataRequest dataRequest = dataRequestBuilder.build();

    final KijiRowData rowData = mReader.get(entityId, dataRequest);

    // Step 2.
    final byte[] hbaseRow = entityId.getHBaseRowKey();

    for (String qualifier : rowData.getQualifiers(familyName)) {
      final KijiColumnName kijiColumn = new KijiColumnName(familyName, qualifier);
      final HBaseColumnName hbaseColumn = mColumnNameTranslator.toHBaseColumnName(kijiColumn);
      final HFileKeyValue mrKey = new HFileKeyValue(
          hbaseRow,
          hbaseColumn.getFamily(),
          hbaseColumn.getQualifier(),
          upToTimestamp,
          HFileKeyValue.Type.DeleteColumn,
          HConstants.EMPTY_BYTE_ARRAY);

      write(mrKey);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void deleteColumn(EntityId entityId, String family, String qualifier) throws IOException {
    deleteColumn(entityId, family, qualifier, HConstants.LATEST_TIMESTAMP);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteColumn(EntityId entityId, String family, String qualifier, long upToTimestamp)
      throws IOException {
    final KijiColumnName kijiColumn = new KijiColumnName(family, qualifier);
    final HBaseColumnName hbaseColumn = mColumnNameTranslator.toHBaseColumnName(kijiColumn);
    final HFileKeyValue mrKey = new HFileKeyValue(
        entityId.getHBaseRowKey(),
        hbaseColumn.getFamily(),
        hbaseColumn.getQualifier(),
        upToTimestamp,
        HFileKeyValue.Type.DeleteColumn,
        HConstants.EMPTY_BYTE_ARRAY);

    write(mrKey);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteCell(EntityId entityId, String family, String qualifier) throws IOException {
    deleteCell(entityId, family, qualifier, HConstants.LATEST_TIMESTAMP);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteCell(EntityId entityId, String family, String qualifier, long timestamp)
      throws IOException {
    final KijiColumnName kijiColumn = new KijiColumnName(family, qualifier);
    final HBaseColumnName hbaseColumn = mColumnNameTranslator.toHBaseColumnName(kijiColumn);
    final HFileKeyValue mrKey = new HFileKeyValue(
        entityId.getHBaseRowKey(),
        hbaseColumn.getFamily(),
        hbaseColumn.getQualifier(),
        timestamp,
        HFileKeyValue.Type.DeleteCell,
        HConstants.EMPTY_BYTE_ARRAY);

    write(mrKey);
  }

  /** {@inheritDoc} */
  @Override
  public EntityIdFactory getEntityIdFactory() {
    return mEntityIdFactory;
  }

  /** {@inheritDoc} */
  @Override
  public EntityId getEntityId(Object... components) {
    return mEntityIdFactory.getEntityId(components);
  }

  @Override
  public void close() throws IOException {
    ResourceUtils.closeOrLog(mReader);
    ResourceUtils.releaseOrLog(mTable);
    ResourceUtils.releaseOrLog(mKiji);
    super.close();
  }
}

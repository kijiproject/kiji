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
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.impl.DefaultKijiCellEncoderFactory;
import org.kiji.schema.layout.CellSpec;
import org.kiji.schema.layout.HBaseColumnNameTranslator;
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
    try {
      getMapReduceContext().write(mrKey, NULL);
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }
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
    ResourceUtils.releaseOrLog(mTable);
    ResourceUtils.releaseOrLog(mKiji);
    super.close();
  }
}

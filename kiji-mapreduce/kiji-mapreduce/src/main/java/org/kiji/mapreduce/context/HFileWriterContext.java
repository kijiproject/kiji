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

package org.kiji.mapreduce.context;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import org.kiji.mapreduce.HFileKeyValue;
import org.kiji.mapreduce.KijiConfKeys;
import org.kiji.mapreduce.KijiTableContext;
import org.kiji.schema.EntityId;
import org.kiji.schema.EntityIdFactory;
import org.kiji.schema.HBaseColumnName;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiCellEncoder;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.impl.DefaultKijiCellEncoderFactory;
import org.kiji.schema.layout.ColumnNameTranslator;
import org.kiji.schema.layout.impl.CellSpec;

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
public class HFileWriterContext
    extends InternalKijiContext
    implements KijiTableContext {

  /** NullWritable shortcut. */
  private static final NullWritable NULL = NullWritable.get();

  private final Kiji mKiji;
  private final KijiTable mTable;
  private final ColumnNameTranslator mColumnNameTranslator;
  private final EntityIdFactory mEntityIdFactory;

  /**
   * Constructs a new table writer context using the specified options.
   *
   * @param hadoopContext Underlying Hadoop context.
   * @throws IOException on I/O error.
   */
  public HFileWriterContext(TaskInputOutputContext<?, ?, ?, ?> hadoopContext)
      throws IOException {
    super(hadoopContext);
    final Configuration conf = new Configuration(hadoopContext.getConfiguration());
    final KijiURI outputURI = KijiURI.parse(conf.get(KijiConfKeys.OUTPUT_KIJI_TABLE_URI));
    mKiji = Kiji.open(outputURI, conf);
    mTable = mKiji.openTable(outputURI.getTable());
    mColumnNameTranslator = new ColumnNameTranslator(mTable.getLayout());
    mEntityIdFactory = mTable.getEntityIdFactory();
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(EntityId entityId, String family, String qualifier, T value)
      throws IOException {
    put(entityId, family, qualifier, HConstants.LATEST_TIMESTAMP, value);
  }

  /** {@inheritDoc} */
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
  public EntityId getEntityId(String key) {
    return mEntityIdFactory.fromKijiRowKey(key);
  }

  @Override
  public void close() throws IOException {
    mTable.close();
    mKiji.close();
    super.close();
  }
}

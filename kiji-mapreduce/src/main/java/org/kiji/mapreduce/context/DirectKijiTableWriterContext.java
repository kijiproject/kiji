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
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import org.kiji.mapreduce.KijiConfKeys;
import org.kiji.mapreduce.KijiTableContext;
import org.kiji.schema.EntityId;
import org.kiji.schema.EntityIdFactory;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiPutter;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.KijiURIException;

/**
 * Kiji context that to write cells to a configured output table.
 *
 * <p> Implemented as direct writes sent to the HTable.
 *
 * <p> Using this table writer context in a MapReduce is strongly discouraged :
 * pushing a lot of data into a running HBase instance may trigger region splits
 * and cause the HBase instance to go offline.
 */
public class DirectKijiTableWriterContext
    extends InternalKijiContext
    implements KijiTableContext {

  private final Kiji mKiji;
  private final KijiTable mTable;
  private final KijiPutter mPutter;
  private final EntityIdFactory mEntityIdFactory;

  /**
   * Constructs a new table writer context using the specified options.
   *
   * @param hadoopContext Underlying Hadoop context.
   * @throws IOException on I/O error.
   */
  public DirectKijiTableWriterContext(TaskInputOutputContext<?, ?, ?, ?> hadoopContext)
      throws IOException {
    super(hadoopContext);
    final Configuration conf = new Configuration(hadoopContext.getConfiguration());
    final KijiURI outputURI = getOutputURI(conf);
    mKiji = Kiji.open(outputURI, conf);
    mTable = mKiji.openTable(outputURI.getTable());
    mPutter = mTable.openTableWriter();
    mEntityIdFactory = mTable.getEntityIdFactory();
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(EntityId entityId, String family, String qualifier, T value)
      throws IOException {
    mPutter.put(entityId, family, qualifier, value);
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(EntityId entityId, String family, String qualifier, long timestamp, T value)
      throws IOException {
    mPutter.put(entityId, family, qualifier, timestamp, value);
  }

  /** {@inheritDoc} */
  @Override
  public EntityId getEntityId(String key) {
    return mEntityIdFactory.fromKijiRowKey(key);
  }

  @Override
  public void flush() throws IOException {
    mPutter.flush();
    super.flush();
  }

  @Override
  public void close() throws IOException {
    mPutter.close();
    mTable.close();
    mKiji.close();
    super.close();
  }

  /**
   * Reports the URI of the configured output table.
   *
   * @param conf Read the output URI from this configuration.
   * @return the configured output URI.
   * @throws IOException on I/O error.
   */
  private static KijiURI getOutputURI(Configuration conf) throws IOException {
    try {
      return KijiURI.parse(conf.get(KijiConfKeys.OUTPUT_KIJI_TABLE_URI));
    } catch (KijiURIException kue) {
      throw new IOException(kue);
    }
  }
}

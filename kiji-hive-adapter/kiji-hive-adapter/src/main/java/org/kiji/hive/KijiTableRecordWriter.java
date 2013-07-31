/**
 * (c) Copyright 2013 WibiData, Inc.
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

package org.kiji.hive;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

import com.google.common.base.Preconditions;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.hive.io.KijiCellWritable;
import org.kiji.hive.io.KijiRowDataWritable;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.tools.ToolUtils;
import org.kiji.schema.util.ResourceUtils;

/**
 * Writes key-value records from a KijiTableInputSplit (usually 1 region in an HTable).
 */
public class KijiTableRecordWriter
    implements FileSinkOperator.RecordWriter {
  private static final Logger LOG = LoggerFactory.getLogger(KijiTableRecordWriter.class);

  private final Kiji mKiji;
  private final KijiTable mKijiTable;
  private final KijiTableWriter mKijiTableWriter;

  /**
   * Constructor.
   *
   * @param conf The job configuration.
   * @throws java.io.IOException If the input split cannot be opened.
   */
  public KijiTableRecordWriter(Configuration conf)
      throws IOException {
    String kijiURIString = conf.get(KijiTableOutputFormat.CONF_KIJI_TABLE_URI);
    KijiURI kijiURI = KijiURI.newBuilder(kijiURIString).build();
    mKiji = Kiji.Factory.open(kijiURI);
    mKijiTable = mKiji.openTable(kijiURI.getTable());
    mKijiTableWriter = mKijiTable.openTableWriter();
  }

  @Override
  public void close(boolean abort) throws IOException {
    ResourceUtils.closeOrLog(mKijiTableWriter);
    ResourceUtils.releaseOrLog(mKijiTable);
    ResourceUtils.releaseOrLog(mKiji);
  }

  @Override
  public void write(Writable writable) throws IOException {
    Preconditions.checkArgument(writable instanceof KijiRowDataWritable,
        "KijiTableRecordWriter can only operate on KijiRowDataWritable objects.");

    KijiRowDataWritable kijiRowDataWritable = (KijiRowDataWritable) writable;
    KijiTableLayout kijiTableLayout = mKijiTable.getLayout();

    // TODO(KIJIHIVE-30) Process EntityId components here as well.
    EntityId entityId = ToolUtils.createEntityIdFromUserInputs(
        kijiRowDataWritable.getEntityId().toShellString(),
        kijiTableLayout);

    Map<KijiColumnName, NavigableMap<Long, KijiCellWritable>> writableData =
        kijiRowDataWritable.getData();
    for (KijiColumnName kijiColumnName: writableData.keySet()) {
      String family = kijiColumnName.getFamily();
      String qualifier = kijiColumnName.getQualifier();

      NavigableMap<Long, KijiCellWritable> timeseries = writableData.get(kijiColumnName);
      // Ignoring the redundant timestamp in this Map in favor of the one contained in the cell.
      for (KijiCellWritable kijiCellWritable : timeseries.values()) {
        Long timestamp = kijiCellWritable.getTimestamp();
        Schema schema = kijiCellWritable.getSchema();
        Preconditions.checkNotNull(schema);
        Object data = kijiCellWritable.getData();
        switch (schema.getType()) {
          case NULL:
            // Don't write null values.
            break;
          case BOOLEAN:
          case INT:
          case LONG:
          case FLOAT:
          case DOUBLE:
          case STRING:
          case BYTES:
          case FIXED:
            // Write the primitive type to Kiji.
            mKijiTableWriter.put(entityId, family, qualifier, timestamp, data);
            break;
          case RECORD:
          case ARRAY:
          case MAP:
          case UNION:
          default:
            // TODO(KIJIHIVE-31): Support the writing of some of these complex types.
            throw new UnsupportedOperationException("Unsupported type: " + schema.getType());
        }
      }
    }
  }
}

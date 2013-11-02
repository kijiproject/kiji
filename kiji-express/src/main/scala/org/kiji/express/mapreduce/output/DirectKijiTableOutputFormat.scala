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

package org.kiji.express.mapreduce.output

import org.apache.avro.Schema
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.OutputFormat
import org.apache.hadoop.mapreduce.RecordWriter
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat
import org.kiji.express.Cell
import org.kiji.express.EntityId
import org.kiji.express.flow.InvalidKijiTapException
import org.kiji.express.flow.WriterSchemaSpec
import org.kiji.express.util.AvroUtil
import org.kiji.express.util.Resources
import org.kiji.express.util.Resources.doAndRelease
import org.kiji.mapreduce.framework.HFileKeyValue
import org.kiji.mapreduce.framework.KijiConfKeys
import org.kiji.schema.EntityIdFactory
import org.kiji.schema.Kiji
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiSchemaTable
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiTableWriter
import org.kiji.schema.KijiURI
import org.kiji.schema.avro.AvroValidationPolicy
import org.kiji.schema.impl.Versions
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.util.ProtocolVersion
import org.kiji.express.flow.QualifiedColumnRequestOutput
import org.kiji.express.flow.QualifiedColumnRequestOutput

/**
 * A cell from a Kiji table containing some datum, addressed by a family, qualifier,
 * and version timestamp.
 *
 * @param family of the Kiji table cell.
 * @param qualifier of the Kiji table cell.
 * @param version  of the Kiji table cell.
 * @param datum in the Kiji table cell.
 * @tparam T is the type of the datum in the cell.
 */
case class HFileCell private[express] (
  entity_id: EntityId,
  col_request: QualifiedColumnRequestOutput,
  timestamp: Long,
  datum: AnyRef)


/**
 * An implementation of a Hadoop OutputFormat that will sink data directly to a Kiji table.
 * Note: This should only be used for demonstration purposes and situations where it's well
 * understood that a small amount of data is going to be written to HBase. This will make a Put
 * request per call which has serious performance implications.
 */
class DirectKijiTableOutputFormat extends NullOutputFormat[HFileCell, NullWritable] {

  /**
   * Implementation of a RecordWriter that does the actual writing of the data to HBase.
   * @param kijiWriter is the table writer.
   * @param layout is the table layout of the table being written to.
   * @param schemaTable is the schema table of the instance being written to.
   */
  class DirectKijiRecordWriter(
      kijiWriter: KijiTableWriter,
      layout: KijiTableLayout,
      schemaTable: KijiSchemaTable) extends RecordWriter[HFileCell, NullWritable] {

    val eidFactory = EntityIdFactory.getFactory(layout)

    val layoutVersion = ProtocolVersion.parse(layout.getDesc.getVersion)
    val validationEnabled = { layoutVersion.compareTo(Versions.LAYOUT_1_3_0) >= 0 }

    override def close(context: TaskAttemptContext) {
      kijiWriter.close()
    }

    override def write(key: HFileCell, value: NullWritable) {
      val jEntityId = key.entity_id.toJavaEntityId(eidFactory)
      val qc = key.col_request
      val schema: Option[Schema] = getSchemaIfPossible(qc)

      kijiWriter.put(
        jEntityId,
        qc.family,
        qc.qualifier,
        key.timestamp,
        AvroUtil.encodeToJava(key.datum, schema))
    }

    /**
     * Gets the schema from the schemaIdOption if it exists, otherwise tries to resolve the default
     * reader schema for the table.  Returns None if neither of those are possible.
     *
     * @param getColumnName() of the column to try to get the schema for.
     * @param schemaSpecOption of the schema to try to resolve.
     * @return a schema to use for writing, if possible.
     */
    def getSchemaIfPossible(qualColumn: QualifiedColumnRequestOutput): Option[Schema] = {
      val colName = qualColumn.getColumnName

      if (qualColumn.useDefaultReaderSchema) {
        Some(layout.getCellSpec(colName).getDefaultReaderSchema)
      } else if (qualColumn.schemaId.isDefined) {
        Some(schemaTable.getSchema(qualColumn.schemaId.get))
      } else if (validationEnabled &&
        layout.getCellSpec(colName).getCellSchema.getAvroValidationPolicy
        != AvroValidationPolicy.SCHEMA_1_0) {
        // is if avro validation policy is schema-1.0 compatibility mode.
        throw new InvalidKijiTapException(
          "Column '%s' must have a schema specified.".format(colName))
      } else
        None
    }
  }

  override def getRecordWriter(context: TaskAttemptContext):
    RecordWriter[HFileCell, NullWritable] = {
    // Open a table writer.

    val config = context.getConfiguration()
    val uriString: String = config.get(KijiConfKeys.KIJI_OUTPUT_TABLE_URI)
    val uri: KijiURI = KijiURI.newBuilder(uriString).build()

    val kiji: Kiji = Kiji.Factory.open(uri, config)

    val (tableWriter, layout) =
      Resources.withKijiTable(uri, config) { table =>
        (table.openTableWriter(), table.getLayout())
      }

    new DirectKijiRecordWriter(tableWriter, layout, kiji.getSchemaTable())
  }
}

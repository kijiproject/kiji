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

package org.kiji.mapreduce.lib.bulkimport;

import java.io.IOException;
import java.text.ParseException;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.KijiTableContext;
import org.kiji.mapreduce.lib.util.CommonLogParser;
import org.kiji.mapreduce.lib.util.CommonLogParser.Field;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiColumnName;

/**
 * Bulk importer that takes lines from an NCSA/Apache common logs and can produce a row for each
 * entry in the log file.  This is the basic log file format used by the Apache HTTP server.
 * This bulk importer uses {@link org.kiji.mapreduce.lib.util.CommonLogParser} for parsing lines
 * into fields.
 *
 * <h2>Creating a bulk import job for NCSA/Apache Common logs:</h2>
 * <p>
 *   The common log bulk importer can be passed into a
 *   {@link org.kiji.mapreduce.bulkimport.KijiBulkImportJobBuilder}.  A
 *   {@link KijiTableImportDescriptor}, which defines the mapping from the import fields to the
 *   destination Kiji columns, must be passed in as part of the job configuration.  For writing
 *   to an HFile which can later be loaded with the <code>kiji bulk-load</code> tool the job
 *   creation looks like:
 * </p>
 * <pre><code>
 *   // Set the import descriptor file to be used for this bulk importer.
 *   conf.set(DescribedInputTextBulkImporter.CONF_FILE, "commonlog-import-descriptor.json");
 *   // Configure and create the MapReduce job.
 *   final MapReduceJob job = KijiBulkImportJobBuilder.create()
 *       .withConf(conf)
 *       .withBulkImporter(CommonLogBulkImporter.class)
 *       .withInput(new TextMapReduceJobInput(new Path(inputFile.toString())))
 *       .withOutput(new HFileMapReduceJobOutput(mOutputTable, hfileDirPath))
 *       .build();
 * </code></pre>
 * <p>
 *   Alternately the bulk importer can be configured to write directly to a Kiji Table.  This is
 *   <em>not recommended</em> because it generates individual puts for each cell that is being
 *   written. For small jobs or tests, a direct Kiji table output job can be created by modifying
 *   out the .withOutput parameter to:
 *   <code>.withOutput(new DirectKijiTableMapReduceJobOutput(mOutputTable))</code>
 * </p>
 *
 * @see KijiTableImportDescriptor
 * @see <a href="http://www.w3.org/Daemon/User/Config/Logging.html#common-logfile-format">
 *      Common logfile format </a>
 */
@ApiAudience.Public
public final class CommonLogBulkImporter extends DescribedInputTextBulkImporter {
  private static final Logger LOG =
      LoggerFactory.getLogger(CommonLogBulkImporter.class);

  /** {@inheritDoc} */
  @Override
  public void produce(Text value, KijiTableContext context) throws IOException {
    Map<Field, String> fieldMap;
    try {
      fieldMap = CommonLogParser.get().parseCommonLog(value.toString());
    } catch (ParseException pe) {
      reject(value, context, "Unable to parse row: " + value.toString());
      return;
    }

    Field entityIdSource = Field.valueOf(getEntityIdSource());
    EntityId eid = context.getEntityId(fieldMap.get(entityIdSource));

    for (KijiColumnName kijiColumnName : getDestinationColumns()) {
      Field source = Field.valueOf(getSource(kijiColumnName));
      String fieldValue = fieldMap.get(source);
      if (fieldValue != null) {
        // TODO(KIJIMRLIB-12) Add some ability to use timestamps derived from the log file.
        context.put(eid,  kijiColumnName.getFamily(), kijiColumnName.getQualifier(), fieldValue);
      } else {
        reject(value, context, "Log file missing field: " + source);
      }
    }
  }
}

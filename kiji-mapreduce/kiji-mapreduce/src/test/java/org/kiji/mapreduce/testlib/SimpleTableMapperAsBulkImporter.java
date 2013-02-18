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

package org.kiji.mapreduce.testlib;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.kiji.mapreduce.KijiTableContext;
import org.kiji.mapreduce.MapReduceJob;
import org.kiji.mapreduce.bulkimport.KijiBulkImportJobBuilder;
import org.kiji.mapreduce.bulkimport.KijiBulkImporter;
import org.kiji.mapreduce.input.KijiTableMapReduceJobInput;
import org.kiji.mapreduce.input.KijiTableMapReduceJobInput.RowOptions;
import org.kiji.mapreduce.output.DirectKijiTableMapReduceJobOutput;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;

/**
 * Example of a «table mapper» implemented as a bulk-importer that reads from a Kiji table.
 *
 * A table mapper reads from a Kiji table and writes to another Kiji table (or possible the same).
 *
 * Requires a custom job builder to configure the input format.
 */
public final class SimpleTableMapperAsBulkImporter
    extends KijiBulkImporter<EntityId, KijiRowData> {

  /** {@inheritDoc} */
  @Override
  public void produce(EntityId eid, KijiRowData row, KijiTableContext context) throws IOException {
    final Long filePos = row.getMostRecentValue("primitives", "long");
    if (filePos != null) {
      context.put(eid, "info", "zip_code", filePos);
    }
  }

  /**
   * Job launcher is required to configure the Kiji table input format.
   *
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    final Configuration conf = HBaseConfiguration.create();
    final KijiURI uri = KijiURI.newBuilder("kiji://.env/test/test").build();
    final Kiji kiji = Kiji.Factory.open(uri, conf);
    final KijiTable table = kiji.openTable(uri.getTable());

    final KijiDataRequest dataRequest = KijiDataRequest.create("primitives");

    final MapReduceJob mrjob = KijiBulkImportJobBuilder.create()
        .withBulkImporter(SimpleTableMapperAsBulkImporter.class)
        .withInput(new KijiTableMapReduceJobInput(table.getURI(), dataRequest, new RowOptions()))
        .withOutput(new DirectKijiTableMapReduceJobOutput(table.getURI()))
        .build();
    if (!mrjob.run()) {
      System.err.println("Job failed");
      System.exit(1);
    } else {
      System.exit(0);
    }
  }
}

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

package org.kiji.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import org.kiji.mapreduce.output.HFileMapReduceJobOutput;
import org.kiji.mapreduce.testlib.SimpleTableMapReducer;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.testutil.AbstractKijiIntegrationTest;

/** Tests running a table map/reducer. */
public class IntegrationTestTableMapReducer extends AbstractKijiIntegrationTest {
  @Test
  public void testTableMapReducer() throws Exception {
    final Configuration conf = createConfiguration();
    final FileSystem fs = FileSystem.get(conf);

    final KijiURI uri = getKijiURI();
    final Kiji kiji = Kiji.Factory.open(uri, conf);
    try {
      final int nregions = 16;
      final KijiTableLayout tableLayout =
          new KijiTableLayout(KijiMRTestLayouts.getTestLayout(), null);
      final String tableName = tableLayout.getName();
      kiji.createTable(tableName, tableLayout, nregions);

      final KijiTable table = kiji.openTable(tableName);
      try {
        {
          final KijiTableWriter writer = table.openTableWriter();
          for (int i = 0; i < 10; ++i) {
            writer.put(table.getEntityId("row-" + i), "primitives", "int", i % 3);
          }
          writer.close();
        }

        final Path output = new Path(fs.getUri().toString(), "/table-mr-output");

        final MapReduceJob mrjob = KijiGatherJobBuilder.create()
            .withConf(conf)
            .withGatherer(SimpleTableMapReducer.TableMapper.class)
            .withReducer(SimpleTableMapReducer.TableReducer.class)
            .withInputTable(table)
            .withOutput(new HFileMapReduceJobOutput(table, output, 16))
            .build();
        if (!mrjob.run()) {
          Assert.fail("Map/Reduce job failed");
        }

      } finally {
        table.close();
      }

    } finally {
      kiji.release();
    }
  }

}

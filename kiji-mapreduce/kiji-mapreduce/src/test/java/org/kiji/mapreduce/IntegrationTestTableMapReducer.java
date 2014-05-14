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

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import org.kiji.mapreduce.gather.KijiGatherJobBuilder;
import org.kiji.mapreduce.output.MapReduceJobOutputs;
import org.kiji.mapreduce.testlib.SimpleTableMapReducer;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.testutil.AbstractKijiIntegrationTest;

/** Tests running a table map/reducer. */
public class IntegrationTestTableMapReducer extends AbstractKijiIntegrationTest {
  @Test
  public void testTableMapReducer() throws Exception {
    final Configuration conf = createConfiguration();
    final FileSystem fs = FileSystem.get(conf);
    // NOTE: fs should get closed, but because of a bug with FileSystem that causes it to close
    // other thread's filesystem objects we do not. For more information
    // see: https://issues.apache.org/jira/browse/HADOOP-7973

    final KijiURI uri = getKijiURI();
    final Kiji kiji = Kiji.Factory.open(uri, conf);
    try {
      final int nregions = 16;
      final TableLayoutDesc layout = KijiMRTestLayouts.getTestLayout();
      final String tableName = layout.getName();
      kiji.createTable(layout, nregions);

      final KijiTable table = kiji.openTable(tableName);
      try {
        final KijiTableWriter writer = table.openTableWriter();
        try {
          for (int i = 0; i < 10; ++i) {
            writer.put(table.getEntityId("row-" + i), "primitives", "int", i % 3);
          }
        } finally  {
          writer.close();
        }

        final Path output = new Path(fs.getUri().toString(),
            String.format("/%s-%s-%d/table-mr-output",
                getClass().getName(), mTestName.getMethodName(), System.currentTimeMillis()));

        final KijiMapReduceJob mrjob = KijiGatherJobBuilder.create()
            .withConf(conf)
            .withGatherer(SimpleTableMapReducer.TableMapper.class)
            .withReducer(SimpleTableMapReducer.TableReducer.class)
            .withInputTable(table.getURI())
            .withOutput(MapReduceJobOutputs.newHFileMapReduceJobOutput(table.getURI(), output, 16))
            .build();
        assertTrue(mrjob.run());
      } finally {
        table.release();
      }
    } finally {
      kiji.release();
    }
  }
}

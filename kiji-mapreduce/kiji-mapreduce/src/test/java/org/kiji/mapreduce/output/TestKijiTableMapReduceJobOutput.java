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

package org.kiji.mapreduce.output;

import static org.junit.Assert.assertFalse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiTable;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;

public class TestKijiTableMapReduceJobOutput extends KijiClientTest {

  /** Test that mapper speculative execution is disabled for KijiTableMapReduceJobOutput. */
  @Test
  public void testSpecExDisabled() throws Exception {
    final Kiji kiji = getKiji();
    final KijiTableLayout layout = KijiTableLayout.createUpdatedLayout(
        KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE),  null);
    kiji.createTable("table", layout);
    final KijiTable table = kiji.openTable("table");

    final Job job = new Job();
    new DirectKijiTableMapReduceJobOutput(table).configure(job);

    final Configuration conf = job.getConfiguration();
    boolean isMapSpecExEnabled = conf.getBoolean("mapred.map.tasks.speculative.execution", true);
    assertFalse(isMapSpecExEnabled);
  }
}

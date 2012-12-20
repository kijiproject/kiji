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

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertFalse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTable;
import org.kiji.schema.impl.HBaseKijiTable;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;

public class TestKijiTableMapReduceJobOutput {

  /** Test that mapper speculative execution is disabled for KijiTableMapReduceJobOutput. */
  @Test
  public void testSpecExDisabled() throws Exception {
    Kiji kiji = createMock(Kiji.class);
    KijiTable table = createMock(HBaseKijiTable.class);
    final KijiTableLayout layout =
        new KijiTableLayout(KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE), null);
    expect(table.getName()).andReturn("foo").anyTimes();
    expect(table.getKiji()).andReturn(kiji).anyTimes();
    expect(table.getLayout()).andReturn(layout).anyTimes();
    expect(kiji.getName()).andReturn("").anyTimes();

    replay(table);
    replay(kiji);

    KijiTableMapReduceJobOutput jobOut = new KijiTableMapReduceJobOutput(table);
    Job job = new Job();
    jobOut.configure(job);

    verify(table);
    verify(kiji);

    Configuration conf = job.getConfiguration();
    boolean isMapSpecExEnabled = conf.getBoolean("mapred.map.tasks.speculative.execution", true);
    assertFalse(isMapSpecExEnabled);
  }
}

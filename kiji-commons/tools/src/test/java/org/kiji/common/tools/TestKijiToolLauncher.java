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

package org.kiji.common.tools;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

public final class TestKijiToolLauncher {
  /**
   * Tests that we can lookup a tool where we expect.
   */
  @Test
  public void testGetToolName() {
    final KijiTool tool = new KijiToolLauncher().getToolForName("test-tool");
    Assert.assertNotNull("Got a null tool back, should have gotten a real one.", tool);
    Assert.assertEquals(
        "Tool does not advertise itself as responding to 'test-tool'.",
        "test-tool",
        tool.getName()
    );
    Assert.assertTrue("Didn't get the TestTool we expected!", tool instanceof TestTool);
  }

  @Test
  public void testGetMissingTool() {
    KijiTool tool = new KijiToolLauncher().getToolForName("this-tool/can't1`exist");
    Assert.assertNull(tool);
  }

  @Test
  public void testSetsOptionsParsedFlag() throws Exception {
    final Configuration conf = new Configuration();

    Assert.assertFalse(conf.getBoolean("mapred.used.genericoptionsparser", false));
    Assert.assertFalse(conf.getBoolean("mapreduce.client.genericoptionsparser.used", false));

    final KijiToolLauncher launcher = new KijiToolLauncher();
    final KijiTool tool = launcher.getToolForName("test-tool");
    launcher.run(tool, new String[0], conf);

    // Make sure Hadoop 1.x and Hadoop 2.x flags are both enabled by the launcher.
    Assert.assertTrue(conf.getBoolean("mapred.used.genericoptionsparser", false));
    Assert.assertTrue(conf.getBoolean("mapreduce.client.genericoptionsparser.used", false));
  }
}

/**
 * (c) Copyright 2014 WibiData, Inc.
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

package org.kiji.maven.plugins;

import java.io.File;
import java.util.UUID;

import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 * Run this integration test against the bento-maven-plugin to demonstrate that the plugin works.
 * // TODO: Test access HBase, etc.
 */
public class TestMojos {
  @Test
  public void testCluster() throws Exception {
    final File configDir = Files.createTempDir();
    final File venvDir = Files.createTempDir();
    final String bentoName = String.format("test-bento-%s", UUID.randomUUID().toString());

    // Start the bento cluster.
    new StartMojo(
        BentoCluster.getDockerAddress(),
        false,
        configDir,
        bentoName,
        venvDir,
        false,
        false,
        null,
        "http://pypi.wibidata.net/simple",
        null,
        null
    ).execute();
    try {
      // Read hadoop configuration from the command line.
      final Configuration conf = BentoTestUtils.bentoConfiguration(configDir);

      BentoTestUtils.validateHdfs(conf);
      BentoTestUtils.validateZookeeper(bentoName);
    } finally {
      new StopMojo(
          BentoCluster.getDockerAddress(),
          false,
          bentoName,
          venvDir,
          false,
          false,
          null,
          null
      ).execute();
    }
  }
}

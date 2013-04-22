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

package org.kiji.mapreduce.platform;

import java.util.Map;

import org.kiji.annotations.ApiAudience;
import org.kiji.delegation.Priority;

/**
 * Factory for CDH4-specific KijiMRPlatformBridge implementation.
 */
@ApiAudience.Private
public final class CDH4MR1KijiMRBridgeFactory extends KijiMRPlatformBridgeFactory {

  /** {@inheritDoc} */
  @Override
  @SuppressWarnings("unchecked")
  public KijiMRPlatformBridge getBridge() {
    try {
      Class<? extends KijiMRPlatformBridge> bridgeClass =
          (Class<? extends KijiMRPlatformBridge>) Class.forName(
              "org.kiji.mapreduce.platform.CDH4MR1KijiMRBridge");
      return bridgeClass.newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Could not instantiate platform bridge", e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public int getPriority(Map<String, String> runtimeHints) {
    String hadoopVer = org.apache.hadoop.util.VersionInfo.getVersion();

    if (hadoopVer.matches("2\\..*-cdh4\\..*")) {
      // Hadoop 2.x-cdh4.* matches correctly; use this bridge.
      return Priority.NORMAL;
    } else {
      // Can't provide for this implementation.
      return Priority.DISABLED;
    }
  }
}


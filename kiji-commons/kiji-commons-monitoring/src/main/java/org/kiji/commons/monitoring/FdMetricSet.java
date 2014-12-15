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

package org.kiji.commons.monitoring;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.Map;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.google.common.collect.ImmutableMap;
import com.sun.management.UnixOperatingSystemMXBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A metric set which provides metrics on JVM file descriptor usage.
 */
public final class FdMetricSet implements MetricSet {
  private static final Logger LOG = LoggerFactory.getLogger(FdMetricSet.class);

  /**
   * Construct a new file descriptor metric set.
   */
  private FdMetricSet() { }

  /**
   * Create a new file descriptor metric set.
   *
   * @return A new CPU metric set.
   */
  public static FdMetricSet create() {
    return new FdMetricSet();
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, Metric> getMetrics() {
    final ImmutableMap.Builder<String, Metric> metrics = ImmutableMap.builder();

    final OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();

    if (os instanceof UnixOperatingSystemMXBean) {
      final UnixOperatingSystemMXBean unix = (UnixOperatingSystemMXBean) os;

      metrics.put("open.count", new Gauge<Long>() {
            @Override
            public Long getValue() {
              return unix.getOpenFileDescriptorCount();
            }
          });

      metrics.put("max.count", new Gauge<Long>() {
            @Override
            public Long getValue() {
              return unix.getMaxFileDescriptorCount();
            }
          });

    } else {
      LOG.info("Unable to create file descriptor metric set for operating system type {}.",
          os.getName());
    }

    return metrics.build();
  }
}

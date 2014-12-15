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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A metric set which provides metrics on JVM and system CPU usage.
 */
public final class CpuMetricSet implements MetricSet {
  private static final Logger LOG = LoggerFactory.getLogger(CpuMetricSet.class);

  /**
   * Construct a new CPU metric set.
   */
  private CpuMetricSet() { }

  /**
   * Create a new CPU metric set.
   *
   * @return A new CPU metric set.
   */
  public static CpuMetricSet create() {
    return new CpuMetricSet();
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, Metric> getMetrics() {
    final ImmutableMap.Builder<String, Metric> metrics = ImmutableMap.builder();

    final OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();

    if (osBean instanceof com.sun.management.OperatingSystemMXBean) {
      final com.sun.management.OperatingSystemMXBean os =
          (com.sun.management.OperatingSystemMXBean) osBean;

      metrics.put("jvm.cpu.process.load", new Gauge<Double>() {
            @Override
            public Double getValue() {
              return os.getProcessCpuLoad();
            }
          });

      metrics.put("jvm.cpu.system.load", new Gauge<Double>() {
            @Override
            public Double getValue() {
              return os.getSystemCpuLoad();
            }
          });

      metrics.put("jvm.cpu.process.time", new Gauge<Long>() {
            @Override
            public Long getValue() {
              return os.getProcessCpuTime();
            }
          });

    } else {
      LOG.info("Unable to create CPU metric set for JVM type {}.",
          ManagementFactory.getRuntimeMXBean().getVmName());
    }
    return metrics.build();
  }
}

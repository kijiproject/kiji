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

import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A metric set which provides metrics on {@link ThreadPoolExecutor} usage.
 */
public final class ExecutorMetricSet implements MetricSet {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutorMetricSet.class);

  private final ThreadPoolExecutor mExecutor;

  /**
   * Construct a thread pool executor metric set.
   *
   * @param executor The executor to instrument.
   */
  private ExecutorMetricSet(final ThreadPoolExecutor executor) {
    mExecutor = executor;
  }

  /**
   * Create a new thread pool executor metric set.
   *
   * @param executor The executor to instrument.
   * @return A new CPU metric set.
   */
  public static ExecutorMetricSet create(
      final ThreadPoolExecutor executor
  ) {
    return new ExecutorMetricSet(executor);
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, Metric> getMetrics() {
    final ImmutableMap.Builder<String, Metric> metrics = ImmutableMap.builder();

    metrics.put("task.count", new Gauge<Long>() {
          @Override
          public Long getValue() {
            return mExecutor.getTaskCount();
          }
        });

    metrics.put("task.active.count", new Gauge<Integer>() {
          @Override
          public Integer getValue() {
            return mExecutor.getActiveCount();
          }
        });

    metrics.put("task.completed.count", new Gauge<Long>() {
          @Override
          public Long getValue() {
            return mExecutor.getCompletedTaskCount();
          }
        });

    metrics.put("task.queue.count", new Gauge<Integer>() {
          @Override
          public Integer getValue() {
            return mExecutor.getQueue().size();
          }
        });

    metrics.put("thread.count", new Gauge<Integer>() {
          @Override
          public Integer getValue() {
            return mExecutor.getPoolSize();
          }
        });

    metrics.put("thread.core.count", new Gauge<Integer>() {
          @Override
          public Integer getValue() {
            return mExecutor.getCorePoolSize();
          }
        });

    metrics.put("thread.max.count", new Gauge<Integer>() {
          @Override
          public Integer getValue() {
            return mExecutor.getMaximumPoolSize();
          }
        });

    return metrics.build();
  }
}

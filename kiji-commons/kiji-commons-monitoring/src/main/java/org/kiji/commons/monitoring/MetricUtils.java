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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map.Entry;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import com.codahale.metrics.riemann.Riemann;
import com.codahale.metrics.riemann.RiemannReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.commons.SocketAddressUtils;

/**
 * Provides utility methods for working with the Dropwizard Metrics library.
 */
public final class MetricUtils {
  private static final Logger LOG = LoggerFactory.getLogger(MetricUtils.class);

  /**
   * Create a new metric registry pre-registered with JVM metrics.
   *
   * @return A new metric registry with pre-registered JVM metrics.
   */
  public static MetricRegistry createMetricRegistry() {
    final MetricRegistry registry = new MetricRegistry();
    registerAll(registry, "jvm.gc", new GarbageCollectorMetricSet());
    registerAll(registry, "jvm.mem", new MemoryUsageGaugeSet());
    registerAll(registry, "jvm.thread", new ThreadStatesGaugeSet());
    registerAll(registry, "jvm.cpu", CpuMetricSet.create());
    registerAll(registry, "jvm.fd", FdMetricSet.create());
    return registry;
  }

  /**
   * Create a new {@link Histogram} backed by the latency utils package. This histogram should
   * not be used with a registry that has more than a single scheduled reporter.
   *
   * @return A new Histogram backed by the latency utils package.
   */
  public static Histogram createLatencyUtilsHistogram() {
    return new Histogram(LatencyUtilsReservoir.create());
  }

  /**
   * Register a Riemann reporter with the provided metrics registry.
   *
   * @param riemannAddress The address of the Riemann service.
   * @param registry The metric registry to report to Riemann.
   * @param localAddress The address of the local process host.
   * @param prefix A prefix to add to reported metrics.
   * @param intervalPeriod The update period.
   * @param intervalUnit The unit of time of the update period.
   * @return The reporter.
   * @throws IOException On unrecoverable I/O exception.
   */
  public static RiemannReporter registerRiemannMetricReporter(
      final InetSocketAddress riemannAddress,
      final MetricRegistry registry,
      final InetSocketAddress localAddress,
      final String prefix,
      final long intervalPeriod,
      final TimeUnit intervalUnit
  ) throws IOException {
    final InetSocketAddress publicAddress = SocketAddressUtils.localToPublic(localAddress);

    // Get the hostname of this machine. Graphite uses '.' as a separator, so all instances in the
    // hostname are replaced with '_'.
    final String localhost =
        publicAddress.getHostName().replace('.', '_') + ":" + publicAddress.getPort();

    final RiemannReporter reporter = RiemannReporter
        .forRegistry(registry)
        .localHost(localhost)
        .prefixedWith(prefix)
        .withTtl(60.0f)
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .useSeparator(".")
        .build(new Riemann(riemannAddress.getHostString(), riemannAddress.getPort()));
    reporter.start(intervalPeriod, intervalUnit);
    return reporter;
  }

  /**
   * Register a console reporter with the provided metrics registry.
   *
   * @param registry The metric registry to report to the console.
   * @param intervalPeriod The update period.
   * @param intervalUnit The unit of time of the update period.
   * @return The reporter.
   */
  public static ConsoleReporter registerConsoleReporter(
      final MetricRegistry registry,
      final long intervalPeriod,
      final TimeUnit intervalUnit
  ) {
    final ConsoleReporter reporter = ConsoleReporter
        .forRegistry(registry)
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .build();
    reporter.start(intervalPeriod, intervalUnit);
    return reporter;
  }

  /**
   * Add metrics to the given registry that track the given executor's thread and task counts.
   *
   * @param executor The executor to instrument.
   * @param registry The metric registry.
   * @param prefix The metric name prefix.
   */
  public static void instrumentExecutor(
      final ThreadPoolExecutor executor,
      final MetricRegistry registry,
      final String prefix
  ) {
    registerAll(registry, prefix, ExecutorMetricSet.create(executor));
  }

  /**
   * Register the given metric set with the registry, with a prefix added to the name of each
   * metric. {@link MetricRegistry} has this method, but it is private.
   *
   * @param registry The registry.
   * @param prefix The prefix to add to each metric name.
   * @param metrics The metrics.
   */
  public static void registerAll(
      final MetricRegistry registry,
      final String prefix,
      final MetricSet metrics
  ) {
    for (final Entry<String, Metric> entry : metrics.getMetrics().entrySet()) {
      if (entry.getValue() instanceof MetricSet) {
        registerAll(registry, MetricRegistry.name(prefix, entry.getKey()), metrics);
      } else {
        registry.register(MetricRegistry.name(prefix, entry.getKey()), entry.getValue());
      }
    }
  }

  /** Private constructor for utility class. */
  private MetricUtils() {
  }
}

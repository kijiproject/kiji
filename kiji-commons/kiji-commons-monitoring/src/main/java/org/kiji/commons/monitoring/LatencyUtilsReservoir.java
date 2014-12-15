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

import java.io.OutputStream;
import java.io.PrintStream;

import javax.annotation.concurrent.ThreadSafe;

import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import org.HdrHistogram.Histogram;
import org.LatencyUtils.LatencyStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Reservoir} which utilizes {@link LatencyStats} for tracking latencies.
 *
 * This reservoir will reset every time a snapshot is taken, so it should not be used with multiple
 * scheduled reporters.
 */
@ThreadSafe
public final class LatencyUtilsReservoir implements Reservoir {
  private static final Logger LOG = LoggerFactory.getLogger(LatencyUtilsReservoir.class);

  private final LatencyStats mLatencyStats;

  /**
   * Construct a new latency utils reservoir.
   *
   * @param latencyStats The LatencyStats backing the reservoir.
   */
  private LatencyUtilsReservoir(final LatencyStats latencyStats) {
    mLatencyStats = latencyStats;
  }

  /**
   * Create a new latency utils reservoir.
   *
   * @param latencyStats The LatencyStats backing the reservoir.
   * @return a new latency utils reservoir.
   */
  public static LatencyUtilsReservoir create(final LatencyStats latencyStats) {
    return new LatencyUtilsReservoir(latencyStats);
  }

  /**
   * Create a new latency utils reservoir with the default settings.
   *
   * @return a new latency utils reservoir.
   */
  public static LatencyUtilsReservoir create() {
    return new LatencyUtilsReservoir(new LatencyStats());
  }

  /**
   * Throws {@link UnsupportedOperationException}.  This method is not currently used by the
   * metrics library, and would be difficult to implement for LatencyUtils.
   *
   * @return Nothing.
   */
  @Override
  public int size() {
    throw new UnsupportedOperationException();
  }

  /** {@inheritDoc} */
  @Override
  public void update(final long value) {
    mLatencyStats.recordLatency(value);
  }

  /** {@inheritDoc} */
  @Override
  public Snapshot getSnapshot() {
    return new HistogramSnapshot(mLatencyStats.getIntervalHistogram());
  }

  /**
   * An implementation of {@link Snapshot} for {@link LatencyStats}.
   */
  private static final class HistogramSnapshot extends Snapshot {
    private final Histogram mHistogram;

    /**
     * Create a new latency utils reservoir snapshot.
     *
     * @param histogram The histogram backing the snapshot.
     */
    public HistogramSnapshot(final Histogram histogram) {
      mHistogram = histogram;
    }

    /** {@inheritDoc} */
    @Override
    public double getValue(final double quantile) {
      try {
        return mHistogram.getValueAtPercentile(quantile * 100);
      } catch (IndexOutOfBoundsException e) {
        return Double.NaN;
      }
    }

    /**
     * Throws {@link UnsupportedOperationException}.  This method is not currently used by the
     * metrics library, and would be difficult to implement for LatencyUtils.
     *
     * @return Nothing.
     */
    @Override
    public long[] getValues() {
      throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public int size() {
      return (int) mHistogram.getTotalCount();
    }

    /** {@inheritDoc} */
    @Override
    public long getMax() {
      return mHistogram.getMaxValue();
    }

    /** {@inheritDoc} */
    @Override
    public double getMean() {
      return mHistogram.getMean();
    }

    /** {@inheritDoc} */
    @Override
    public long getMin() {
      return mHistogram.getMinValue();
    }

    /** {@inheritDoc} */
    @Override
    public double getStdDev() {
      return mHistogram.getStdDeviation();
    }

    /** {@inheritDoc} */
    @Override
    public void dump(final OutputStream output) {
      mHistogram.outputPercentileDistribution(new PrintStream(output), 1.0);
    }
  }
}

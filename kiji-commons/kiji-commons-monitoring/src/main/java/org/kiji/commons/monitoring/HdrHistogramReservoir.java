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

import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import org.HdrHistogram.AtomicHistogram;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.WriterReaderPhaser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.commons.monitoring.LatencyUtilsReservoir.HistogramSnapshot;

/**
 * A {@link Reservoir} which utilizes an HDR {@link Histogram} for tracking latencies.
 *
 * This reservoir will reset every time a snapshot is taken, so it should not be used with multiple
 * scheduled reporters.
 *
 * The implementation is heavily inspired by {@link org.LatencyUtils.LatencyStats}, and allows for
 * wait free recording of latencies.
 */
public final class HdrHistogramReservoir implements Reservoir {
  private static final Logger LOG = LoggerFactory.getLogger(HdrHistogramReservoir.class);

  private volatile AtomicHistogram mActiveHistogram;
  private volatile AtomicHistogram mInactiveHistogram;
  private final WriterReaderPhaser mRecordingPhaser;

  /**
   * Constructs a new HDR histogram based reservoir.
   *
   * @param lowestDiscernableValue Smallest recorded value discernible from 0.
   * @param highestTrackableValue Highest trackable value.
   * @param numberOfSignificantValueDigits Number of significant decimal digits.
   */
  private HdrHistogramReservoir(
      final long lowestDiscernableValue,
      final long highestTrackableValue,
      final int numberOfSignificantValueDigits
  ) {
    mActiveHistogram =
        new AtomicHistogram(
            lowestDiscernableValue,
            highestTrackableValue,
            numberOfSignificantValueDigits);
    mInactiveHistogram =
        new AtomicHistogram(
            lowestDiscernableValue,
            highestTrackableValue,
            numberOfSignificantValueDigits);

    mRecordingPhaser = new WriterReaderPhaser();
  }

  /**
   * Creates a new HDR histogram based reservoir.
   *
   * @param lowestDiscernableValue Smallest recorded value discernible from 0.
   * @param highestTrackableValue Highest trackable value.
   * @param numberOfSignificantValueDigits Number of significant decimal digits.
   * @return A reservoir backed by an HDR histogram.
   */
  public static HdrHistogramReservoir create(
      final long lowestDiscernableValue,
      final long highestTrackableValue,
      final int numberOfSignificantValueDigits
  ) {
    return new HdrHistogramReservoir(
        lowestDiscernableValue,
        highestTrackableValue,
        numberOfSignificantValueDigits);
  }

  /**
   * Throws {@link UnsupportedOperationException}.  This method is not used by the metrics library,
   * and would be difficult to implement for HDR histogram.
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
    long criticalValueAtEnter = mRecordingPhaser.writerCriticalSectionEnter();
    try {
      mActiveHistogram.recordValue(value);
    } finally {
      mRecordingPhaser.writerCriticalSectionExit(criticalValueAtEnter);
    }
  }

  /** {@inheritDoc} */
  @Override
  public synchronized Snapshot getSnapshot() {
    // WriterReaderPhaser has documentation on what is happening here.
    try {
      mRecordingPhaser.readerLock();
      mInactiveHistogram.reset();

      // Swap the histograms
      final AtomicHistogram temp = mInactiveHistogram;
      mInactiveHistogram = mActiveHistogram;
      mActiveHistogram = temp;

      final long now = System.currentTimeMillis();
      mActiveHistogram.setStartTimeStamp(now);
      mInactiveHistogram.setEndTimeStamp(now);

      mRecordingPhaser.flipPhase();

      final Histogram histogram = new Histogram(
          mInactiveHistogram.getLowestDiscernibleValue(),
          mInactiveHistogram.getHighestTrackableValue(),
          mInactiveHistogram.getNumberOfSignificantValueDigits());

      mInactiveHistogram.copyInto(histogram);

      return new HistogramSnapshot(histogram);
    } finally {
      mRecordingPhaser.readerUnlock();
    }
  };
}

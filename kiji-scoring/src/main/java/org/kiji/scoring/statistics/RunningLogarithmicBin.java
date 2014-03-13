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

package org.kiji.scoring.statistics;

import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Objects;
import com.google.common.collect.Maps;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;

/** Bins positive values by order of magnitude. */
@ApiAudience.Public
@ApiStability.Experimental
public final class RunningLogarithmicBin {
  private final NavigableMap<Long, AtomicLong> mBins;

  /**
   * Create the initial empty bins.
   *
   * @param binCount the number of bins in this RunningLogarithmicBin.
   */
  private void populateBins(
      final int binCount
  ) {
    mBins.put(0L, new AtomicLong(0));
    for (int i = 0; i < binCount; i++) {
      mBins.put((long) Math.pow(10.0, -1 + i), new AtomicLong(0));
    }
  }

  /**
   * Initialize a new RunningLogarithmicBin of a given size.
   *
   * @param binCount the desired number of bins in this RunningLogarithmicBin.
   */
  public RunningLogarithmicBin(
      final int binCount
  ) {
    mBins = Maps.newTreeMap();
    populateBins(binCount);
  }

  /**
   * Increment the counter for the bin into which the given value falls.
   *
   * @param value the value to bin.
   */
  public void addValue(
      final long value
  ) {
    mBins.floorEntry(value).getValue().incrementAndGet();
  }

  /**
   * Get the number of values in the bin into which the given value falls.
   *
   * @param value the value from which to derive a bin.
   * @return the number of values in the bin into which the given value falls.
   */
  public long getCount(
      final long value
  ) {
    return mBins.floorEntry(value).getValue().get();
  }

  /**
   * Get the entire bin structure.  Modifications made to the return value will not be reflected in
   * the underlying Map.
   *
   * @return a copy of the entire bin structure.
   */
  public NavigableMap<Long, Long> getBins() {
    final Map<Long, AtomicLong> binsCopy = Maps.newHashMap(mBins);
    final NavigableMap<Long, Long> output = Maps.newTreeMap();
    for (Map.Entry<Long, AtomicLong> entry : binsCopy.entrySet()) {
      output.put(entry.getKey(), entry.getValue().get());
    }
    return output;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("bins", getBins())
        .toString();
  }
}

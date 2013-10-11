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

import java.math.BigInteger;

import com.google.common.base.Objects;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;

/** Calculates the mean of a growing set of values. */
@ApiAudience.Public
@ApiStability.Experimental
public final class RunningMean {
  private long mCount = 0;
  private BigInteger mTotal = BigInteger.ZERO;

  /**
   * Add a new value to this running mean.
   *
   * @param value the value to add.
   */
  public void addValue(
      final long value
  ) {
    mCount++;
    mTotal = mTotal.add(BigInteger.valueOf(value));
  }

  /**
   * Get the number of values which have been averaged by this running mean.
   *
   * @return the number of values which have been averaged by this running mean.
   */
  public long getCount() {
    return mCount;
  }

  /**
   * Get the current value of the mean.
   *
   * @return the current value of the mean.
   */
  public double getMean() {
    return mTotal.divide(BigInteger.valueOf(mCount)).doubleValue();
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("count", mCount)
        .add("mean", getMean())
        .toString();
  }
}

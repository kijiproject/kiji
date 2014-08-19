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

package org.kiji.express.flow.histogram

import com.google.common.base.Preconditions

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability

/**
 * Sets up an equal-width binning configuration. The width of bins will be constant.
 * <br>
 * Warning: This binner is subject to double-precision error and may bin incorrectly near bin
 * boundaries.
 *
 * @param mBinStart is the upper bound (exclusive) of the first bin.
 * @param mBinSize is the width of each bin.
 * @param mBinCount is the total number of bins to create.
 */
@ApiAudience.Framework
@ApiStability.Experimental
final class EqualWidthBinner private(
    private val mBinStart: Double,
    private val mBinSize: Double,
    private val mBinCount: Int
) extends HistogramBinner {
  override def binValue(value: Double): Int = {
    val rawBinId: Int = math.floor((value - mBinStart) / mBinSize).toInt + 1
    math.min(mBinCount + 1, math.max(rawBinId, 0))
  }

  override def binBoundary(binId: Int): Double = {
    Preconditions.checkState(
        binId >= 0,
        "Expected binId to be larger than 0: %s",
        binId: java.lang.Integer
    )

    if (binId == 0) {
      Double.NegativeInfinity
    } else if (binId > (mBinCount + 1)) {
      Double.PositiveInfinity
    } else {
      (binId - 1) * mBinSize + mBinStart
    }
  }
}

/**
 * Companion object providing factory methods.
 */
@ApiAudience.Framework
@ApiStability.Experimental
object EqualWidthBinner {
  /**
   * Sets up an equal-width binning configuration. The width of bins will be constant.
   * <br>
   * Warning: This binner is subject to double-precision error and may bin incorrectly near bin
   * boundaries.
   *
   * @param binStart is the upper bound (exclusive) of the first bin.
   * @param binSize is the width of each bin.
   * @param binCount is the total number of bins to create.
   * @return a binner producing equal-width bins.
   */
  def apply(
      binStart: Double,
      binSize: Double,
      binCount: Int
  ): EqualWidthBinner = {
    new EqualWidthBinner(binStart, binSize, binCount)
  }
}

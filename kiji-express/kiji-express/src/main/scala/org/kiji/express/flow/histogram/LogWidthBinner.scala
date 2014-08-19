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
 * Sets up a log-width binning configuration. The width of bins will increase exponentially with
 * increasing bin IDs.
 * <br>
 * Warning: This binner is subject to double-precision error and may bin incorrectly near bin
 * boundaries.
 *
 * @param mLogBase is the exponential base to expand the width of bins with.
 * @param mStartingPower is the starting power to raise the exponential base to.
 * @param mPowerStepSize is the amount to increase the power the exponential base is raised to for
 *     each bin.
 */
@ApiAudience.Framework
@ApiStability.Experimental
final class LogWidthBinner private(
    private val mLogBase: Double,
    private val mStartingPower: Double,
    private val mPowerStepSize: Double
) extends HistogramBinner {
  // Only need to calculate this once.
  private val mLogDenominator: Double = math.log(mLogBase)

  override def binValue(value: Double): Int = {
    Preconditions.checkState(
        value > 0.0,
        "Expected value to be larger than 0: %s",
        value: java.lang.Double
    )

    math.floor((math.log(value) / mLogDenominator - mStartingPower) / mPowerStepSize).toInt + 1
  }

  override def binBoundary(binId: Int): Double = {
    Preconditions.checkState(
        binId >= 0,
        "Expected binId to be larger than 0: %s",
        binId: java.lang.Integer
    )

    if (binId == 0) {
      0.0
    } else {
      math.pow(mLogBase, (binId - 1) * mPowerStepSize + mStartingPower)
    }
  }
}

/**
 * Companion object providing factory methods.
 */
@ApiAudience.Framework
@ApiStability.Experimental
object LogWidthBinner {
  /**
   * Sets up a log-width binning configuration. The width of bins will increase exponentially with
   * increasing bin IDs.
   * <br>
   * Warning: This binner is subject to double-precision error and may bin incorrectly near bin
   * boundaries.
   *
   * @param logBase is the exponential base to expand the width of bins with.
   * @param startingPower is the starting power to raise the exponential base to.
   * @param powerStepSize is the amount to increase the power the exponential base is raised to for
   *     each bin.
   * @return a binner producing a log-width bins.
   */
  def apply(
      logBase: Double = math.E,
      startingPower: Double = 0.0,
      powerStepSize: Double = 1.0
  ): LogWidthBinner = {
    new LogWidthBinner(logBase, startingPower, powerStepSize)
  }
}

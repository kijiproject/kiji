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

import com.twitter.scalding.Stat
import com.twitter.scalding.UniqueID

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability

/**
 * The configuration describing a histogram. Includes configuration for bin widths/starts.
 *
 * @param mName of the histogram.
 * @param mPath to a file on the local filesystem to write the histogram to.
 * @param mBinner defines the method used to bin values.
 */
@ApiAudience.Framework
@ApiStability.Experimental
final class HistogramConfig private(
    private val mName: String,
    private val mPath: String,
    private val mBinner: HistogramBinner
) {
  /**
   * Increments a counter associated with the provided statistic.
   *
   * @param stat is the quantity being recorded (by the histogram).
   * @param uniqueIdContainer used to identify the job that this profile method is being used
   *     within. This is used to get a kryo configured as it would be for cascading.
   */
  def incrementBinCount(stat: Double, uniqueIdContainer: UniqueID): Unit = {
    val counter = Stat(mBinner.binValue(stat).toString, mName)(uniqueIdContainer)

    counter.inc
  }

  /**
   * Calculates the lower bound of a bin.
   *
   * @param binId is a number identifying the bin.
   * @return the lower bound of a bin.
   */
  def binLowerBound(binId: Int): Double = {
    mBinner.binBoundary(binId)
  }

  /**
   * Calculates the upper bound of a bin.
   *
   * @param binId is a number identifying the bin.
   * @return the upper bound of a bin.
   */
  def binUpperBound(binId: Int): Double = {
    binLowerBound(binId + 1)
  }

  /**
   * Returns the name of the histogram.
   *
   * @return the name of the histogram.
   */
  def name: String = mName

  /**
   * Returns the path to write the histogram to.
   *
   * @return the path to write the histogram to.
   */
  def path: String = mPath
}

/**
 * Companion object providing factory methods.
 */
@ApiAudience.Framework
@ApiStability.Experimental
object HistogramConfig {
  /**
   * The configuration describing a histogram. Includes configuration for bin widths/starts.
   *
   * @param name of the histogram.
   * @param path to write the histogram to.
   * @param binner defines the method used to bin values.
   * @return a new histogram configuration.
   */
  def apply(
      name: String,
      path: String,
      binner: HistogramBinner
  ): HistogramConfig = {
    new HistogramConfig(name, path, binner)
  }
}

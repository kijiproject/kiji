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

import com.google.common.base.Objects;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.scoring.avro.KijiFreshenerRecord;

/**
 * Container representing statistics gathered by a FreshKijiTableReader about all runs of a single
 * Freshener.
 */
@ApiAudience.Public
@ApiStability.Experimental
public final class FreshenerStatistics {
  private final KijiFreshenerRecord mFreshenerRecord;
  private final RunningMean mTimedOutPercent = new RunningMean();
  private final RunningMean mMeanFresheningDuration = new RunningMean();
  private final RunningMean mScoreFunctionRanPercent = new RunningMean();
  private final RunningLogarithmicBin mFresheningDurationBins = new RunningLogarithmicBin(15);

  /**
   * Initialize a new FreshenerStatistics.
   *
   * @param record KijiFreshenerRecord used to build the Freshener whose runs are aggregated in
   *     these statistics.
   */
  private FreshenerStatistics(
      final KijiFreshenerRecord record
  ) {
    mFreshenerRecord = record;
  }

  /**
   * Create a new FreshenerStatistics.
   *
   * @param record KijiFreshenerRecord used to build the Freshener whose runs are aggregated
   *     in these statistics.
   * @return a new FreshenerStatistics.
   */
  public static FreshenerStatistics create(
      final KijiFreshenerRecord record
  ) {
    return new FreshenerStatistics(record);
  }

  /**
   * Get the KijiFreshenerRecord of the Freshener whose runs are aggregated in these
   * statistics.
   *
   * @return the KijiFreshenerRecord of the Freshener whose runs are aggregated in these
   *     statistics.
   */
  public KijiFreshenerRecord getFreshenerRecord() {
    return mFreshenerRecord;
  }

  /**
   * Get the RunningMean representing the percent of the time this Freshener timed out.
   *
   * @return the RunningMean representing the percent of the time this Freshener timed out.
   */
  public RunningMean getTimedOutPercent() {
    return mTimedOutPercent;
  }

  /**
   * Get the RunningMean representing the mean time in nanoseconds this Freshener took to run.
   *
   * @return the RunningMean representing the mean time in nanoseconds this Freshener took to run.
   */
  public RunningMean getMeanFresheningDuration() {
    return mMeanFresheningDuration;
  }

  /**
   * Get the RunningMean representing the percent of the time the ScoreFunction was run by this
   * Freshener.
   *
   * @return the RunningMean representing the percent of the time the ScoreFunction was run by this
   *     Freshener.
   */
  public RunningMean getScoreFunctionRanPercent() {
    return mScoreFunctionRanPercent;
  }

  /**
   * Get the duration of runs of this Freshener binned by duration.
   *
   * @return the duration of runs of this Freshener binned by duration.
   */
  public RunningLogarithmicBin getFresheningDurationBins() {
    return mFresheningDurationBins;
  }

  /**
   * Add the values from the given FreshenerSingleRunStatistics to the aggregated statistics about
   * this Freshener.
   *
   * @param stats the FreshenerSingleRunStatistics to add to this aggregate.
   */
  public void addValues(
      final FreshenerSingleRunStatistics stats
  ) {
    // Convert the booleans to longs so they can be averaged.
    mTimedOutPercent.addValue((stats.timedOut()) ? 1 : 0);
    mScoreFunctionRanPercent.addValue((stats.producerRan()) ? 1 : 0);

    mMeanFresheningDuration.addValue(stats.getDuration());
    mFresheningDurationBins.addValue(stats.getDuration());
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("freshener_record", mFreshenerRecord)
        .add("timed_out_percent", mTimedOutPercent)
        .add("score_function_ran_percent", mScoreFunctionRanPercent)
        .add("mean_duration_nano", mMeanFresheningDuration)
        .add("duration_bins_nano", mFresheningDurationBins)
        .toString();
  }
}

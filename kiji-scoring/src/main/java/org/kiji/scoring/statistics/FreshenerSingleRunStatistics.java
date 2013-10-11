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
 * Container representing statistics gathered by the FreshKijiTableReader about a single run of
 * a Freshener.
 */
@ApiAudience.Public
@ApiStability.Experimental
public final class FreshenerSingleRunStatistics {
  private final boolean mTimedOut;
  private final long mDuration;
  private final boolean mScoreFunctionRan;
  private final KijiFreshenerRecord mFreshenerRecord;

  /**
   * Private constructor.  Use {@link #create(boolean, long, boolean, KijiFreshenerRecord)}.
   *
   * @param timedOut whether the Freshener represented by these statistics timed out.
   * @param duration time in nanoseconds between the start of the freshening request which ran
   *     this Freshener and when this Freshener finished.
   * @param scoreFunctionRan whether this Freshener ran its ScoreFunction.
   * @param freshenerRecord record from which this Freshener was built.
   */
  private FreshenerSingleRunStatistics(
      final boolean timedOut,
      final long duration,
      final boolean scoreFunctionRan,
      final KijiFreshenerRecord freshenerRecord
  ) {
    mTimedOut = timedOut;
    mDuration = duration;
    mScoreFunctionRan = scoreFunctionRan;
    mFreshenerRecord = freshenerRecord;
  }

  /**
   * Create a new FreshenerSingleRunStatistics.
   *
   * @param timedOut whether the Freshener represented by these statistics timed out.
   * @param duration time in nanoseconds between the start of the freshening request which ran
   *     this Freshener and when this Freshener finished.
   * @param producerRan whether the Freshener ran its ScoreFunction.
   * @param freshenerRecord record from which this Freshener was built.
   * @return a new FreshenerSingleRunStatistics.
   */
  public static FreshenerSingleRunStatistics create(
      final boolean timedOut,
      final long duration,
      final boolean producerRan,
      final KijiFreshenerRecord freshenerRecord
  ) {
    return new FreshenerSingleRunStatistics(timedOut, duration, producerRan, freshenerRecord);
  }

  /**
   * Whether this Freshener timed out.
   *
   * @return whether this Freshener timed out.
   */
  public boolean timedOut() {
    return mTimedOut;
  }

  /**
   * Get the time in nanoseconds between the start of the freshening request which ran this
   * Freshener and when this Freshener finished.
   *
   * @return the time in nanoseconds between the start of the freshening request which ran this
   * Freshener and when this Freshener finished.
   */
  public long getDuration() {
    return mDuration;
  }

  /**
   * Whether the Freshener ran its ScoreFunction.
   *
   * @return whether the Freshener ran its ScoreFunction.
   */
  public boolean producerRan() {
    return mScoreFunctionRan;
  }

  /**
   * Get the Freshener record from which this Freshener was built.
   *
   * @return the Freshener record from which this Freshener was built.
   */
  public KijiFreshenerRecord getFreshenerRecord() {
    return mFreshenerRecord;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("freshener_record", mFreshenerRecord)
        .add("timed_out", mTimedOut)
        .add("score_function_ran", mScoreFunctionRan)
        .add("duration_nano", mDuration)
        .toString();
  }
}

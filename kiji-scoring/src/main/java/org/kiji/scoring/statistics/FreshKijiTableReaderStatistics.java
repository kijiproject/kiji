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

import java.util.List;
import java.util.Map;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.scoring.FreshKijiTableReaderBuilder.StatisticGatheringMode;
import org.kiji.scoring.avro.KijiFreshenerRecord;

/** Container representing statistics about Fresheners run by a single FreshKijiTableReader. */
@ApiAudience.Public
@ApiStability.Experimental
public final class FreshKijiTableReaderStatistics {
  private final StatisticGatheringMode mMode;
  private final List<FreshenerSingleRunStatistics> mFreshenerSingleRunStatistics =
      Lists.newArrayList();
  private final Map<KijiFreshenerRecord, FreshenerStatistics> mAggregatedFreshenerStatistics
      = Maps.newHashMap();

  /**
   * Initialize a new FreshKijiTableReaderStatistics with the given StatisticsGatheringMode.
   *
   * @param mode specifies what statistics are gathered by the FreshKijiTableReader represented by
   *     these statistics.
   */
  private FreshKijiTableReaderStatistics(
      final StatisticGatheringMode mode
  ) {
    mMode = mode;
  }

  /**
   * Create a new FreshKijiTableReaderStatistics with the given StatisticsGatheringMode.
   *
   * @param mode specifies what statistics are gathered by the FreshKijiTableReader represented by
   *     these statistics.
   * @return a new FreshKijiTableReaderStatistics.
   */
  public static FreshKijiTableReaderStatistics create(
      final StatisticGatheringMode mode
  ) {
    return new FreshKijiTableReaderStatistics(mode);
  }

  /**
   * Create a new aggregate statistic for a Freshener if none is present, otherwise add the given
   * stats to the existing aggregate.
   *
   * @param stats statistics about a single run of a Freshener to add to the aggregate.
   */
  private void createOrAddAggregate(
      final FreshenerSingleRunStatistics stats
  ) {
    final FreshenerStatistics freshenerStatistics =
        mAggregatedFreshenerStatistics.get(stats.getFreshenerRecord());
    if (null != freshenerStatistics) {
      freshenerStatistics.addValues(stats);
    } else {
      final FreshenerStatistics newFreshenerStatistics =
          FreshenerStatistics.create(stats.getFreshenerRecord());
      newFreshenerStatistics.addValues(stats);
      mAggregatedFreshenerStatistics.put(stats.getFreshenerRecord(), newFreshenerStatistics);
    }
  }

  /**
   * Get the StatisticsGatheringMode used to collect these statistics.
   *
   * @return the StatisticsGatheringMode used to collect these statistics.
   */
  public StatisticGatheringMode getMode() {
    return mMode;
  }

  /**
   * Add a new FreshenerSingleRunStatistics to the raw and aggregate statistics.
   *
   * @param stats statistics representing the run of a single Freshener to add to this collection.
   */
  public void addFreshenerRunStatistics(
      final FreshenerSingleRunStatistics stats
  ) {
    mFreshenerSingleRunStatistics.add(stats);
    createOrAddAggregate(stats);
  }

  /**
   * Get the raw statistics for all Fresheners run by the FreshKijiTableReader represented by these
   * statistics.
   *
   * @return the raw statistics for all Fresheners run by the FreshKijiTableReader represented by
   * these statistics.
   */
  public List<FreshenerSingleRunStatistics> getRawFreshenerRunStatistics() {
    return mFreshenerSingleRunStatistics;
  }

  /**
   * Get the aggregated statistics for each Freshener as defined by the KijiFreshenerRecord
   * from which the Freshener was built.
   *
   * @return the aggregated statistics for each Freshener.
   */
  public Map<KijiFreshenerRecord, FreshenerStatistics> getAggregatedFreshenerStatistics() {
    return mAggregatedFreshenerStatistics;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("statistics_gathering_mode", mMode)
        // raw_statistics are broken, they throw ConcurrentModificationException unless this method
        // and addFreshenerRunStatistics are synchronized.  Wrapping mFreshenerSingleRunStatistics
        // in Lists.newArrayList() also works, but is expensive.  It is unlikely anyone will want to
        // collect and log raw statistics in production, so it's possible that this expense is ok.
        //.add("raw_statistics", mFreshenerSingleRunStatistics)
        .add("aggregated_statistics", mAggregatedFreshenerStatistics.values())
        .toString();
  }
}

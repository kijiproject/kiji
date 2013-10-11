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

package org.kiji.scoring.lib;

import java.util.Map;
import java.util.NavigableSet;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiRowData;
import org.kiji.scoring.FreshenerContext;
import org.kiji.scoring.FreshenerSetupContext;
import org.kiji.scoring.KijiFreshnessPolicy;

/**
 * A stock {@link org.kiji.scoring.KijiFreshnessPolicy} which returns fresh if requested data was
 * modified later than a specified timestamp.
 */
@ApiAudience.Public
@ApiStability.Experimental
public class NewerThan extends KijiFreshnessPolicy {

  public static final String NEWER_THAN_KEY = "org.kiji.scoring.lib.NewerThan.newer_than";

  private long mNewerThanTimestamp = -1;

  /**
   * Default empty constructor for automatic construction. This is for reflection utils. Users
   * should use {@link #NewerThan(long)} instead.
   */
  public NewerThan() {}

  /**
   * Constructor which initializes all state.
   *
   * @param newerThan the unix time in milliseconds before which data is stale.
   */
  public NewerThan(long newerThan) {
    mNewerThanTimestamp = newerThan;
  }

  /**
   * Get the time in milliseconds since 1970 before which data should be considered stale for this
   * freshness policy.
   *
   * @return the time in milliseconds since 1970 before which data should be considered stale for
   * this freshness policy.
   */
  public long getNewerThanTimeInMillis() {
    return mNewerThanTimestamp;
  }

  // One-time setup methods ------------------------------------------------------------------------

  /** {@inheritDoc} */
  @Override
  public Map<String, String> serializeToParameters() {
    final Map<String, String> serialized = Maps.newHashMap();
    serialized.put(NEWER_THAN_KEY, String.valueOf(mNewerThanTimestamp));
    return serialized;
  }

  /** {@inheritDoc} */
  @Override
  public void setup(FreshenerSetupContext context) {
    mNewerThanTimestamp = Long.valueOf(context.getParameter(NEWER_THAN_KEY));
  }

  // per-request methods ---------------------------------------------------------------------------

  /** {@inheritDoc} */
  @Override
  public boolean isFresh(KijiRowData rowData, FreshenerContext context) {
    Preconditions.checkState(-1 != mNewerThanTimestamp,
        "Newer than timestamp not set. Did you call NewerThan.setup?");

    final KijiColumnName columnName = context.getAttachedColumn();

    // If the column does not exist in the row data, it is not fresh.
    if (!rowData.containsColumn(columnName.getFamily(), columnName.getQualifier())) {
      return false;
    }

    NavigableSet<Long> timestamps =
        rowData.getTimestamps(columnName.getFamily(), columnName.getQualifier());
    // If there are no values in the column in the row data, it is not fresh.  If there are values
    // but the newest value is older than mNewerThanTimestamp, it is not fresh.
    return !timestamps.isEmpty() && timestamps.first() >= mNewerThanTimestamp;
  }
}

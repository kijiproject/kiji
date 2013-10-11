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
 * modified within a specified number of milliseconds of the current time.
 *
 */
@ApiAudience.Public
@ApiStability.Experimental
public final class ShelfLife extends KijiFreshnessPolicy {

  public static final String SHELF_LIFE_KEY = "org.kiji.scoring.lib.ShelfLife.shelf_life";

  private long mShelfLifeMillis = -1;

  /**
   * Default empty constructor for automatic construction. This is for reflection utils. Users
   * should use {@link #ShelfLife(long)} instead.
   */
  public ShelfLife() {}

  /**
   * Constructor which initializes all state.
   *
   * @param shelfLife the age in milliseconds beyond which data becomes stale.
   */
  public ShelfLife(long shelfLife) {
    if (shelfLife < 0) {
      throw new IllegalArgumentException("Shelf life must be a positive number of milliseconds.");
    }
    mShelfLifeMillis = shelfLife;
  }

  /**
   * Get the number of milliseconds this shelf life is configured with.
   *
   * @return The 'shelf life' in milliseconds.
   */
  public long getShelfLifeInMillis() {
    return mShelfLifeMillis;
  }

  // One-time setup methods ------------------------------------------------------------------------

  /** {@inheritDoc} */
  @Override
  public Map<String, String> serializeToParameters() {
    final Map<String, String> serialized = Maps.newHashMap();
    serialized.put(SHELF_LIFE_KEY, String.valueOf(mShelfLifeMillis));
    return serialized;
  }

  /** {@inheritDoc} */
  @Override
  public void setup(FreshenerSetupContext context) {
    mShelfLifeMillis = Long.valueOf(context.getParameter(SHELF_LIFE_KEY));
  }

  // per-request methods ---------------------------------------------------------------------------

  /** {@inheritDoc} */
  @Override
  public boolean isFresh(KijiRowData rowData, FreshenerContext context) {
    final KijiColumnName columnName = context.getAttachedColumn();
    if (mShelfLifeMillis == -1) {
      throw new RuntimeException("Shelf life not set.  Did you call ShelfLife.setup()?");
    }

    // If the column does not exist in the row data, it is not fresh.
    if (!rowData.containsColumn(columnName.getFamily(), columnName.getQualifier())) {
      return false;
    }
    NavigableSet<Long> timestamps =
        rowData.getTimestamps(columnName.getFamily(), columnName.getQualifier());
    // If there are no values in the column in the row data, it is not fresh.  If there are values,
    // but the newest is more than mShelfLifeMillis old, it is not fresh.
    return !timestamps.isEmpty()
        && System.currentTimeMillis() - timestamps.first() <= mShelfLifeMillis;
  }
}

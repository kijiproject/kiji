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

import java.util.Collections;
import java.util.Map;
import java.util.NavigableSet;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.scoring.KijiFreshnessPolicy;
import org.kiji.scoring.PolicyContext;

/**
 * A stock {@link org.kiji.scoring.KijiFreshnessPolicy} which returns fresh if requested data was
 * modified later than a specified timestamp.
 */
@ApiAudience.Public
@ApiStability.Experimental
public class NewerThan implements KijiFreshnessPolicy {
  private long mNewerThanTimestamp = -1;

  /**
   * Default empty constructor for automatic construction. User must call
   * {@link #deserialize(String)} to initialize state.
   */
  public NewerThan() {}

  /**
   * Constructor which initializes all state.  No call to {@link #deserialize(String)} is necessary.
   *
   * @param newerThan the unix time in milliseconds before which data is stale.
   */
  public NewerThan(long newerThan) {
    mNewerThanTimestamp = newerThan;
  }

  /** {@inheritDoc} */
  @Override
  public boolean isFresh(KijiRowData rowData, PolicyContext policyContext) {
    final KijiColumnName columnName = policyContext.getAttachedColumn();
    if (mNewerThanTimestamp == -1) {
      throw new RuntimeException(
          "Newer than timestamp not set.  Did you call NewerThan.deserialize()?");
    }
    if (columnName == null) {
      throw new RuntimeException("Target column was not set in the PolicyContext.");
    }
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

  /** {@inheritDoc} */
  @Override
  public boolean shouldUseClientDataRequest() {
    return true;
  }

  /** {@inheritDoc} */
  @Override
  public KijiDataRequest getDataRequest() {
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, KeyValueStore<?, ?>> getRequiredStores() {
    return Collections.emptyMap();
  }

  /** {@inheritDoc} */
  @Override
  public String serialize() {
    // The only required state is the newer than timestamp.
    final JsonObject jsonObject = new JsonObject();
    jsonObject.add("newerThan", new JsonPrimitive(mNewerThanTimestamp));

    return jsonObject.toString();
  }

  /** {@inheritDoc} */
  @Override
  public void deserialize(String policyState) {
    final JsonParser parser = new JsonParser();
    final JsonObject jsonObject = (JsonObject) parser.parse(policyState);
    // Load the newer than timestamp from the policy state.
    mNewerThanTimestamp = jsonObject.get("newerThan").getAsLong();
  }
}

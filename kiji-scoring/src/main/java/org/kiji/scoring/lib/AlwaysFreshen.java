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

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.scoring.KijiFreshnessPolicy;
import org.kiji.scoring.PolicyContext;

/**
 * A stock {@link org.kiji.scoring.KijiFreshnessPolicy} which returns stale for any KijiRowData.
 */
@ApiAudience.Public
@ApiStability.Experimental
public final class AlwaysFreshen implements KijiFreshnessPolicy {

  /** {@inheritDoc} */
  @Override
  public boolean isFresh(KijiRowData rowData, PolicyContext policyContext) {
    return false;
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
    // Return the empty string because AlwaysFreshen requires no state.
    return "";
  }

  /** {@inheritDoc} */
  @Override
  public void deserialize(String policyState) {
    // empty because this policy has no state.
  }
}

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

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.scoring.FreshenerContext;
import org.kiji.scoring.KijiFreshnessPolicy;

/**
 * A stock {@link org.kiji.scoring.KijiFreshnessPolicy} which returns stale for every KijiRowData.
 */
@ApiAudience.Public
@ApiStability.Experimental
public final class AlwaysFreshen extends KijiFreshnessPolicy {

  // per-request methods ---------------------------------------------------------------------------

  /** {@inheritDoc} */
  @Override
  public boolean shouldUseClientDataRequest(FreshenerContext context) {
    return false;
  }

  /** {@inheritDoc} */
  @Override
  public KijiDataRequest getDataRequest(FreshenerContext context) {
    return EMPTY_REQUEST;
  }

  /** {@inheritDoc} */
  @Override
  public boolean isFresh(KijiRowData rowData, FreshenerContext context) {
    return STALE;
  }
}

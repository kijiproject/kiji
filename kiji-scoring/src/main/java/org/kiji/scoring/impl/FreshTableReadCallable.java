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
package org.kiji.scoring.impl;

import java.util.concurrent.Callable;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.scoring.FreshKijiTableReader;
import org.kiji.scoring.FreshKijiTableReader.FreshRequestOptions;

/**
 * Callable which performs a freshened read from a table. Used in a Future to freshen
 * asynchronously.
 *
 * <p>
 *   This class is package private to be used by InternalFreshKijiTableReader. It should not be used
 *   elsewhere.
 * </p>
 */
@ApiAudience.Private
final class FreshTableReadCallable implements Callable<KijiRowData> {

  private final FreshKijiTableReader mReader;
  private final EntityId mEntityId;
  private final KijiDataRequest mDataRequest;
  private final FreshRequestOptions mOptions;

  /**
   * Initialize a new FreshTableReadCallable.
   *
   * @param reader the FreshKijiTableReader to use to perform the read.
   * @param entityId the EntityId of the row from which to read and freshen data.
   * @param dataRequest the KijiDataRequest defining the data to read and freshen from the row.
   * @param options options applicable to the freshening request.
   */
  public FreshTableReadCallable(
      final FreshKijiTableReader reader,
      final EntityId entityId,
      final KijiDataRequest dataRequest,
      final FreshRequestOptions options
  ) {
    mReader = reader;
    mEntityId = entityId;
    mDataRequest = dataRequest;
    mOptions = options;
  }

  /** {@inheritDoc} */
  @Override
  public KijiRowData call() throws Exception {
    return mReader.get(mEntityId, mDataRequest, mOptions);
  }
}

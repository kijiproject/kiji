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
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableReaderPool;

/**
 * Callable which performs a read from a table.  Used in a Future to read asynchronously.
 *
 * <p>
 *   This class is package private to be used by InternalFreshKijiTableReader. It should not be used
 *   elsewhere.
 * </p>
 */
@ApiAudience.Private
final class TableReadCallable implements Callable<KijiRowData> {

  private final KijiTableReaderPool mReaderPool;
  private final EntityId mEntityId;
  private final KijiDataRequest mDataRequest;

  /**
   * Initialize a new TableReadCallable.
   *
   * @param readerPool the KijiTableReaderPool from which to get a reader to perform the read.
   * @param entityId the EntityId of the row from which to read data.
   * @param dataRequest the KijiDataRequest defining the data to read from the row.
   */
  public TableReadCallable(
      final KijiTableReaderPool readerPool,
      final EntityId entityId,
      final KijiDataRequest dataRequest
  ) {
    mReaderPool = readerPool;
    mEntityId = entityId;
    mDataRequest = dataRequest;
  }

  /** {@inheritDoc} */
  @Override
  public KijiRowData call() throws Exception {
    final KijiTableReader reader = ScoringUtils.getPooledReader(mReaderPool);
    try {
      return reader.get(mEntityId, mDataRequest);
    } finally {
      reader.close();
    }
  }
}

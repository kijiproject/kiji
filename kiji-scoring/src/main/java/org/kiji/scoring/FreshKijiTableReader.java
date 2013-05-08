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
package org.kiji.scoring;

import java.io.IOException;
import java.util.List;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTableReader;

/**
 * Interface for reading freshened data from a Kiji Table.
 *
 * <p>
 *   Utilizes {@link org.kiji.schema.EntityId} and {@link org.kiji.schema.KijiDataRequest}
 *   to return {@link org.kiji.schema.KijiRowData}.  Accessible via
 *   {@link FreshKijiTableReaderFactory#openReader(org.kiji.schema.KijiTable, int)}.
 * </p>
 *
 * <p>
 *   Reads performed with FreshKijiTableReaders pass through freshness filters according to
 *   {@link org.kiji.scoring.KijiFreshnessPolicy}s registered in the
 *   {@link org.kiji.schema.KijiMetaTable} that services the table associated with this reader.
 * </p>
 *
 * <p>
 *   Freshening describes the process of conditionally applying a
 *   {@link org.kiji.mapreduce.produce.KijiProducer} to a row in response to user queries for data
 *   in that row.  Consequently, methods of a FreshKijiTableReader have the possibility of
 *   generating side effect writes to the rows users query.
 * </p>
 *
 * <p>
 *   FreshKijiTableReader get methods are used in the same way as regular KijiTableReader get
 *   methods.
 * </p>
 * <p>
 *   To get the three most recent versions of cell data from a column <code>bar</code> from
 *   the family <code>foo</code> within the time range (123, 456):
 * <pre>
 *   KijiDataRequestBuilder builder = KijiDataRequest.builder()
 *     .withTimeRange(123L, 456L);
 *     .newColumnsDef()
 *     .withMaxVersions(3)
 *     .add("foo", "bar");
 *   final KijiDataRequest request = builder.build();
 *
 *   final KijiTableReader freshReader = FreshKijiTableReaderFactory.openFreshReader(myKijiTable);
 *   final KijiRowData data = freshReader.get(myEntityId, request);
 * </pre>
 *   This code will return the three most recent values including newly generated values output by
 *   the producer if it ran.
 * </p>
 *
 * <p>
 *   Instances of this reader are not threadsafe and should be restricted to use in a single thread.
 *   Because this class maintains a connection to the underlying KijiTable and other resources,
 *   users should call {@link #close()} when done using a reader.
 * </p>
 *
 * @see org.kiji.scoring.KijiFreshnessPolicy
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
public interface FreshKijiTableReader extends KijiTableReader {

  /**
   * Freshens data as needed before returning.  Behaves the same as
   *   {@link org.kiji.schema.KijiTableReader#get(org.kiji.schema.EntityId,
   *   org.kiji.schema.KijiDataRequest)} except for the possibility of freshening.
   *
   * @param entityId EntityId of the row to query.
   * @param dataRequest What data to retrieve.
   * @return The data requested after freshening.
   * @throws IOException in case of an error reading from the table.
   */
  @Override
  KijiRowData get(EntityId entityId, KijiDataRequest dataRequest) throws IOException;

  /**
   * Attempts to freshen all data requested in parallel before returning the most up to date data
   *   available.
   *
   * @param entityIds A list of EntityIds for the rows to query.
   * @param dataRequest What data to retrieve.
   * @return a list of KijiRowData corresponding the the EntityIds and data request after
   *   freshening.
   * @throws IOException in case of an error reading from the table.
   */
  @Override
  List<KijiRowData> bulkGet(List<EntityId> entityIds, KijiDataRequest dataRequest)
      throws IOException;

  /**
   * Clear cached freshness policies and reload from the metatable.
   *
   * @throws IOException in case of an error reading from the metatable.
   */
  void reloadPolicies() throws IOException;
}

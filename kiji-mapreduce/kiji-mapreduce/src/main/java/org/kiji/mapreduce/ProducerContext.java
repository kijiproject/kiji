/**
 * (c) Copyright 2012 WibiData, Inc.
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

package org.kiji.mapreduce;

import java.io.IOException;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;

/** Context for producers to output new cells in the currently processed row. */
@ApiAudience.Public
public interface ProducerContext extends KijiContext {
  /** @return the entity ID of the row being currently written to. */
  EntityId getEntityId();

  /**
   * Outputs a new cell to the configured output column with the current timestamp.
   *
   * Valid only when the producer is configured to output to a single column.
   *
   * @param value Value to write.
   * @throws IOException on I/O error.
   *
   * @param <T> Type of the value to emit.
   */
  <T> void put(T value)
      throws IOException;

  /**
   * Outputs a new cell to the configured output column.
   *
   * Valid only when the producer is configured to output to a single column.
   *
   * @param timestamp Timestamp of the cell to write.
   * @param value Value to write.
   * @throws IOException on I/O error.
   *
   * @param <T> Type of the value to emit.
   */
  <T> void put(long timestamp, T value)
      throws IOException;

  /**
   * Outputs a new cell to the configured output column with the current timestamp.
   *
   * Valid only when the producer is configured to output to a map-type family.
   *
   * @param qualifier Qualifier of the cell to output.
   * @param value Value of the cell to output.
   * @throws IOException on I/O error.
   *
   * @param <T> Type of the value to emit.
   */
  <T> void put(String qualifier, T value)
      throws IOException;

  /**
   * Outputs a new cell to the configured output column.
   *
   * Valid only when the producer is configured to output to a map-type family.
   *
   * @param qualifier Qualifier of the cell to output.
   * @param timestamp Timestamp of the cell to write.
   * @param value Value of the cell to output.
   * @throws IOException on I/O error.
   *
   * @param <T> Type of the value to emit.
   */
  <T> void put(String qualifier, long timestamp, T value)
      throws IOException;
}

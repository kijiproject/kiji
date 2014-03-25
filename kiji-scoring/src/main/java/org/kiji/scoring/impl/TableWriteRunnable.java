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

import java.io.IOException;
import java.util.concurrent.Future;

import org.apache.hadoop.hbase.HConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiIOException;
import org.kiji.scoring.impl.MultiBufferedWriter.SingleBuffer;

/**
 * Runnable to asynchronously persist a value to a Kiji table.
 *
 * <p>
 *   This class is package private to be used by InternalFreshKijiTableReader. It should not be used
 *   elsewhere.
 * </p>
 *
 * @param <T> type of the value to write.
 */
@ApiAudience.Private
final class TableWriteRunnable<T> implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(TableWriteRunnable.class);

  private final SingleBuffer mBuffer;
  private final EntityId mEntityId;
  private final String mFamily;
  private final String mQualifier;
  private final T mValue;

  /**
   * Initialize a new TableWriteRunnable.
   *
   * @param buffer SingleBuffer with which to write the value.
   * @param entityId EntityId of the row into which to write the value.
   * @param family Kiji family of the column into which to write the value.
   * @param qualifier Kiji qualifier of the column into which to write the value.
   * @param value Value to write into the given column and row.
   */
  public TableWriteRunnable(
      final SingleBuffer buffer,
      final EntityId entityId,
      final String family,
      final String qualifier,
      final T value
  ) {
    mBuffer = buffer;
    mEntityId = entityId;
    mFamily = family;
    mQualifier = qualifier;
    mValue = value;
  }

  /**
   * Initialize a new TableWriteRunnable.
   *
   * @param buffer SingleBuffer with which to write the value.
   * @param entityId EntityId of the row into which to write the value.
   * @param family Kiji family of the column into which to write the value.
   * @param qualifier Kiji qualifier of the column into which to write the value.
   * @param value Future representing the asynchronously calculated value to write.
   */
  public TableWriteRunnable(
      final SingleBuffer buffer,
      final EntityId entityId,
      final String family,
      final String qualifier,
      final Future<T> value
  ) {
    mBuffer = buffer;
    mEntityId = entityId;
    mFamily = family;
    mQualifier = qualifier;
    mValue = ScoringUtils.getFromFuture(value);
  }

  /** {@inheritDoc} */
  @Override
  public void run() {
    mBuffer.put(mEntityId, mFamily, mQualifier, HConstants.LATEST_TIMESTAMP, mValue);
    try {
      mBuffer.flush();
    } catch (IOException ioe) {
      LOG.debug("failed to write: {} - {}:{} - {}", mEntityId, mFamily, mQualifier, mValue);
      throw new KijiIOException(ioe);
    }
    LOG.debug("successfully wrote: {} - {}:{} - {}", mEntityId, mFamily, mQualifier, mValue);
  }
}

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

package org.kiji.scoring.impl;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiBufferedWriter;
import org.kiji.schema.KijiTable;

/**
 * Buffered writer supporting connection sharing to minimize opened writer connections. Buffers
 * are identified by a String buffer name and each must be flushed separately.
 */
@ApiAudience.Private
public final class MultiBufferedWriter implements Closeable {
  /** Delegate BufferedWriter to actually perform writes. */
  private final KijiBufferedWriter mWriter;
  /** Mapping from buffer name to list of fully qualified puts. */
  private final Map<String, List<EFQTV>> mBuffers;

  /**
   * Default constructor.
   *
   * @param table the KijiTable to which these buffers write.
   * @throws IOException in case of an error opening the writer connection.
   */
  public MultiBufferedWriter(final KijiTable table) throws IOException {
    mWriter = table.getWriterFactory().openBufferedWriter();
    mBuffers = new HashMap<String, List<EFQTV>>();
  }

  /**
   * Container class representing all data needed to perform a put operation.  EFQTV stands for
   * Entity, Family, Qualifier, Timestamp, Value.
   *
   * @param <V> the type of data to be put.
   */
  private static final class EFQTV<V> {
    private final EntityId mEntityId;
    private final String mFamily;
    private final String mQualifer;
    private final long mTimestamp;
    private final V mValue;

    /**
     * Default constructor.
     *
     * @param entityId the EntityId to store in this container.
     * @param family the family name to store in this container.
     * @param qualifier the qualifier name to store in this container.
     * @param timestamp the timestamp to store in this container.
     * @param value the value to store in this container.
     */
    private EFQTV(
        final EntityId entityId,
        final String family,
        final String qualifier,
        final long timestamp,
        final V value) {
      mEntityId = entityId;
      mFamily = family;
      mQualifer = qualifier;
      mTimestamp = timestamp;
      mValue = value;
    }
  }

  /**
   * Adds an EFQTV to the given buffer.
   *
   * @param bufferName the name of the buffer into which the EFQTV should be added.
   * @param efqtv the EFQTV to put into the given buffer.
   */
  private void addToBuffer(final String bufferName, final EFQTV efqtv) {
    synchronized (mBuffers) {
      final List<EFQTV> buffer = mBuffers.get(bufferName);
      if (null != buffer) {
        buffer.add(efqtv);
      } else {
        mBuffers.put(bufferName, Lists.newArrayList(efqtv));
      }
    }
  }

  // Public Interface ------------------------------------------------------------------------------

  /**
   * Put the given information into the specified buffer.
   *
   * @param bufferName the name of the buffer into which to put the data.
   * @param eid the EntityId of the target row.
   * @param family the family of the target column.
   * @param qualifier the qualifier of the target column.
   * @param timestamp the timestamp for the target cell.
   * @param value the value for the target cell.
   * @param <V> the type of the value for the target cell.
   */
  public <V> void put(
      String bufferName,
      EntityId eid,
      String family,
      String qualifier,
      long timestamp,
      V value) {
    addToBuffer(bufferName, new EFQTV<V>(eid, family, qualifier, timestamp, value));
  }

  /**
   * Flush all writes from a given buffer.
   *
   * @param bufferId the ID of the buffer to flush.
   * @throws IOException in case of an error writing to the table.
   */
  public void flush(String bufferId) throws IOException {
    synchronized (mWriter) {
      for (EFQTV efqtv : mBuffers.get(bufferId)) {
        mWriter.put(
            efqtv.mEntityId,
            efqtv.mFamily,
            efqtv.mQualifer,
            efqtv.mTimestamp,
            efqtv.mValue);
      }
      mWriter.flush();
      mBuffers.remove(bufferId);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    mWriter.close();
  }
}

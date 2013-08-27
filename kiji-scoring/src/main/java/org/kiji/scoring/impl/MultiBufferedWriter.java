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
import java.util.List;

import com.google.common.collect.Lists;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiBufferedWriter;
import org.kiji.schema.KijiTable;

/**
 * Buffered writer supporting connection sharing to minimize opened writer connections.
 * SingleBuffers retrieved from this class do not need to be closed when they are no longer needed.
 */
@ApiAudience.Private
public final class MultiBufferedWriter implements Closeable {

  // -----------------------------------------------------------------------------------------------
  // Inner classes
  // -----------------------------------------------------------------------------------------------

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
   * A single buffer view of a MultiBufferedWriter.
   */
  public final class SingleBuffer {

    private final List<EFQTV> mBuffer = Lists.newArrayList();

    /**
     * Initialize a new SingleBuffer which delegates to a MultiBufferedWriter to flush data.
     */
    public SingleBuffer() { }

    /**
     * Put the given information into this buffer.
     *
     * @param entityId the row on which to write the value.
     * @param family the family into which to write the value.
     * @param qualifier the qualifier into which to write the value.
     * @param timestamp the timestamp at which to write the value.
     * @param value the value to write to the table.
     * @param <V> the type of the value.
     */
    public <V> void put(
        final EntityId entityId,
        final String family,
        final String qualifier,
        final long timestamp,
        final V value
    ) {
      synchronized (mBuffer) {
        mBuffer.add(new EFQTV<V>(entityId, family, qualifier, timestamp, value));
      }
    }

    /**
     * Flush the contents of this buffer.
     *
     * @throws IOException in case of an error writing to the table.
     */
    public void flush() throws IOException {
      synchronized (mBuffer) {
        synchronized (mWriter) {
          for (EFQTV efqtv : mBuffer) {
            mWriter.put(
                efqtv.mEntityId,
                efqtv.mFamily,
                efqtv.mQualifer,
                efqtv.mTimestamp,
                efqtv.mValue);
          }
          mWriter.flush();
          mBuffer.clear();
        }
      }
    }
  }

  // -----------------------------------------------------------------------------------------------
  // State
  // -----------------------------------------------------------------------------------------------

  /** Delegate BufferedWriter to actually perform writes. */
  private final KijiBufferedWriter mWriter;

  /**
   * Default constructor.
   *
   * @param table the KijiTable to which these buffers write.
   * @throws IOException in case of an error opening the writer connection.
   */
  public MultiBufferedWriter(final KijiTable table) throws IOException {
    mWriter = table.getWriterFactory().openBufferedWriter();
  }

  // -----------------------------------------------------------------------------------------------
  // Public Interface
  // -----------------------------------------------------------------------------------------------

  /**
   * Get a new SingleBuffer view of this MultiBufferedWriter.
   *
   * @return a new SingleBuffer which delegates to this MultiBufferedWriter to flush data.
   */
  public SingleBuffer openSingleBuffer() {
    return new SingleBuffer();
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    mWriter.close();
  }
}

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

package org.kiji.mapreduce.framework;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.mapreduce.platform.KijiMRPlatformBridge;

/**
 * A KeyValue-like object that implements WritableComparable.
 *
 * <p>This class works around two shortcomings in the existing KeyValue implementation
 * within HBase:
 *   <ol>
 *     <li><code>KeyValue</code> is Writable, but not WritableComparable.</li>
 *     <li><code>KeyValue</code> maintains state in private caches, which is not cleared
 *     during Writable deserialization.</li>
 *   </ol>
 * </p>
 *
 * <p>HFileKeyValue wraps a <code>KeyValue</code> object, and resets it during
 * Writable deserialization to clear any cached state.</p>
 * <p>Note: this class has a natural ordering that is inconsistent with equals.</p>
 * @see org.apache.hadoop.hbase.KeyValue
 */
@ApiAudience.Framework
@ApiStability.Evolving
public final class HFileKeyValue implements WritableComparable<HFileKeyValue> {
  private static final Logger LOG = LoggerFactory.getLogger(HFileKeyValue.class);

  /** The wrapped KeyValue (this is never null). */
  private KeyValue mKeyValue;

  /** Default constructor for Writable instantiation -- you probably don't want to use this. */
  public HFileKeyValue() {
    mKeyValue = new KeyValue();
  }

  /**
   * Constructor that builds a filled HFileKeyValue.
   *
   * @param rowKey The row key.
   * @param family The column family.
   * @param qualifier The column qualifier.
   * @param timestamp The cell timestamp.
   * @param value The cell value.
   */
  public HFileKeyValue(byte[] rowKey, byte[] family, byte[] qualifier, long timestamp,
      byte[] value) {
    this(rowKey, family, qualifier, timestamp, Type.Put, value);
  }

  /**
   * Constructor that builds a filled HFileKeyValue.
   *
   * @param rowKey The row key.
   * @param family The column family.
   * @param qualifier The column qualifier.
   * @param timestamp The cell timestamp.
   * @param type The HBase cell type (put or one of the various deletes)
   * @param value The cell value.
   */
  public HFileKeyValue(byte[] rowKey, byte[] family, byte[] qualifier, long timestamp,
      Type type, byte[] value) {
    mKeyValue = new KeyValue(rowKey, family, qualifier, timestamp, type.getKeyValueType(),
        value);
  }

  /**
   * Creates a new HFileKeyValue with a row key (all other fields left empty).
   *
   * @param rowKey An HBase row key.
   * @return A new HFileKeyValue.
   */
  public static HFileKeyValue createFromRowKey(byte[] rowKey) {
    return new HFileKeyValue(rowKey, HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY,
        HConstants.LATEST_TIMESTAMP, HConstants.EMPTY_BYTE_ARRAY);
  }

  /**
   * Gets the KeyValue's column family.
   *
   * <p>This requires some array copying, which is only done once within the wrapped
   * KeyValue object and cached.</p>
   *
   * @return The column family.
   * @see org.apache.hadoop.hbase.KeyValue
   */
  public byte[] getFamily() {
    return mKeyValue.getFamily();
  }

  /**
   * Gets the KeyValue's row key.
   *
   * <p>This requires some array copying, which is only done once within the wrapped
   * KeyValue object and cached.</p>
   *
   * @return The row key.
   * @see org.apache.hadoop.hbase.KeyValue
   */
  public byte[] getRowKey() {
    return mKeyValue.getRow();
  }

  /**
   * Provides access to the wrapped KeyValue object.
   *
   * @return The wrapped HBase KeyValue object.
   * @see org.apache.hadoop.hbase.KeyValue
   */
  public KeyValue getKeyValue() {
    return mKeyValue;
  }

  /**
   * Sets the timestamp that should be used if the KeyValue is using
   * HConstants.LATEST_TIMESTAMP.
   *
   * @param now The timestamp as a byte array.
   * @return Whether the timestamp of the KeyValue was modified.
   * @see org.apache.hadoop.hbase.KeyValue
   */
  public boolean updateLatestStamp(byte[] now) {
    return mKeyValue.updateLatestStamp(now);
  }

  /**
   * Gets the length of the serialized HFileKeyValue in bytes.
   *
   * @return The size of the HFileKeyValue in bytes.
   */
  public int getLength() {
    return mKeyValue.getLength();
  }

  /** {@inheritDoc} */
  @Override
  public int compareTo(HFileKeyValue other) {
    return KijiMRPlatformBridge.get().compareKeyValues(mKeyValue, other.mKeyValue);
  }

  // TODO(KIJIMR-85): Add equals() and hashCode() here. Remove findBugs exclusion
  // and javadoc warning.

  /** {@inheritDoc} */
  @Override
  public void readFields(DataInput in) throws IOException {
    mKeyValue = KijiMRPlatformBridge.get().readKeyValue(in);
  }

  /** {@inheritDoc} */
  @Override
  public void write(DataOutput out) throws IOException {
    KijiMRPlatformBridge.get().writeKeyValue(mKeyValue, out);
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return mKeyValue.toString();
  }

  /**
   * A type code to identify key values as puts or various deletes.
   *
   * <p>This reflects the namesake enum within KeyValue.  There is no DeleteRow since
   * HBase does not represent that operation with a single KeyValue: instead a
   * DeleteFamily would be issued for each locality group in the table.</p>
   */
  public static enum Type {
    Put(KeyValue.Type.Put),
    DeleteFamily(KeyValue.Type.DeleteFamily),
    DeleteColumn(KeyValue.Type.DeleteColumn),
    DeleteCell(KeyValue.Type.Delete);

    private final KeyValue.Type mKeyValueType;

    /**
     * Constructor.
     *
     * @param keyValueType The equivalent underlying HBase KeyValue.Type.
     */
    Type(final KeyValue.Type keyValueType) {
      this.mKeyValueType = keyValueType;
    }

    /**
     * Get the equivalent HBase KeyValue.Type.
     *
     * @return The equivalent underlying HBase KeyValue.Type.
     */
    private KeyValue.Type getKeyValueType() {
      return mKeyValueType;
    }
  }

  /**
   * A comparator that works over byte arrays, to avoid serialization.
   *
   * <p>This comparator is registered with the Writable serialization framework so it is
   * used when comparing HFileKeyValue keys.</p>
   */
  public static final class FastComparator extends WritableComparator {
    /** Constructor. */
    public FastComparator() {
      super(HFileKeyValue.class);
    }

    /** {@inheritDoc} */
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      // Here, we are given two byte arrays with start offsets and lengths.
      //
      // Both byte arrays were the result of the Writable serialization calling our
      // write() method, which means we can expect (in the following order):
      //
      // 4 bytes - The first 4 bytes is an INTEGER that specifies the length of the entire
      //     KeyValue buffer.
      // 4 bytes - The second 4 bytes is an INTEGER that specifies the length of the KEY
      //     portion of the KeyValue (call this KEYLENGTH).
      // 4 bytes - The third 4 bytes is an INTEGER that specifies that length of the VALUE
      //     portion of the KeyValue (call this VALUELENGTH).
      // KEYLENGTH bytes - The next KEYLENGTH bytes is the KEY buffer.
      // VALUELENGTH bytes - The final VALUELENGTH bytes is the VALUE buffer.
      //
      // Use the key comparator to compare just the KEY portion of the KeyValue buffers.
      // KeyValue.ROW_OFFSET is 8 bytes, the size of KEYLENGTH and VALUELENGTH.
      return KijiMRPlatformBridge.get().compareFlatKey(
          b1,
          s1 + Bytes.SIZEOF_INT + KeyValue.ROW_OFFSET,
          Bytes.toInt(b1, s1 + Bytes.SIZEOF_INT),
          b2,
          s2 + Bytes.SIZEOF_INT + KeyValue.ROW_OFFSET,
          Bytes.toInt(b2, s2 + Bytes.SIZEOF_INT));
    }
  }

  // Register the FastComparator for HFileKeyValue comparisons.
  static {
    WritableComparator.define(HFileKeyValue.class, new FastComparator());
  }
}

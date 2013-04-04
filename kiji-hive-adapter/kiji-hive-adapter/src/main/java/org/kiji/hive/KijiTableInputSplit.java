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

package org.kiji.hive;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.FileSplit;

import org.kiji.schema.KijiURI;

/**
 * There is a bug in Hive that causes an IllegalArgumentException to
 * be thrown if this is not an instance of FileSplit. So even though
 * there are no files involved, we need to extend FileSplit.
 */
public class KijiTableInputSplit extends FileSplit {
  private KijiURI mKijiURI;
  private byte[] mRegionStartKey;
  private byte[] mRegionEndKey;

  /**
   * Default constructor required for Writable deserialization.
   */
  public KijiTableInputSplit() {
    this(null, null, null, null, null);
  }

  /**
   * Constructs an input split description.
   *
   * @param kijiURI The URI of the Kiji table.
   * @param regionStartKey The HBase start key of the region (inclusive).
   * @param regionEndKey The HBase end key of the region (exclusive).
   * @param regionHost The hostname of the machine currently serving the region.
   * @param dummyPath A dummy file path for Hive (it must match what
   *     is in the configuration according to FileInputFormat#getInputFilePaths);
   */
  public KijiTableInputSplit(KijiURI kijiURI,
      byte[] regionStartKey, byte[] regionEndKey, String regionHost, Path dummyPath) {
    super(dummyPath, 0L, 0L, new String[] { regionHost });

    mKijiURI = kijiURI;
    mRegionStartKey = regionStartKey;
    mRegionEndKey = regionEndKey;
  }

  /**
   * Gets the kiji table URI containing this split.
   *
   * @return The table URI.
   */
  public KijiURI getKijiTableURI() {
    return mKijiURI;
  }

  /**
   * Gets the HTable row key that marks the beginning of this split.
   *
   * @return The row key.
   */
  public byte[] getRegionStartKey() {
    return mRegionStartKey;
  }

  /**
   * Gets the HTable row key that marks the end (exclusive) of this split.
   *
   * @return The row key.
   */
  public byte[] getRegionEndKey() {
    return mRegionEndKey;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    String kijiURIString = in.readUTF();
    mKijiURI = KijiURI.newBuilder(kijiURIString).build();
    mRegionStartKey = Bytes.readByteArray(in);
    mRegionEndKey = Bytes.readByteArray(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeUTF(mKijiURI.toString());
    Bytes.writeByteArray(out, mRegionStartKey);
    Bytes.writeByteArray(out, mRegionEndKey);
  }
}

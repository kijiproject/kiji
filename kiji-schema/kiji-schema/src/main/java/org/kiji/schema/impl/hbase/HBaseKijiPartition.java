/**
 * (c) Copyright 2015 WibiData, Inc.
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

package org.kiji.schema.impl.hbase;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.KijiPartition;

/**
 * An HBase Kiji Partition. Corresponds to an HBase region.
 */
@ApiAudience.Framework
@ApiStability.Experimental
public final class HBaseKijiPartition implements KijiPartition {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseKijiPartition.class);

  /** Host of the region. */
  private final InetAddress mHost;

  /** The start rowkey, inclusive. */
  private byte[] mStartKey;

  /** The end rowkey, exclusive. */
  private byte[] mEndKey;

  /**
   * Create a new HBase Kiji Partition.
   *
   * @param host The partition host.
   * @param startKey The partition start key.
   * @param endKey The partition end key.
   */
  private HBaseKijiPartition(
      final InetAddress host,
      final byte[] startKey,
      final byte[] endKey
  ) {
    mHost = host;
    mStartKey = startKey;
    mEndKey = endKey;
  }

  /**
   * Get the HBase region's host.
   *
   * @return The partitions host machine.
   */
  public InetAddress getHost() {
    return mHost;
  }

  /**
   * Gets the start key (inclusive) of this partition.
   *
   * @return The start key (inclusive) of this partition.
   */
  public byte[] getStartKey() {
    return mStartKey;
  }

  /**
   * Gets the end key (exclusive) of this partition.
   *
   * @return The end key (exclusive) of this partition.
   */
  public byte[] getEndKey() {
    return mEndKey;
  }

  /**
   * Get the Cassandra Kiji Partitions for the given cluster.
   *
   * @param htable An open connection to the HBase table.
   * @return The collection of Kiji partitions.
   * @throws IOException if a remote or network exception occurs.
   */
  public static Collection<HBaseKijiPartition> getPartitions(
      final HTable htable
  ) throws IOException {
    final ImmutableList.Builder<HBaseKijiPartition> partitions = ImmutableList.builder();

    NavigableMap<HRegionInfo, ServerName> regionLocations = htable.getRegionLocations();
    for (Map.Entry<HRegionInfo, ServerName> regionLocation : regionLocations.entrySet()) {
      partitions.add(
          new HBaseKijiPartition(
              InetAddress.getByName(regionLocation.getValue().getHostname()),
              regionLocation.getKey().getStartKey(),
              regionLocation.getKey().getEndKey()));
    }

    return partitions.build();
  }
}

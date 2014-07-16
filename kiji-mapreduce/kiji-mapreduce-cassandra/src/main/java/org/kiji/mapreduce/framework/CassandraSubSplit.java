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

package org.kiji.mapreduce.framework;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Class representing a smallest-possible range of tokens that share replica nodes.
 *
 * This class essentially maps to a Cassandra virtual node (vnode).
 */
final class CassandraSubSplit {
  // TODO: Add separate field for actual owner of token, versus replica nodes?
  /** Starting token (inclusive).  */
  private final long mStartToken;

  /** Ending token (inclusive). */
  private final long mEndToken;

  /** List of hosts that contain copies of data in the token range. */
  private final Set<String> mHosts;

  /** Minimum token value (assuming Murmur3 partitioner). */
  public static final long RING_START_TOKEN = Long.MIN_VALUE;

  /** Maximum token value (assuming Murmur3 partitioner). */
  public static final long RING_END_TOKEN = Long.MAX_VALUE;

  /**
   * Create a subsplit given a token range and a set of replica nodes.
   *
   * @param startToken The minimum token for the subsplit (inclusive).
   * @param endToken The maximum token for the subsplit (inclusive).
   * @param hosts A set of replica nodes for this token range.
   * @return A new subsplit for this token range.
   */
  public static CassandraSubSplit createFromHostSet(
      long startToken, long endToken, Set<String> hosts) {
    return new CassandraSubSplit(startToken, endToken, hosts);
  }

  /**
   * Create a subsplit given a token range and a set of replica nodes.
   *
   * @param startToken The minimum token for the subsplit (inclusive).
   * @param endToken The maximum token for the subsplit (inclusive).
   * @param host The master node for this token range.
   * @return A new subsplit for this token range.
   */
  public static CassandraSubSplit createFromHost(long startToken, long endToken, String host) {
    Set<String> hosts = Sets.newHashSet();
    hosts.add(host);
    return new CassandraSubSplit(startToken, endToken, hosts);
  }

  /**
   * Private constructor for a subsplit.
   *
   * @param startToken The minimum token for the subsplit (inclusive).
   * @param endToken The maximum token for the subsplit (inclusive).
   * @param hosts A set of replica nodes for this token range.
   */
  private CassandraSubSplit(long startToken, long endToken, Set<String> hosts) {
    Preconditions.checkNotNull(hosts);
    Preconditions.checkArgument(hosts.size() > 0);
    for (String host : hosts) {
      Preconditions.checkNotNull(host);
      Preconditions.checkArgument(host.length() > 1);
    }
    this.mStartToken = startToken;
    this.mEndToken = endToken;
    this.mHosts = Sets.newHashSet(hosts);
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return String.format(
        "Subsplit from %s to %s @ %s",
        mStartToken,
        mEndToken,
        mHosts
    );
  }

  /**
   * Getter for the minimum token value for this subsplit.
   *
   * @return The minimum token value for this subsplit.
   */
  public long getStartToken() {
    return mStartToken;
  }

  /**
   * Getter for the maximum token value for this subsplit.
   *
   * @return The maximum token value for this subsplit.
   */
  public long getEndToken() {
    return mEndToken;
  }

  /**
   * Getter for the replica nodes for this subsplit.
   *
   * @return The replica nodes for this subsplit.
   */
  public Set<String> getHosts() {
    return mHosts;
  }

  /**
   * Get a comma-separated list of the hosts for this subsplit.
   *
   * @return A CSV of hosts, as a string.
   */
  public String getSortedHostListAsString() {
    Preconditions.checkNotNull(mHosts);
    List<String> hostList = Lists.newArrayList(mHosts);
    Collections.sort(hostList);
    Preconditions.checkNotNull(hostList);
    Preconditions.checkArgument(hostList.size() > 0);
    return Joiner.on(",").join(hostList);
  }
}

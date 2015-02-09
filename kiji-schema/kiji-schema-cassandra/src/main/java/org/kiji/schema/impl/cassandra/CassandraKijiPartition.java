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

package org.kiji.schema.impl.cassandra;

import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

import java.net.InetAddress;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.KijiPartition;

/**
 * A Cassandra Kiji Partition.  Corresponds to the token range of a Cassandra VNode.
 */
@ApiAudience.Framework
@ApiStability.Experimental
public class CassandraKijiPartition implements KijiPartition {

  /** Host of token range. */
  private final InetAddress mHost;

  /** The token range. */
  private final Range<Long> mTokenRange;

  /**
   * Construct a new token range.
   *
   * @param host The partition host.
   * @param tokenRange The token range.
   */
  public CassandraKijiPartition(final InetAddress host, final Range<Long> tokenRange) {
    mHost = host;
    mTokenRange = tokenRange;
  }

  /**
   * Get the host of this partition.
   *
   * @return The host of this partition.
   */
  public InetAddress getHost() {
    return mHost;
  }

  /**
   * The token range of this partition.
   *
   * @return The token range for this partition.
   */
  public Range<Long> getTokenRange() {
    return mTokenRange;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("host", mHost)
        .add("token-range", mTokenRange)
        .toString();
  }

  /**
   * Get the Cassandra Kiji Partitions for the given cluster.
   *
   * @param session An open connection to the cluster.
   * @return The collection of Kiji partitions.
   */
  public static Collection<CassandraKijiPartition> getPartitions(
      Session session
  ) {
    final SortedMap<Long, InetAddress> startTokens = getStartTokens(session);
    final Map<Range<Long>, InetAddress> tokenRanges = getTokenRanges(startTokens);

    final ImmutableList.Builder<CassandraKijiPartition> partitions = ImmutableList.builder();

    for (Map.Entry<Range<Long>, InetAddress> tokenRange : tokenRanges.entrySet()) {
      partitions.add(new CassandraKijiPartition(tokenRange.getValue(), tokenRange.getKey()));
    }

    return partitions.build();
  }

  /**
   * Retrieve the set of (start-token, host) pairs of a cluster sorted by start token.
   *
   * Package private for testing.
   *
   * @param session A connection to the cluster.
   * @return The set of (start-token, host) pairs.
   */
  static SortedMap<Long, InetAddress> getStartTokens(
      Session session
  ) {
    // Cassandra lets us query for the coordinator-local tokens as well as the coordinator peer
    // tokens, but they are split up into different tables. Accordingly, we have to make sure that
    // when the two queries are executed, the coordinator node is consistent.
    // For more information on Cassandra system tables:
    //    https://www.datastax.com/documentation/cql/3.0/cql/cql_using/use_query_system_c.html

    final ResultSetFuture localTokensFuture =
        session.executeAsync(select("tokens").from("system", "local"));
    final ResultSetFuture peerTokensFuture =
        session.executeAsync(select("rpc_address", "tokens").from("system", "peers"));

    final ResultSet localTokens = localTokensFuture.getUninterruptibly();
    final ResultSet peerTokens = peerTokensFuture.getUninterruptibly();

    // If this assert ever fails in practice, we may need to implement auto-retry.
    if (localTokens
        .getExecutionInfo()
        .getQueriedHost()
        .equals(peerTokens.getExecutionInfo().getQueriedHost())) {
      throw new InternalKijiError(
          "Coordinator node must be consistent across local and peer token range queries."
              + " Please retry.");
    }

    final InetAddress coordinator =
        localTokens.getExecutionInfo().getQueriedHost().getSocketAddress().getAddress();

    ImmutableSortedMap.Builder<Long, InetAddress> tokens = ImmutableSortedMap.naturalOrder();

    for (Row row : localTokens.all()) {
      for (String token : row.getSet("tokens", String.class)) {
        tokens.put(Long.parseLong(token), coordinator);
      }
    }

    for (Row row : peerTokens.all()) {
      final InetAddress peer = row.getInet("rpc_address");
      for (String token : row.getSet("tokens", String.class)) {
        tokens.put(Long.parseLong(token), peer);
      }
    }

    return tokens.build();
  }

  /**
   * Convert a set of (start-token, host) pairs into a set of (token-range, host) pairs.
   *
   * Package private for testing.
   *
   * @param startTokens The set of start tokens with hosts.
   * @return The token corresponding token ranges.
   */
  static Map<Range<Long>, InetAddress> getTokenRanges(
      final SortedMap<Long, InetAddress> startTokens
  ) {

    ImmutableMap.Builder<Range<Long>, InetAddress> tokenRangesBldr = ImmutableMap.builder();

    final PeekingIterator<Entry<Long, InetAddress>> startTokensItr =
        Iterators.peekingIterator(startTokens.entrySet().iterator());

    // Add a range for [-∞, firstStartToken) owned by the final key (the wrap-around range).
    // For more information on Casandra VNode token ranges:
    //    http://www.datastax.com/dev/blog/virtual-nodes-in-cassandra-1-2
    tokenRangesBldr.put(
        Range.lessThan(startTokens.firstKey()),
        startTokens.get(startTokens.lastKey()));

    while (startTokensItr.hasNext()) {
      Entry<Long, InetAddress> startToken = startTokensItr.next();
      if (!startTokensItr.hasNext()) {
        // The final start token
        // Add a range for [lastStartToken, ∞)
        tokenRangesBldr.put(Range.atLeast(startToken.getKey()), startToken.getValue());
      } else {
        // Add a range for [thisStartToken, nextStartToken)
        tokenRangesBldr.put(
            Range.closedOpen(startToken.getKey(), startTokensItr.peek().getKey()),
            startToken.getValue());
      }
    }

    final Map<Range<Long>, InetAddress> tokenRanges = tokenRangesBldr.build();

    // Check that the returned ranges are coherent; most importantly that all possible tokens fall
    // within the returned range set.

    if (startTokens.size() + 1 == tokenRanges.size()) {
      throw new InternalKijiError(
          String.format("Unexpected number of token ranges. start-tokens: %s, token-ranges: %s.",
              startTokens.size(), tokenRanges.size()));
    }

    final RangeSet<Long> ranges = TreeRangeSet.create();
    for (Range<Long> tokenRange : tokenRanges.keySet()) {
      ranges.add(tokenRange);
    }

    if (ranges.encloses(Range.closed(Long.MIN_VALUE, Long.MAX_VALUE))) {
      throw new InternalKijiError("Token range does not include all possible tokens.");
    }

    return tokenRanges;
  }
}

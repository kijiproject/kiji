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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * Class for InputSplits for Cassandra-backed Kiji instances.
 */
final class CassandraInputSplit extends InputSplit implements Writable {
  private List<CassandraTokenRange> mTokenRanges;
  private List<String> mHosts;

  // TODO: Is there a better answer here?
  private static final long SPLIT_LENGTH = 1L;

  /**
   * Constructor.
   */
  public CassandraInputSplit() {}

  /**
   * Creates an InputSplit from a single subsplit.
   *
   * @param subsplit from which to build an InputSplit.
   * @return the InputSplit.
   */
  public static CassandraInputSplit createFromSubplit(CassandraSubSplit subsplit) {
    return new CassandraInputSplit(
        Lists.newArrayList(
            new CassandraTokenRange(subsplit.getStartToken(), subsplit.getEndToken())),
        Lists.newArrayList(subsplit.getHosts())
    );
  }

  /**
   * Creates an InputSplit from a collection of subsplits.
   *
   * @param subsplits from which to build the input split.
   * @return the InputSplit.
   */
  public static CassandraInputSplit createFromSubplits(Collection<CassandraSubSplit> subsplits) {
    List<CassandraTokenRange> tokenRanges = Lists.newArrayList();
    Set<String> hosts = Sets.newHashSet();
    for (CassandraSubSplit subsplit : subsplits) {
      tokenRanges.add(new CassandraTokenRange(subsplit.getStartToken(), subsplit.getEndToken()));
      hosts.addAll(subsplit.getHosts());
    }
    return new CassandraInputSplit(tokenRanges, Lists.newArrayList(hosts));
  }

  /**
   * Private constructor.
   *
   * @param tokenRanges over which the InputSplit will query Cassandra.
   * @param hosts that own the data in the given token ranges.
   */
  private CassandraInputSplit(List<CassandraTokenRange> tokenRanges, List<String> hosts) {
    this.mTokenRanges = tokenRanges;
    this.mHosts = hosts;
  }

  /** {@inheritDoc} */
  @Override
  public String[] getLocations() {
    return mHosts.toArray(new String[mHosts.size()]);
  }

  /** {@inheritDoc} */
  @Override
  public long getLength() {
    return SPLIT_LENGTH;
  }

  // These three methods are for serializing and deserializing
  // KeyspaceSplits as needed by the Writable interface.

  /** {@inheritDoc} */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(mTokenRanges.size());
    for (CassandraTokenRange tokenRange : mTokenRanges) {
      out.writeLong(tokenRange.getStartToken());
      out.writeLong(tokenRange.getEndToken());
    }
    out.writeInt(mHosts.size());
    for (String endpoint : mHosts) {
      out.writeUTF(endpoint);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void readFields(DataInput in) throws IOException {
    int numTokenRanges = in.readInt();
    mTokenRanges = Lists.newArrayList();
    for (int i = 0; i < numTokenRanges; i++) {
      long startToken = in.readLong();
      long endToken = in.readLong();
      mTokenRanges.add(new CassandraTokenRange(startToken, endToken));
    }
    int numOfEndpoints = in.readInt();
    mHosts = Lists.newArrayList();
    for (int i = 0; i < numOfEndpoints; i++) {
      mHosts.add(in.readUTF());
    }
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return String.format(
        "MultiqueryInputSplit(%s)",
        mHosts
    );
  }

  /**
   * @return an Iterator over the token ranges covered by this InputSplit.
   */
  public Iterator<CassandraTokenRange> getTokenRangeIterator() {
    return mTokenRanges.iterator();
  }
}

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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.google.common.collect.AbstractIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Always tries hosts in the same order.  Useful for querying system tables.
 */
class ConsistentHostOrderPolicy implements LoadBalancingPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(ConsistentHostOrderPolicy.class);

  private final CopyOnWriteArrayList<Host> mLiveHosts = new CopyOnWriteArrayList<Host>();
  private final AtomicInteger mIndex = new AtomicInteger();

  private QueryOptions mQueryOptions;
  private volatile boolean mHasLoggedLocalCLUse;

  /**
   * Creates a load balancing policy that picks host to query in a round robin
   * fashion (on all the hosts of the Cassandra cluster).
   */
  public ConsistentHostOrderPolicy() {}

  @Override
  public void init(Cluster cluster, Collection<Host> hosts) {
    this.mLiveHosts.addAll(hosts);
    this.mQueryOptions = cluster.getConfiguration().getQueryOptions();
    this.mIndex.set(new Random().nextInt(Math.max(hosts.size(), 1)));
  }

  /**
   * Return the HostDistance for the provided host.
   * <p>
   * This policy consider all nodes as local. This is generally the right
   * thing to do in a single datacenter deployment. If you use multiple
   * datacenter, see {@link com.datastax.driver.core.policies.DCAwareRoundRobinPolicy} instead.
   *
   * @param host the host of which to return the distance of.
   * @return the HostDistance to {@code host}.
   */
  @Override
  public HostDistance distance(Host host) {
    return HostDistance.LOCAL;
  }

  /**
   * Returns the hosts to use for a new query.
   * <p>
   * The returned plan will try each known host of the cluster. Upon each
   * call to this method, the {@code i}th host of the plans returned will cycle
   * over all the hosts of the cluster in a round-robin fashion.
   *
   * @param loggedKeyspace the keyspace currently logged in on for this
   * query.
   * @param statement the query for which to build the plan.
   * @return a new query plan, i.e. an iterator indicating which host to
   * try first for querying, which one to use as failover, etc...
   */
  @Override
  public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement) {

    // We clone mLiveHosts because we want a version of the list that
    // cannot change concurrently of the query plan iterator (this
    // would be racy). We use clone() as it don't involve a copy of the
    // underlying array (and thus we rely on mLiveHosts being a CopyOnWriteArrayList).
    @SuppressWarnings("unchecked")
    final List<Host> hosts = (List<Host>) mLiveHosts.clone();
    final int startIdx = mIndex.get();

    // Overflow protection; not theoretically thread safe but should be good enough
    if (startIdx > Integer.MAX_VALUE - 10000) {
      mIndex.set(0);
    }

    return new AbstractIterator<Host>() {

      private int mIdx = startIdx;
      private int mRemaining = hosts.size();

      @Override
      protected Host computeNext() {
        if (mRemaining <= 0) {
          return endOfData();
        }

        mRemaining--;
        int c = mIdx++ % hosts.size();
        if (c < 0) {
          c += hosts.size();
        }
        return hosts.get(c);
      }
    };
  }

  @Override
  public void onUp(Host host) {
    mLiveHosts.addIfAbsent(host);
  }

  @Override
  public void onDown(Host host) {
    mLiveHosts.remove(host);
  }

  @Override
  public void onAdd(Host host) {
    onUp(host);
  }

  @Override
  public void onRemove(Host host) {
    onDown(host);
  }
}

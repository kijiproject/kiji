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

package org.kiji.schema.cassandra.util;

import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import junit.framework.Assert;
import org.junit.Test;

import org.kiji.schema.cassandra.CassandraKijiClientTest;

public class TestSessionCache {

  @Test
  public void testClustersAreCached() throws Exception {

    final Cluster cluster;

    final List<String> contactPoints = CassandraKijiClientTest.EmbeddedCassandra.getContactPoints();
    final int contactPort = CassandraKijiClientTest.EmbeddedCassandra.getContactPort();

    try (final Session session =
        SessionCache.getSession(contactPoints, contactPort, null, null)) {
      cluster = session.getCluster();

      Assert.assertFalse(cluster.isClosed());

      try (final Session sess2 = SessionCache.getSession(contactPoints, contactPort, null, null)) {
        Assert.assertEquals(cluster, sess2.getCluster());
      }
      Assert.assertFalse(cluster.isClosed());
    }
    Assert.assertTrue(cluster.isClosed());
  }
}

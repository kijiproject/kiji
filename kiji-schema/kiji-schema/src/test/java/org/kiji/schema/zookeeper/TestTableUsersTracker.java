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

package org.kiji.schema.zookeeper;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Queues;
import junit.framework.Assert;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.KillSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.impl.TestZooKeeperMonitor.QueueingUsersUpdateHandler;
import org.kiji.schema.layout.impl.ZooKeeperClient;
import org.kiji.schema.layout.impl.ZooKeeperMonitor;
import org.kiji.schema.layout.impl.ZooKeeperMonitor.UsersTracker;
import org.kiji.schema.util.ZooKeeperTest;

public class TestTableUsersTracker extends ZooKeeperTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestTableUsersTracker.class);

  private volatile CuratorFramework mZKClient;

  @Before
  public void setUp() throws Exception {
    mZKClient =
        ZooKeeperUtils.getZooKeeperClient(getZKAddress());
  }

  @After
  public void tearDown() throws Exception {
    mZKClient.close();
  }

  @Test
  public void testUsersTrackerUpdatesHandler() throws Exception {
    final KijiURI tableURI = KijiURI.newBuilder().withInstanceName("i").withTableName("t").build();
    final String userID = "user";
    final String layout1 = "layout-id-1";
    final String layout2 = "layout-id-2";

    BlockingQueue<Multimap<String, String>> usersQueue = Queues.newSynchronousQueue();
    TableUsersTracker tracker = new TableUsersTracker(mZKClient, tableURI,
        new QueueingTableUsersUpdateHandler(usersQueue));
    try {
      tracker.start();
      Assert.assertEquals(ImmutableSetMultimap.<String, String>of(),
          usersQueue.poll(5, TimeUnit.SECONDS));

      TableUserRegistration userRegistration =
          new TableUserRegistration(mZKClient, tableURI, userID);

      try {
        userRegistration.start(layout1);
        Assert.assertEquals(ImmutableSetMultimap.of(userID, layout1),
            usersQueue.poll(5, TimeUnit.SECONDS));

        // Make sure its able to handles session expiration
        KillSession.kill(mZKClient.getZookeeperClient().getZooKeeper(), getZKAddress());

        userRegistration.updateLayoutID(layout2);
        // Temporary the table user registration goes away before being replaced
        Assert.assertEquals(ImmutableSetMultimap.<String, String>of(),
            usersQueue.poll(5, TimeUnit.SECONDS));
        Assert.assertEquals(ImmutableSetMultimap.of(userID, layout2),
            usersQueue.poll(5, TimeUnit.SECONDS));
      } finally {
        userRegistration.close();
      }

      Assert.assertEquals(ImmutableSetMultimap.<String, String>of(),
          usersQueue.poll(5, TimeUnit.SECONDS));
    } finally {
      tracker.close();
    }
  }

  @Test
  public void testUserRegistrationHandlesSessionExpiration() throws Exception {
    final KijiURI tableURI = KijiURI.newBuilder().withInstanceName("i").withTableName("t").build();
    final String userID = "user";
    final String layoutID = "layout-id";

    BlockingQueue<Multimap<String, String>> usersQueue = Queues.newSynchronousQueue();

    TableUserRegistration userRegistration =
        new TableUserRegistration(mZKClient, tableURI, userID);

    try {
      userRegistration.start(layoutID);

      KillSession.kill(mZKClient.getZookeeperClient().getZooKeeper(), getZKAddress());

      TableUsersTracker tracker = new TableUsersTracker(mZKClient, tableURI,
          new QueueingTableUsersUpdateHandler(usersQueue));
      try {
        tracker.start();

        // Initially no user as registration is recovering
        Assert.assertEquals(ImmutableSetMultimap.<String, String>of(),
            usersQueue.poll(5, TimeUnit.SECONDS));

        // The registration recovers and comes back
        Assert.assertEquals(ImmutableSetMultimap.of(userID, layoutID),
            usersQueue.poll(5, TimeUnit.SECONDS));
      } finally {
        tracker.close();
      }
    } finally {
      userRegistration.close();
    }
  }

  /** Test the TableUsersTracker correctly recognizes ZooKeeperMonitor based user registrations. */
  @Test
  public void testUsersTrackerHandlesZKMTableUserRegistrations() throws Exception {

    final KijiURI tableURI = KijiURI.newBuilder().withInstanceName("i").withTableName("t").build();
    final String userID = "user";
    final String layout1 = "layout-id-1";
    final String layout2 = "layout-id-2";

    BlockingQueue<Multimap<String, String>> usersQueue = Queues.newSynchronousQueue();


    ZooKeeperMonitor zkMonitor =
        new ZooKeeperMonitor(ZooKeeperClient.getZooKeeperClient(getZKAddress()));


    TableUsersTracker tracker = new TableUsersTracker(mZKClient, tableURI,
        new QueueingTableUsersUpdateHandler(usersQueue));
    try {
      tracker.start();
      Assert.assertEquals(ImmutableSetMultimap.<String, String>of(),
          usersQueue.poll(5, TimeUnit.SECONDS));

      ZooKeeperMonitor.TableUserRegistration userRegistration =
          zkMonitor.newTableUserRegistration(userID, tableURI);

      try {
        userRegistration.updateRegisteredLayout(layout1);
        Assert.assertEquals(ImmutableSetMultimap.of(userID, layout1),
            usersQueue.poll(5, TimeUnit.SECONDS));

        // Make sure the tracker is able to handles session expiration
        KillSession.kill(mZKClient.getZookeeperClient().getZooKeeper(), getZKAddress());

        userRegistration.updateRegisteredLayout(layout2);
        // Temporary the table user registration goes away before being replaced
        Assert.assertEquals(ImmutableSetMultimap.<String, String>of(),
            usersQueue.poll(5, TimeUnit.SECONDS));
        Assert.assertEquals(ImmutableSetMultimap.of(userID, layout2),
            usersQueue.poll(5, TimeUnit.SECONDS));
      } finally {
        userRegistration.close();
      }

      Assert.assertEquals(ImmutableSetMultimap.<String, String>of(),
          usersQueue.poll(5, TimeUnit.SECONDS));
    } finally {
      tracker.close();
    }
  }

  /**
   * Test the TableUserRegistration is correctly recognized by the ZooKeeperMonitor based table user
   * tracker.
   */
  @Test
  public void testTableUserRegistrationWorksWithZKMTableUsersTracker() throws Exception {
    final KijiURI tableURI = KijiURI.newBuilder().withInstanceName("i").withTableName("t").build();
    final String userID = "user";
    final String layout1 = "layout-id-1";
    final String layout2 = "layout-id-2";

    BlockingQueue<Multimap<String, String>> usersQueue = Queues.newSynchronousQueue();

    ZooKeeperMonitor zkMonitor =
        new ZooKeeperMonitor(ZooKeeperClient.getZooKeeperClient(getZKAddress()));

    UsersTracker tracker =
        zkMonitor.newTableUsersTracker(tableURI, new QueueingUsersUpdateHandler(usersQueue));
    try {
      tracker.open();
      Assert.assertEquals(ImmutableSetMultimap.<String, String>of(),
          usersQueue.poll(5, TimeUnit.SECONDS));

      TableUserRegistration userRegistration =
        new TableUserRegistration(mZKClient, tableURI, userID);

      try {
        userRegistration.start(layout1);
        Assert.assertEquals(ImmutableSetMultimap.of(userID, layout1),
            usersQueue.poll(5, TimeUnit.SECONDS));

        // Make sure the registration handles session expiration
        KillSession.kill(mZKClient.getZookeeperClient().getZooKeeper(), getZKAddress());

        userRegistration.updateLayoutID(layout2);
        // Temporary the table user registration goes away before being replaced
        Assert.assertEquals(ImmutableSetMultimap.<String, String>of(),
            usersQueue.poll(5, TimeUnit.SECONDS));
        Assert.assertEquals(ImmutableSetMultimap.of(userID, layout2),
            usersQueue.poll(5, TimeUnit.SECONDS));
      } finally {
        userRegistration.close();
      }
      Assert.assertEquals(ImmutableSetMultimap.<String, String>of(),
          usersQueue.poll(5, TimeUnit.SECONDS));
    } finally {
      tracker.close();
    }
  }

  public static final class QueueingTableUsersUpdateHandler implements TableUsersUpdateHandler {

    private final BlockingQueue<Multimap<String, String>> mUsersQueue;

    public QueueingTableUsersUpdateHandler(BlockingQueue<Multimap<String, String>> usersQueue) {
      mUsersQueue = usersQueue;
    }

    @Override
    public void update(Multimap<String, String> users) {
      try {
        LOG.debug("Received table users update: {}.", users);
        mUsersQueue.put(users);
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while attempting to enqueue table users update.");
        Thread.currentThread().interrupt();
      }
    }
  }
}

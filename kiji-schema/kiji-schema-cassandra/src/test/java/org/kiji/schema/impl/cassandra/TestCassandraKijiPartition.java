package org.kiji.schema.impl.cassandra;

import java.io.IOException;
import java.net.InetAddress;

import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Range;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.cassandra.CassandraKijiClientTest;

public class TestCassandraKijiPartition extends CassandraKijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestCassandraKijiPartition.class);

  @Test
  public void testGetTokens() throws IOException {
    final CassandraKiji kiji = getKiji();
    final Session session = kiji.getCassandraAdmin().getSession();
    System.out.println(CassandraKijiPartition.getStartTokens(session));
  }

  @Test
  public void testTokenRanges() throws IOException {

    final InetAddress address1 = InetAddress.getByAddress(new byte[] {1, 1, 1, 1});
    final InetAddress address2 = InetAddress.getByAddress(new byte[] {2, 2, 2, 2});
    final InetAddress address3 = InetAddress.getByAddress(new byte[] {3, 3, 3, 3});

    Assert.assertEquals(
        ImmutableMap.of(
            Range.lessThan(-100L), address3,
            Range.closedOpen(-100L, 0L), address1,
            Range.closedOpen(0L, 100L), address2,
            Range.atLeast(100L), address3),

        CassandraKijiPartition.getTokenRanges(
            ImmutableSortedMap.of(
                -100L, address1,
                0L, address2,
                100L, address3)));
  }
}

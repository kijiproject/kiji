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

package org.kiji.commons;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.junit.Assert;
import org.junit.Test;

public class TestSocketAddressUtils {

  @Test
  public void testLocalToPublicAnyAddress() throws UnknownHostException {
    final InetAddress anyAddress = InetAddress.getByAddress(new byte[] {0, 0, 0, 0});
    final InetSocketAddress address = new InetSocketAddress(anyAddress, 80);
    final InetSocketAddress publicAddress = SocketAddressUtils.localToPublic(address);

    Assert.assertFalse("0.0.0.0".equals(publicAddress.getHostName()));
    Assert.assertFalse("localhost".equals(publicAddress.getHostName()));
    Assert.assertSame(80, publicAddress.getPort());
  }

  @Test
  public void testLocalToPublicLoopbackAddress() throws UnknownHostException {
    final InetAddress loopbackAddress = InetAddress.getLoopbackAddress();
    final InetSocketAddress address = new InetSocketAddress(loopbackAddress, 80);
    final InetSocketAddress publicAddress = SocketAddressUtils.localToPublic(address);

    Assert.assertFalse("0.0.0.0".equals(publicAddress.getHostName()));
    Assert.assertFalse("localhost".equals(publicAddress.getHostName()));
    Assert.assertSame(80, publicAddress.getPort());
  }

  @Test
  public void testLocalToPublicUnresolvedAddress() throws UnknownHostException {
    final InetSocketAddress address = InetSocketAddress.createUnresolved("www.example.com", 80);
    final InetSocketAddress publicAddress = SocketAddressUtils.localToPublic(address);

    Assert.assertEquals("www.example.com", publicAddress.getHostName());
    Assert.assertSame(80, publicAddress.getPort());
  }


  @Test
  public void testLocalToPublicOtherAddress() throws UnknownHostException {
    final InetAddress address = InetAddress.getByName("www.example.com");
    final InetSocketAddress socketAddress = new InetSocketAddress(address, 80);
    final InetSocketAddress publicAddress = SocketAddressUtils.localToPublic(socketAddress);

    Assert.assertEquals("www.example.com", publicAddress.getHostName());
    Assert.assertSame(80, publicAddress.getPort());
  }
}

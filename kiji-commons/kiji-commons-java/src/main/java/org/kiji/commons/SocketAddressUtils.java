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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility functions for working with socket addresses.
 */
public final class SocketAddressUtils {
  private static final Logger LOG = LoggerFactory.getLogger(SocketAddressUtils.class);

  /**
   * Attempts to convert the provided InetSocketAddress from a local address to an address with a
   * publicly resolvable hostname. If the address is not a local address, or the local hostname is
   * not available, then the original address is returned.
   *
   * @param socketAddress The address to convert.
   * @return A converted address, or the original address.
   */
  public static InetSocketAddress localToPublic(InetSocketAddress socketAddress) {
    final InetAddress address = socketAddress.getAddress();
    if (address != null && (address.isAnyLocalAddress() || address.isLoopbackAddress())) {

      InetAddress localAddress;

      try {
        localAddress = InetAddress.getLocalHost();
      } catch (UnknownHostException e) {
        LOG.error("Unable to resolve local hostname. Returning original address.", address);
        return socketAddress;
      }

      return new InetSocketAddress(localAddress, socketAddress.getPort());
    } else {
      return socketAddress;
    }
  }

  /**
   * Get the publicly resolvable hostname of the current host machine.
   *
   * @return The publicly resolvable hostname of the current host machine.
   */
  public static InetAddress getPublicLocalHost() {
    return localToPublic(new InetSocketAddress(0)).getAddress();
  }

  /** Private constructor for utility class. */
  private SocketAddressUtils() { }
}

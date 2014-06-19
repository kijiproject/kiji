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

package org.kiji.rest.discovery;

import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.List;

import com.google.common.collect.Lists;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.lifecycle.Managed;
import com.yammer.dropwizard.lifecycle.ServerLifecycleListener;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceInstanceBuilder;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiURI;
import org.kiji.schema.util.JvmId;
import org.kiji.schema.zookeeper.ZooKeeperUtils;

/**
 * A managed registration manager which allows this REST service to be registered as a service. Will
 * automatically register this REST service when the Jetty server starts up.
 */
public class RestServiceRegistrar implements Managed{
  private static final Logger LOG = LoggerFactory.getLogger(RestServiceRegistrar.class);

  private static final String BASE_PATH = "/kiji/services";
  private static final String REST_SERVICE_NAME = "kiji-rest";
  private static final String REST_ADMIN_SERVICE_NAME = "kiji-rest-admin";

  private final CuratorFramework mZKClient;
  private final ServiceDiscovery<RestServiceMetadata> mServiceDiscovery;

  /**
   * Create a RestServiceRegistrar to register this REST instance with the Kiji service discovery
   * framework.
   *
   * @param clusterURI of cluster being served by this REST instance.
   * @param environment of the REST server.
   */
  public RestServiceRegistrar(final KijiURI clusterURI, final Environment environment) {
    mZKClient = ZooKeeperUtils.getZooKeeperClient(clusterURI);

    environment.addServerLifecycleListener(new RestServiceRegistrationListener());

    JsonInstanceSerializer<RestServiceMetadata> serializer =
        new JsonInstanceSerializer<RestServiceMetadata>(RestServiceMetadata.class);

    mServiceDiscovery =
        ServiceDiscoveryBuilder
            .builder(RestServiceMetadata.class)
            .client(mZKClient)
            .basePath(BASE_PATH)
            .serializer(serializer)
            .build();
  }

  /**
   * Register a Rest service with the discovery framework.
   *
   * @param serviceInstance to register.
   * @throws Exception on serialization or ZooKeeper error.
   */
  public void registerRestService(
      final ServiceInstance<RestServiceMetadata> serviceInstance
  ) throws Exception {
    mServiceDiscovery.registerService(serviceInstance);
  }

  /** {@inheritDoc} */
  @Override
  public void start() throws Exception {
    mServiceDiscovery.start();
  }

  /** {@inheritDoc} */
  @Override
  public void stop() throws Exception {
    try {
      mServiceDiscovery.close();
    } finally {
      mZKClient.close();
    }
  }

  /**
   * Listens for the Jetty server to connect, and registers a Kiji REST service with the service
   * discovery framework.  This may only happen once Jetty connects, because otherwise we can not
   * know the port that will be used.
   */
  private final class RestServiceRegistrationListener implements ServerLifecycleListener {

    /**
     * The name of the main application connector. See
     * {@link com.yammer.dropwizard.config.ServerFactory} for details.
     *
     * *NOTE* this is changed in DropWizard 0.7 to "application" as defined in
     * {@code io.dropwizard.server.DefaultServerFactory}
     */
    static final String MAIN_CONNECTOR = "main";

    /**
     * The name of the admin application connector. See
     * {@link com.yammer.dropwizard.config.ServerFactory} for details.
     */
    static final String ADMIN_CONNECTOR = "internal";

    /** {@inheritDoc} */
    @Override
    public void serverStarted(final Server server) {
      /*
        A note on hostnames and IPs:

        Determining the hostname and/or IP address of a server is challenging. We use three
        methods, in order of most to least confidence of correctness:

        1) Get the hostname or IP from the configuration (accessed through the connector below).
           This is only non-null when there is a `bindHost` entry in configuration.yml (see
           http://dropwizard.readthedocs.org/en/latest/manual/configuration.html). This should be
           set whenever REST is running on a server with two public (non-loopback) IP addresses,
           and thus we consider it the truth, if it is available.

        2) Get the hostname from the #getAddress() method below.  This method looks at all
           non-loopback IP addresses of the machine. If there is more than one IP address, we
           arbitrarily pick the first.  We will fallback to use this method if `bindHost` is not
           specified.

        3) Get the machine's hostname through `InetAddress.getLocalHost().getHostName()`.  This is
           the method that Schema uses to retrieve hostnames to create JVM ID's (which are used
           in table and instance user registrations).  There is no guarantee that the hostname
           returned by this method is any use for external machines, as it may not be resolvable
           through DNS.  Thus, this method is potentially quite unreliable.

        Kiji REST service metadata includes both an `address` field (the built-in Curator service
        discovery field), as well as a `hostname` field.  The intent is to use the `address` field
        for the externally reachable address of the service, whereas `hostname` will hold the
        hostname of the machine the service is running on, even if it is not resolvable through
        DNS.  Thus, `address` should be used for connecting, whereas `hostname` should only be
        used for debugging / internationally.
      */
      for (final Connector connector : server.getConnectors()) {

        final String service;
        if (connector.getName().equals(MAIN_CONNECTOR)) {
          service = REST_SERVICE_NAME;
        } else if (connector.getName().equals(ADMIN_CONNECTOR)) {
          service = REST_ADMIN_SERVICE_NAME;
        } else {
          LOG.warn("Unknown connector encountered: {}.", connector.getName());
          continue;
        }

        final int port = connector.getLocalPort();
        final String bindHost = connector.getHost();
        final int pid = JvmId.getPid();

        // Use the configured host if available. Otherwise use one of the machine's IP address.
        String address = bindHost == null ? getAddress() : bindHost;

        String hostname;
        try {
          hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
          LOG.warn("Unable to lookup hostname. {}", e.getMessage());
          hostname = address;
        }

        try {
          final ServiceInstance<RestServiceMetadata> serviceInstance =
              ServiceInstance
                  .<RestServiceMetadata>builder()
                  .name(service)
                  .address(address)
                  .port(port)
                  .payload(new RestServiceMetadata(hostname, pid))
                  .build();

          registerRestService(serviceInstance);
        } catch (Exception e) {
          LOG.error("Cannot register Kiji REST service: {}", e.getMessage());
        }
      }
    }
  }

  /**
   * Get the IP address of this host.
   *
   * @return the IP address of this host.
   */
  private static String getAddress() {
    try {
      final Iterable<InetAddress> ips = ServiceInstanceBuilder.getAllLocalIPs();
      List<String> addresses = Lists.newArrayList();

      for (InetAddress ip : ips) {
        final String hostAddress =  ip.getHostAddress();
        if (hostAddress != null && !hostAddress.isEmpty()) {
          addresses.add(hostAddress);
        }
      }

      final String address;
      if (addresses.isEmpty()) {
        LOG.error("Found no local IP addresses.");
        address = null;
      } else if (addresses.size() == 1) {
        address = addresses.get(0);
        LOG.debug("Using local address {}.", address);
      } else {
        address = addresses.get(0);
        LOG.info("Found local addresses: {}. Using {}.", addresses, address);
      }
      return address;
    } catch (SocketException e) {
      LOG.warn("Unable to retrieve local addresses: {}", e.getMessage());
      return null;
    }
  }
}

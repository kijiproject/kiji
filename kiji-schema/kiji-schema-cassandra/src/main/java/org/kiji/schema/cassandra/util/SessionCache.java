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

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;

import com.datastax.driver.core.AbstractSession;
import com.datastax.driver.core.CloseFuture;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.commons.ReferenceCountedCache;
import org.kiji.schema.cassandra.CassandraKijiURI;

/**
 * A cache to hold open connections to Cassandra clusters.
 */
public final class SessionCache {
  private static final Logger LOG = LoggerFactory.getLogger(SessionCache.class);

  /** Global cache of Cassandra connections keyed on ensemble addresses. */
  private static final ReferenceCountedCache<ClusterKey, Entry> CACHE =
      ReferenceCountedCache.createWithResourceTracking(
          new Function<ClusterKey, Entry>() {
            /** {@inheritDoc}. */
            @Override
            public Entry apply(ClusterKey instanceURI) {
              final Cluster cluster = createConnectionToInstance(instanceURI);
              final Session session = cluster.connect();
              return new Entry(cluster, session);
            }
          });

  /**
   * Create a new connection to a Cassandra cluster.
   *
   * @param clusterKey The cluster key which defines the contact points, ports, and authentication
   *    parameters for the connection.
   * @return A new cluster connection.
   */
  private static Cluster createConnectionToInstance(
      final ClusterKey clusterKey
  ) {
    LOG.info("Creating new cluster connection with key: {}", clusterKey);
    final Set<String> contactPoints = clusterKey.getContactPoints();
    final Builder clusterBuilder = Cluster
        .builder()
        .addContactPoints(contactPoints.toArray(new String[contactPoints.size()]))
        .withPort(clusterKey.getContactPort());

    if (clusterKey.getUsername() != null) {
      clusterBuilder.withAuthProvider(
          new PlainTextAuthProvider(clusterKey.getUsername(), clusterKey.getPassword()));
    }

    return clusterBuilder.build();
  }

  /**
   * Get a session to the Cassandra cluster of a Kiji URI.
   *
   * <p>
   *    The caller *must* close the session after it will no longer be used.  The caller *must not*
   *    close the connection backing the session.
   * </p>
   *
   * @param clusterURI The cluster URI to return a connection to.
   * @return An open session for the cluster.
   */
  public static Session getSession(
      final CassandraKijiURI clusterURI
  ) {
    return getSession(
        new ClusterKey(
            clusterURI.getContactPoints(),
            clusterURI.getContactPort(),
            clusterURI.getUsername(),
            clusterURI.getPassword()));
  }

  /**
   * Get an open session to the Cassandra cluster with the given parameters.
   *
   * <p>
   *    The caller *must* close the session after it will no longer be used.  The caller *must not*
   *    close the connection backing the session.
   * </p>
   *
   * @param contactPoints The Cassandra contact points.
   * @param contactPort The Cassandra contact port.
   * @param username The simple authentication username, or null if no authentication.
   * @param password The simple authentication password, or null if no authentication.
   * @return An open session to the cluster.
   */
  public static Session getSession(
      final Collection<String> contactPoints,
      final int contactPort,
      final String username,
      final String password
  ) {
    return getSession(new ClusterKey(contactPoints, contactPort, username, password));
  }

  /**
   * Get an open session to the Cassandra cluster with the given cluster key.
   *
   * @param key The Cassandra cluster information.
   * @return An open session.
   */
  private static Session getSession(final ClusterKey key) {
    LOG.debug("Retrieving cached session with key: {}.", key);
    return new CachedSession(key, CACHE.get(key).getSession());
  }

  /** Private utility constructor. */
  private SessionCache() { }

  /**
   * A cache entry containing a Cassandra cluster and session.
   */
  private static final class Entry implements Closeable {
    private final Cluster mCluster;
    private final Session mSession;

    /**
     * Create a new cache entry.
     *
     * @param cluster The cached cluster.
     * @param session The cached session.
     */
    public Entry(final Cluster cluster, final Session session) {
      mCluster = cluster;
      mSession = session;
    }

    /**
     * Get the session.
     *
     * @return The session.
     */
    public Session getSession() {
      return mSession;
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
      mCluster.close(); // Closing the cluster will close all attached sessions
    }
  }

  /**
   * A key which defines the connection parameters for a Cassandra cluster.
   */
  private static final class ClusterKey {
    private final Set<String> mContactPoints;
    private final int mContactPort;
    private final String mUsername;
    private final String mPassword;

    /**
     * Create a new cluster key.
     *
     * @param contactPoints The Cassandra contact points.
     * @param contactPort The Cassandra contact port.
     * @param username The simple authentication username, or null if no authentication.
     * @param password The simple authentication password, or null if no authentication.
     */
    private ClusterKey(
        final Iterable<String> contactPoints,
        final int contactPort,
        final String username,
        final String password
    ) {
      mContactPoints = ImmutableSet.copyOf(contactPoints);
      mContactPort = contactPort;
      mUsername = username;
      mPassword = password;
    }

    /**
     * Get the Cassandra contact points.
     *
     * @return The Cassandra contact points.
     */
    public Set<String> getContactPoints() {
      return mContactPoints;
    }

    /**
     * Get the Cassandra contact port.
     *
     * @return The Cassandra contact port.
     */
    public int getContactPort() {
      return mContactPort;
    }

    /**
     * Get the simple authentication username.
     *
     * @return The simple authentication username, or null if no authentication.
     */
    public String getUsername() {
      return mUsername;
    }

    /**
     * Get the simple authentication password.
     *
     * @return The simple authentication password, or null if no authentication.
     */
    public String getPassword() {
      return mPassword;
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
      return Objects.hash(mContactPoints, mContactPort, mUsername, mPassword);
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      final ClusterKey other = (ClusterKey) obj;
      return Objects.equals(this.mContactPoints, other.mContactPoints)
          && Objects.equals(this.mContactPort, other.mContactPort)
          && Objects.equals(this.mUsername, other.mUsername)
          && Objects.equals(this.mPassword, other.mPassword);
    }

    @Override
    public String toString() {
      return com.google.common.base.Objects.toStringHelper(this)
          .add("contact-points", mContactPoints)
          .add("contact-port", mContactPort)
          .add("username", mUsername)
          .add("password", mPassword)
          .toString();
    }
  }

  /**
   * A Cassandra session that will release a reference count from the cache on close.
   */
  private static final class CachedSession extends AbstractSession {
    private final ClusterKey mKey;
    private final Session mProxy;

    /**
     * Create a new cached session that will release the cluster key from the cache when closed.
     *
     * @param key The cluster key to cache.
     * @param proxy The proxy session to proxy all calls to.
     */
    private CachedSession(final ClusterKey key, final Session proxy) {
      mKey = key;
      mProxy = proxy;
    }

    /** {@inheritDoc} */
    @Override
    public String getLoggedKeyspace() {
      return mProxy.getLoggedKeyspace();
    }

    /** {@inheritDoc} */
    @Override
    public Session init() {
      return mProxy.init();
    }

    /** {@inheritDoc} */
    @Override
    public ResultSetFuture executeAsync(final Statement statement) {
      return mProxy.executeAsync(statement);
    }

    /** {@inheritDoc} */
    @Override
    public ListenableFuture<PreparedStatement> prepareAsync(final String query) {
      return mProxy.prepareAsync(query);
    }

    /** {@inheritDoc} */
    @Override
    public CloseFuture closeAsync() {
      try {
        CACHE.release(mKey);

        // CloseFuture#immediateFuture is package private, so reflection is used to call it.
        final Method method = CloseFuture.class.getDeclaredMethod("immediateFuture");
        AccessController.doPrivileged(
            new PrivilegedAction<Object>() {
              @Override
              public Object run() {
                method.setAccessible(true);
                return null;
              }
            });
        return (CloseFuture) method.invoke(null);
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    /** {@inheritDoc} */
    @Override
    public boolean isClosed() {
      return mProxy.isClosed();
    }

    /** {@inheritDoc} */
    @Override
    public Cluster getCluster() {
      return mProxy.getCluster();
    }

    /** {@inheritDoc} */
    @Override
    public State getState() {
      return mProxy.getState();
    }
  }
}

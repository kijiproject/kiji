/**
 * (c) Copyright 2013 WibiData, Inc.
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

package org.kiji.rest;

import java.io.IOException;
import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.util.StringUtils;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiURI;
import org.kiji.schema.hbase.HBaseFactory;
import org.kiji.schema.util.ResourceUtils;

/**
 * Utility class for listing instances on a cluster.
 */
public final class InstanceUtil {

  /**
   * Disable default constructor for utility class.
   */
  private InstanceUtil() {
  }

  /**
   * Parses an HBase table name for a kiji instance name.
   *
   * @param hBaseTableName The HBase table name to parse
   * @return instance name (or null if none found)
   */
  private static String parseInstanceName(final String hBaseTableName) {
    final String[] parts = StringUtils.split(hBaseTableName, '\u0000', '.');
    if (parts.length < 3 || !KijiURI.KIJI_SCHEME.equals(parts[0])) {
      return null;
    }
    return parts[1];
  }

  /**
   * Acquires the set of Kiji instance names by scanning the list of HBase tables looking for those
   * associated with a Kiji instance. Then performs a per-row mapTo operation on each instance URI.
   * The set of mapTo-transformed instance URIs is returned. If the mapTo operation produces a null,
   * the corresponding row is filtered out.
   *
   * For example, the following code retrieves the instance names as a set of strings:
   *
   * <code>
   * InstanceUtil.getInstances(
   *     KijiURI.newBuilder("kiji://.env").build(),
   *     new InstancesMapTo<String>() {
   *       public String apply(KijiURI instanceURI) {
   *         return instanceURI.getInstance();
   *       }
   *     });
   * </code>
   *
   * @param <T> type of the returned value of the mapTo.
   * @param hbaseURI the URI of the cluster.
   * @param mapTo the per-row operation defined as an anonymous class with an apply method.
   *     If this returns null, then the row is ignored.
   * @return set of transformed rows.
   * @throws IOException if the cluster can not be queries for a list of instances.
   */
  public static <T> Set<T> getInstances(
      final KijiURI hbaseURI,
      final InstancesMapTo<T> mapTo) throws IOException {
    final Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.ZOOKEEPER_QUORUM,
        Joiner.on(",").join(hbaseURI.getZookeeperQuorumOrdered()));
    conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, hbaseURI.getZookeeperClientPort());
    final HBaseAdmin hbaseAdmin =
        HBaseFactory.Provider.get().getHBaseAdminFactory(hbaseURI).create(conf);

    try {
      final Set<T> mappedToInstances = Sets.newHashSet();
      for (HTableDescriptor hTableDescriptor : hbaseAdmin.listTables()) {
        final String instanceName = parseInstanceName(hTableDescriptor.getNameAsString());
        if (null != instanceName) {
          T mappedToInstance = mapTo.apply(getInstanceURI(hbaseURI, instanceName));
          if (null != mappedToInstance) {
            mappedToInstances.add(mappedToInstance);
          }
        }
      }
      return mappedToInstances;
    } finally {
      ResourceUtils.closeOrLog(hbaseAdmin);
    }
  }

  /**
   * Returns a set of instance names.
   *
   * @param hbaseURI URI of the HBase instance to list the content of.
   * @return set of instance names.
   * @throws IOException on I/O error.
   */
  public static Set<String> getInstanceNames(final KijiURI hbaseURI) throws IOException {
    return getInstances(
        hbaseURI,
        new InstancesMapTo<String>() {
          public String apply(KijiURI instanceURI) throws IOException {
            return instanceURI.getInstance();
          }
        });
  }

  /**
   * Returns a set of instance URIs.
   *
   * @param hbaseURI URI of the HBase instance to list the content of.
   * @return set of instance URIs.
   * @throws IOException on I/O error.
   */
  public static Set<KijiURI> getInstanceURIs(final KijiURI hbaseURI) throws IOException {
    return getInstances(
        hbaseURI,
        new InstancesMapTo<KijiURI>() {
          public KijiURI apply(KijiURI instanceURI) throws IOException {
            return instanceURI;
          }
        });
  }

  /**
   * Open and close an instance to check that it is available.
   * Throw an exception if the instance is not available.
   *
   * @param instanceURI the URI of the instance.
   * @throws IOException when an instance can not be opened or closed.
   */
  public static void openAndCloseInstance(final KijiURI instanceURI) throws IOException {
    // Check existence of instance by opening and closing.
    final Configuration conf = new Configuration();
    conf.setInt("hbase.client.retries.number", 3);
    final Kiji kiji = Kiji.Factory.open(instanceURI, conf);
    kiji.release();
  }

  /**
   * Builds instance URI from cluster URI and instance name.
   *
   * @param clusterURI the URI of the cluster.
   * @param instanceName the name of the instance.
   * @return the instance URI.
   */
  private static KijiURI getInstanceURI(final KijiURI clusterURI, final String instanceName) {
    return KijiURI.newBuilder(clusterURI).withInstanceName(instanceName).build();
  }

  /**
   * Interface for defining generic-output functions on Kiji instances.
   *
   * @param <T> type of output.
   */
  interface InstancesMapTo<T> {
    /**
     * Apply method to instance URI.
     *
     * @param instanceURI of the instance to run this method on.
     * @return the required output value.
     * @throws IOException if the apply fails (allowance for a wide range of Kiji operations).
     */
    T apply(final KijiURI instanceURI) throws IOException;
  }
}

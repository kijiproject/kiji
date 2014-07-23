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

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiURI;
import org.kiji.schema.cassandra.CassandraKijiURI;
import org.kiji.schema.testutil.AbstractKijiIntegrationTest;

/**
 * Creates an appropriate URI for a Cassandra Kiji integration test.
 *
 * Based on {@link org.kiji.schema.cassandra.AbstractCassandraKijiIntegrationTest}.
 */
public final class CassandraKijiMapReduceIntegrationTestUtil {
  private static final Logger LOG =
      LoggerFactory.getLogger(CassandraKijiMapReduceIntegrationTestUtil.class);

  /** Private constructor for utility class. */
  private CassandraKijiMapReduceIntegrationTestUtil() { }

  /** Property a user can specify with a URI for running tests with external cluster. */
  private static final String BASE_TEST_URI_PROPERTY = "kiji.test.cluster.uri";

  /** Counter that ensures that every new URI points to a unique Kiji instance. */
  private static AtomicInteger mKijiCounter = new AtomicInteger();

  /**
   * Create and return a globally unique KijiURI for a Cassandra Kiji instance.
   *
   * Reads the IP address and native port for the Cassandra cluster from a file
   * `cassandra-maven-plugin.properties`, assumed to be on the classpath.  Does not actually install
   * the instance, just creates the URI.
   *
   * @return A globally unique URI for a Cassandra Kiji instance.
   */
  public static CassandraKijiURI getGloballyUniqueCassandraKijiUri() {

    CassandraKijiURI cassandraKijiURI;
    final Configuration conf = HBaseConfiguration.create();

    if (System.getProperty(BASE_TEST_URI_PROPERTY) != null) {
      final KijiURI kijiURI =
          KijiURI.newBuilder(System.getProperty(BASE_TEST_URI_PROPERTY)).build();
      if (!(kijiURI instanceof CassandraKijiURI)) {
        throw new IllegalArgumentException(
            String.format(
                "User-specified cluster URI %s is not a Cassandra URI.",
                System.getProperty(BASE_TEST_URI_PROPERTY)
            ));
      }
      cassandraKijiURI = (CassandraKijiURI) kijiURI;

    } else {
      Properties properties = new Properties();
      try {
        // Read the IP address and port from a filtered .properties file.
        InputStream input = AbstractKijiIntegrationTest.class
            .getClassLoader()
            .getResourceAsStream("cassandra-maven-plugin.properties");
        properties.load(input);
        input.close();
        LOG.info(
            "Successfully loaded Cassandra Maven plugin properties from file: ",
            properties.toString()
        );
      } catch (IOException ioe) {
        throw new KijiIOException(
            "Problem loading cassandra-maven-plugin.properties file from the classpath.");
      }

      String instanceName = "it" + mKijiCounter.getAndIncrement();
      // Create a Kiji instance.
      cassandraKijiURI = CassandraKijiURI.newBuilder(
          String.format(
              "kiji-cassandra://%s:%s/%s:%s/%s",
              conf.get(HConstants.ZOOKEEPER_QUORUM),
              conf.getInt(
                  HConstants.ZOOKEEPER_CLIENT_PORT,
                  HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT),
              properties.getProperty("cassandra.initialIp"),
              properties.getProperty("cassandra.nativePort"),
              instanceName
          )).build();
      LOG.info("Base URI for Cassandra integration tests = ", cassandraKijiURI.toString());
    }
    return cassandraKijiURI;
  }
}

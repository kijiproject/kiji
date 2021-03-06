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

package org.kiji.schema.impl.cassandra;

import java.io.IOException;
import java.util.Map;

import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiAlreadyExistsException;
import org.kiji.schema.KijiInstaller;
import org.kiji.schema.KijiInvalidNameException;
import org.kiji.schema.KijiURI;
import org.kiji.schema.cassandra.CassandraKijiURI;
import org.kiji.schema.hbase.HBaseFactory;

/** Installs or uninstalls Kiji instances from an Cassandra cluster. */
@ApiAudience.Public
@ApiStability.Evolving
public final class CassandraKijiInstaller extends KijiInstaller {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraKijiInstaller.class);
  /** Singleton KijiInstaller. **/
  private static final CassandraKijiInstaller SINGLETON = new CassandraKijiInstaller();

  /** Constructs a CassandraKijiInstaller. */
  private CassandraKijiInstaller() {
    super();
  }

  /** {@inheritDoc} */
  @Override
  public void install(
      KijiURI uri,
      HBaseFactory hbaseFactory,
      Map<String, String> properties,
      Configuration conf
    ) throws IOException {
    Preconditions.checkArgument(uri instanceof CassandraKijiURI,
        "Kiji URI for a new Cassandra Kiji installation must be a CassandraKijiURI: '{}'.", uri);
    final CassandraKijiURI cassandraURI = (CassandraKijiURI) uri;
    if (cassandraURI.getInstance() == null) {
      throw new KijiInvalidNameException(String.format(
          "Kiji URI '%s' does not specify a Kiji instance name", cassandraURI));
    }
    try {
      LOG.info(String.format("Installing Cassandra Kiji instance '%s'.", cassandraURI));

      try (CassandraAdmin admin = CassandraAdmin.create(cassandraURI)) {
        // Install the system, meta, and schema tables.
        CassandraSystemTable.install(admin, cassandraURI, properties);
        CassandraMetaTable.install(admin, cassandraURI);
        CassandraSchemaTable.install(admin, cassandraURI);
      }

      // Grant the current user all privileges on the instance just created, if security is enabled.
      final Kiji kiji = CassandraKijiFactory.get().open(cassandraURI);
      try {
        if (kiji.isSecurityEnabled()) {
          throw new UnsupportedOperationException("Kiji Cassandra does not implement security.");
        }
      } finally {
        kiji.release();
      }
    } catch (AlreadyExistsException aee) {
      throw new KijiAlreadyExistsException(String.format(
          "Cassandra Kiji instance '%s' already exists.", cassandraURI), cassandraURI);
    }
    LOG.info(String.format("Installed Cassandra Kiji instance '%s'.", cassandraURI));
  }

  /** {@inheritDoc} */
  @Override
  public void uninstall(
      KijiURI uri,
      HBaseFactory hbaseFactory,
      Configuration conf
  ) throws IOException {
    Preconditions.checkArgument(uri instanceof CassandraKijiURI,
        "Kiji URI for a new Cassandra Kiji installation must be a CassandraKijiURI: '{}'.", uri);
    final CassandraKijiURI cassandraURI = (CassandraKijiURI) uri;
    if (cassandraURI.getInstance() == null) {
      throw new KijiInvalidNameException(String.format(
          "Kiji URI '%s' does not specify a Kiji instance name", cassandraURI));
    }

    LOG.info(String.format("Uninstalling Kiji instance '%s'.", cassandraURI.getInstance()));

    final Kiji kiji = CassandraKijiFactory.get().open(cassandraURI);
    try {
      // TODO (SCHEMA-706): Add security checks when we have a plan for security in Cassandra Kiji.

      for (String tableName : kiji.getTableNames()) {
        LOG.info("Deleting Kiji table {}.", tableName);
        kiji.deleteTable(tableName);
      }
      // Delete the user tables:
      try (CassandraAdmin admin = CassandraAdmin.create(cassandraURI)) {
        // Delete the system tables:
        CassandraSystemTable.uninstall(admin, cassandraURI);
        CassandraMetaTable.uninstall(admin, cassandraURI);
        CassandraSchemaTable.uninstall(admin, cassandraURI);
        admin.deleteKeyspace();
      }

    } finally {
      kiji.release();
    }
    LOG.info(String.format("Kiji instance '%s' uninstalled.", cassandraURI.getInstance()));
  }

  /**
   * Gets an instance of a CassandraKijiInstaller.
   *
   * @return An instance of a CassandraKijiInstaller.
   */
  public static CassandraKijiInstaller get() {
    return SINGLETON;
  }
}

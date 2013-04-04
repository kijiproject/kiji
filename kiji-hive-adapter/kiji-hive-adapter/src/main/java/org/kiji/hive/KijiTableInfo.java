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

package org.kiji.hive;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.impl.HBaseKijiTable;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.ResourceUtils;

/**
 * Contains all the information about a kiji table relevant to using hive.
 */
public class KijiTableInfo implements Closeable {
  public static final String KIJI_TABLE_URI = "kiji.table.uri";

  private final KijiURI mKijiURI;

  private Connection mConnection;

  /**
   * Constructor.
   *
   * @param properties The Hive table properties.
   */
  public KijiTableInfo(Properties properties) {
    mKijiURI = getURIFromProperties(properties);
    mConnection = null;
  }

  /**
   * Gets the URI from the passed in properties.
   * @param properties for the job.
   * @return KijiURI extracted from the passed in properties.
   */
  public static KijiURI getURIFromProperties(Properties properties) {
    String kijiURIString = properties.getProperty(KIJI_TABLE_URI);
    //TODO Pretty exceptions for URI parser issues.
    KijiURI kijiURI = KijiURI.newBuilder(kijiURIString).build();

    //TODO Ensure that this URI has a table component.
    return kijiURI;
  }

  /**
   * Gets the URI for this KijiTableInfo.
   *
   * @return KijiURI associated with this KijiTableINfo
   */
  public KijiURI getKijiTableURI() {
    return mKijiURI;
  }

  /**
   * Gets the Kiji schema table associated with the connection.
   *
   * @return the KijiSchemaTable associated with this connection.
   * @throws IOException if there is an error.
   */
  public KijiSchemaTable getSchemaTable() throws IOException {
    return getConnection().getSchemaTable();
  }

  /**
   * Gets the Kiji table layout.
   *
   * @return The table layout.
   * @throws IOException If it cannot be read.
   */
  public KijiTableLayout getTableLayout() throws IOException {
    return getConnection().getTableLayout();
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    IOUtils.closeQuietly(mConnection);
  }

  /**
   * Gets a connection to the Kiji table.
   *
   * @return A connection.
   * @throws IOException If the connection cannot be established.
   */
  private Connection getConnection() throws IOException {
    if (null == mConnection) {
      mConnection = new Connection(mKijiURI);
    }
    return mConnection;
  }

  /**
   * Connection to a Kiji table and its metadata.
   */
  private static class Connection implements Closeable {
    /** The Kiji connection. */
    private final Kiji mKiji;
    /** The Kiji table connection. */
    private final KijiTable mKijiTable;

    /**
     * Opens a connection.
     * @param kijiURI The kijiURI
     * @throws IOException If there is a connection error.
     */
    public Connection(KijiURI kijiURI)
        throws IOException {
      final Configuration conf = HBaseConfiguration.create();

      mKiji = Kiji.Factory.open(kijiURI);
      mKijiTable = mKiji.openTable(kijiURI.getTable());
    }

    /**
     * Gets a deep copy of the schema table for this connection so that it can be used without a
     * Kiji connection.
     *
     * @return The schema table.
     * @throws IOException if there was an error copying the schema table.
     */
    public KijiSchemaTable getSchemaTable() throws IOException {
      KijiSchemaTable schemaTable =  new DeepCopiedReadOnlySchemaTable(mKiji.getSchemaTable());
      return schemaTable;
    }

    /**
     * Gets the layout of the Kiji table.
     *
     * @return The table layout.
     */
    public KijiTableLayout getTableLayout() {
      return HBaseKijiTable.downcast(mKijiTable).getLayout();
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
      ResourceUtils.releaseOrLog(mKijiTable);
      ResourceUtils.releaseOrLog(mKiji);
    }
  }
}

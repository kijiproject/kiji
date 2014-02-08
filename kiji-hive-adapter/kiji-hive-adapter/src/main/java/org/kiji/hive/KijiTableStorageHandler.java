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

import java.util.Map;

import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiURI;

/**
 * A Hive storage handler for reading from Kiji tables (read-only).
 */
public class KijiTableStorageHandler extends DefaultStorageHandler {
  private static final Logger LOG = LoggerFactory.getLogger(KijiTableStorageHandler.class);

  /** {@inheritDoc} */
  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return KijiTableInputFormat.class;
  }

  /** {@inheritDoc} */
  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return KijiTableOutputFormat.class;
  }

  /** {@inheritDoc} */
  @Override
  public Class<? extends SerDe> getSerDeClass() {
    return KijiTableSerDe.class;
  }

  /** {@inheritDoc} */
  @Override
  public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    configureKijiJobProperties(tableDesc, jobProperties);
  }

  /** {@inheritDoc} */
  @Override
  public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    configureKijiJobProperties(tableDesc, jobProperties);
  }

  /**
   * Helper method to share logic between {@link #configureInputJobProperties} and
   * {@link #configureOutputJobProperties}.
   *
   * @param tableDesc descriptor for the table being accessed
   * @param jobProperties receives properties copied or transformed
   */
  private void configureKijiJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    final KijiURI kijiURI = KijiTableInfo.getURIFromProperties(tableDesc.getProperties());
    jobProperties.put(KijiTableOutputFormat.CONF_KIJI_TABLE_URI, kijiURI.toString());

    // We need to propagate the table name for the jobs to know which data request to use.
    final String tableName = tableDesc.getTableName();
    jobProperties.put(KijiTableSerDe.HIVE_TABLE_NAME_PROPERTY, tableName);

  }
}

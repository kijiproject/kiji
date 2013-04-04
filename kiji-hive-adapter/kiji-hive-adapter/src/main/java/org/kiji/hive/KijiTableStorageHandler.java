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

import org.kiji.schema.KijiURI;

/**
 * A Hive storage handler for reading from Kiji tables (read-only).
 */
public class KijiTableStorageHandler extends DefaultStorageHandler {

  /** {@inheritDoc} */
  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return KijiTableInputFormat.class;
  }

  /** {@inheritDoc} */
  @Override
  public Class<? extends SerDe> getSerDeClass() {
    return KijiTableSerDe.class;
  }

  /** {@inheritDoc} */
  @Override
  public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    KijiURI kijiURI = KijiTableInfo.getURIFromProperties(tableDesc.getProperties());
    jobProperties.put(KijiTableInputFormat.CONF_KIJI_TABLE_URI, kijiURI.toString());
  }
}

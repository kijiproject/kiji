/**
 * (c) Copyright 2012 WibiData, Inc.
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

import com.google.common.base.Preconditions;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.delegation.Lookups;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiURI;
import org.kiji.schema.filter.KijiRowFilter;
import org.kiji.schema.hbase.HBaseKijiURI;

/** InputFormat for Hadoop MapReduce jobs reading from a Kiji table. */
@ApiAudience.Framework
@ApiStability.Stable
public abstract class KijiTableInputFormat
    extends InputFormat<EntityId, KijiRowData>
    implements Configurable {

  /** Static factory class for getting instances of the appropriate  KijiTableInputFormatFactory. */
  public static final class Factory {
    /**
     * Returns a KijiFactory for the appropriate type of Kiji (HBase or Cassandra), based on a URI.
     *
     * @param uri for the Kiji instance to build with the factory.
     * @return the default KijiFactory.
     */
    public static KijiTableInputFormatFactory get(KijiURI uri) {
      KijiTableInputFormatFactory instance;
      String scheme = uri.getScheme();
      if (scheme.equals(KijiURI.KIJI_SCHEME)) {
        scheme = HBaseKijiURI.HBASE_SCHEME;
      }
      synchronized (Kiji.Factory.class) {
        instance = Lookups
            .getNamed(KijiTableInputFormatFactory.class)
            .lookup(scheme);
        assert (null != instance);
      }
      return instance;
    }
  }

  /**
   * Configures a Hadoop M/R job to read from a given table.
   *
   * @param job Job to configure.
   * @param tableURI URI of the table to read from.
   * @param dataRequest Data request.
   * @param startRow Minimum row key to process. May be left null to indicate
   *     that scanning should start at the beginning of the table.
   * @param endRow Maximum row key to process. May be left null to indicate that
   *     scanning should continue to the end of the table.
   * @param filter Filter to use for scanning. May be left null.
   * @throws IOException on I/O error.
   */
  public static void configureJob(
      Job job,
      KijiURI tableURI,
      KijiDataRequest dataRequest,
      EntityId startRow,
      EntityId endRow,
      KijiRowFilter filter
  ) throws IOException {
    Preconditions.checkNotNull(job, "job must not be null");
    Preconditions.checkNotNull(tableURI, "tableURI must not be null");
    Preconditions.checkNotNull(dataRequest, "dataRequest must not be null");

    final Configuration conf = job.getConfiguration();

    // TODO: Check for jars config:
    // GenericTableMapReduceUtil.initTableInput(hbaseTableName, scan, job);

    // Write all the required values to the job's configuration object.
    final String serializedRequest =
        Base64.encodeBase64String(SerializationUtils.serialize(dataRequest));
    conf.set(KijiConfKeys.KIJI_INPUT_DATA_REQUEST, serializedRequest);
    conf.set(KijiConfKeys.KIJI_INPUT_TABLE_URI, tableURI.toString());
    if (null != startRow) {
      conf.set(KijiConfKeys.KIJI_START_ROW_KEY,
          Base64.encodeBase64String(startRow.getHBaseRowKey()));
    }
    if (null != endRow) {
      conf.set(KijiConfKeys.KIJI_LIMIT_ROW_KEY,
          Base64.encodeBase64String(endRow.getHBaseRowKey()));
    }
    if (null != filter) {
      conf.set(KijiConfKeys.KIJI_ROW_FILTER, filter.toJson().toString());
    }
  }
}

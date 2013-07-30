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

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.hive.io.KijiRowDataWritable;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiRegion;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.util.ResourceUtils;

/**
 * An input format that reads from Kiji Tables.
 *
 * <p>
 *   This input format exists in addition to the
 *   {@link org.kiji.mapreduce.framework.KijiTableInputFormat} because we need one that is
 *   an instance of mapred.InputFormat (not mapreduce.InputFormat) for integration with hive.
 * </p>
 *
 * <p>
 *   The hook that hive provides for turning MapReduce records into rows of a 2-dimensional
 *   SQL-like table is called a Deserializer. Unfortunately, Deserializers only have access to
 *   the value of the record (not the key). This means that even though this input format
 *   generates ImmutableBytesWritable keys that contain the row key of the input kiji table, the
 *   Deserializer won't have access to it. Hence, all of the data required to generate the
 *   2-dimensional view of the row must be contained in the value (in this case, the HBase Result).
 * </p>
 */
public class KijiTableInputFormat
    implements InputFormat<ImmutableBytesWritable, KijiRowDataWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(KijiTableInputFormat.class);

  public static final String CONF_KIJI_DATA_REQUEST = "kiji.data.request";
  public static final String CONF_KIJI_TABLE_URI = "kiji.table.uri";

  /**
   * Returns an object responsible for generating records contained in a
   * given input split.
   *
   * @param split The input split to create a record reader for.
   * @param job The job configuration.
   * @param reporter A job info reporter (for counters, status, etc.).
   * @return The record reader.
   * @throws IOException If there is an error.
   */
  @Override
  public RecordReader<ImmutableBytesWritable, KijiRowDataWritable> getRecordReader(
      InputSplit split, JobConf job, Reporter reporter) throws IOException {
    LOG.info("Getting record reader {}", split.getLocations());
    return new KijiTableRecordReader((KijiTableInputSplit) split, job);
  }

  /**
   * Returns an array of input splits to be used as input to map tasks.
   *
   * @param job The job configuration.
   * @param numTasks A hint from the MR framework for the number of mappers.
   * @return The specifications of each split.
   * @throws IOException If there is an error.
   */
  @Override
  public InputSplit[] getSplits(JobConf job, int numTasks) throws IOException {
    // TODO: Use the numTasks hint effectively. We just ignore it right now.

    final KijiURI kijiURI = getKijiURI(job);
    final InputSplit[] splits;

    Kiji kiji = null;
    KijiTable kijiTable = null;
    try {
      kiji = Kiji.Factory.open(kijiURI);
      kijiTable = kiji.openTable(kijiURI.getTable());

      // Get the start keys for each region in the table.
      List<KijiRegion> kijiRegions = kijiTable.getRegions();
      splits = new InputSplit[kijiRegions.size()];
      for (int i = 0; i < kijiRegions.size(); i++) {
        KijiRegion kijiRegion = kijiRegions.get(i);
        byte[] regionStartKey = kijiRegion.getStartKey();
        byte[] regionEndKey = kijiRegion.getEndKey();

        Collection<String> regionLocations = kijiRegion.getLocations();
        String regionHost = null;
        if (!regionLocations.isEmpty()) {
          // TODO: Allow the usage of regions that aren't the first.
          String regionLocation = regionLocations.iterator().next();
          regionHost = regionLocation.substring(0, regionLocation.indexOf(":"));
        } else {
          LOG.warn("No locations found for region: {}", kijiRegion.toString());
        }
        final Path dummyPath = FileInputFormat.getInputPaths(job)[0];
        splits[i] = new KijiTableInputSplit(kijiURI,
            regionStartKey, regionEndKey, regionHost, dummyPath);
      }
    } catch (IOException e) {
      LOG.warn("Unable to get region information.  Returning an empty list of splits.");
      LOG.warn(StringUtils.stringifyException(e));
      return new InputSplit[0];
    } finally {
      ResourceUtils.releaseOrLog(kijiTable);
      ResourceUtils.releaseOrLog(kiji);
    }
    return splits;
  }

  /**
   * Gets the name of the kiji table this input format will read from.
   *
   * @param conf The job configuration.
   * @return The name of the kiji table this input format will read from.
   */
  private static KijiURI getKijiURI(Configuration conf) {
    final String kijiURIString = conf.get(KijiTableInfo.KIJI_TABLE_URI);
    if (null == kijiURIString) {
      throw new RuntimeException("KijiTableInputFormat needs to be configured. "
          + "Please specify " + KijiTableInfo.KIJI_TABLE_URI + " in the configuration.");
    }
    KijiURI kijiURI = KijiURI.newBuilder(kijiURIString).build();
    return kijiURI;
  }
}

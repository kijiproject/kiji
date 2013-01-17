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

package org.kiji.mapreduce.output;

import java.io.IOException;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.KijiConfKeys;
import org.kiji.mapreduce.MapReduceJobOutput;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTable;

/** A MapReduce job output that writes to a Kiji table. */
@ApiAudience.Public
public class KijiTableMapReduceJobOutput extends MapReduceJobOutput {

  /** Kiji table to write to. */
  private final KijiTable mTable;

  /** Number of reduce tasks to use. */
  private final int mNumReduceTasks;

  /**
   * Creates a new <code>KijiTableMapReduceJobOutput</code> instance.
   *
   * @param table The kiji table to write output to.
   */
  public KijiTableMapReduceJobOutput(KijiTable table) {
    this(table, 0);
  }

  /**
   * Creates a new <code>KijiTableMapReduceJobOutput</code> instance.
   *
   * @param table The kiji table to write output to.
   * @param numReduceTasks The number of reduce tasks to use (use zero if using a producer).
   */
  public KijiTableMapReduceJobOutput(KijiTable table, int numReduceTasks) {
    mTable = table;
    mNumReduceTasks = numReduceTasks;
  }

  /** @return the Kiji table to write to. */
  public KijiTable getTable() {
    return mTable;
  }

  /** {@inheritDoc} */
  @Override
  public void configure(Job job) throws IOException {
    final Kiji kiji = mTable.getKiji();

    job.getConfiguration().set(
        KijiConfKeys.OUTPUT_KIJI_TABLE_URI, String.format("kiji://%s:%s/%s/%s",
            job.getConfiguration().get(HConstants.ZOOKEEPER_QUORUM),
            job.getConfiguration().getInt(HConstants.ZOOKEEPER_CLIENT_PORT,
                HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT),
            kiji.getName(),
            mTable.getName()));

    // For direct HTable writes, there is no MapReduce output:
    job.setOutputFormatClass(NullOutputFormat.class);
    job.setNumReduceTasks(mNumReduceTasks);

    // Since there's no "commit" operation for an entire map task writing to a
    // Kiji table, do not use speculative execution when writing directly to a Kiji table.
    job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
  }

  /** {@inheritDoc} */
  @Override
  protected Class<? extends OutputFormat> getOutputFormatClass() {
    throw new UnsupportedOperationException();
  }
}

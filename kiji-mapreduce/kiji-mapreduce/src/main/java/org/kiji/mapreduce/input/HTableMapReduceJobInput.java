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

package org.kiji.mapreduce.input;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.MapReduceJobInput;
import org.kiji.mapreduce.impl.HTableInputFormat;
import org.kiji.mapreduce.tools.framework.JobIOConfKeys;

/**
 * The class HTableMapReduceJobInput is used to indicate the usage of a HBase table
 * as input to a MapReduce job. Any MapReduce job configured to read from an HBase
 * table should expect to receive an <code>ImmutableBytesWritable</code> as a key
 * and a <code>Result</code> as a value.
 *
 * <h2>Configuring an input:</h2>
 * <p>
 *   HTableMapReduceJobInput must be configured with the name of the input HBase table:
 * </p>
 * <pre>
 *   <code>
 *     final MapReduceJobInput htableJobInput = new HTableMapReduceJobInput("mytable");
 *   </code>
 * </pre>
 */
@ApiAudience.Public
public final class HTableMapReduceJobInput extends MapReduceJobInput {
  /** The name of the HTable to use as job input. */
  private String mTableName;

  /** Default constructor. Used by {@link MapReduceJobInputs} */
  HTableMapReduceJobInput() { }

  /** {@inheritDoc} */
  @Override
  public void initialize(Map<String, String> params) throws IOException {
    mTableName = params.get(JobIOConfKeys.HTABLE_NAME_KEY);
  }

  /**
   * Creates a new <code>HTableMapReduceJobInput</code> instance.  Accessible via
   * {@link MapReduceJobInputs#newHTableMapReduceJobInput(String)}.
   *
   * @param tableName The name of the HBase table (HTable) to use as input for the job.
   */
  HTableMapReduceJobInput(String tableName) {
    mTableName = tableName;
  }

  /** {@inheritDoc} */
  @Override
  public void configure(Job job) throws IOException {
    // Configure the input format class.
    super.configure(job);

    // Configure the input HTable name.
    job.getConfiguration().set(TableInputFormat.INPUT_TABLE, mTableName);
  }

  /** {@inheritDoc} */
  @Override
  protected Class<? extends InputFormat<?, ?>> getInputFormatClass() {
    return HTableInputFormat.class;
  }
}

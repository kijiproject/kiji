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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;
import org.kiji.mapreduce.KijiTableContext;
import org.kiji.mapreduce.framework.KijiConfKeys;
import org.kiji.mapreduce.impl.DirectKijiTableWriterContext;
import org.kiji.schema.KijiURI;

/**
 * The class DirectKijiTableMapReduceJobOutput is used to indicate the usage of a Kiji table
 * as an output for a MapReduce job.
 *
 * <p>
 *   Use of this job output configuration is discouraged for many reasons:
 *   <ul>
 *     <li> It may induce a very high load on the target HBase cluster.
 *     <li> It may result in partial writes (eg. if the job fails half through).
 *   </ul>
 *   The recommended way to write to HBase tables is through the {@link HFileMapReduceJobOutput}.
 * </p>
 *
 * <h2>Configuring an output:</h2>
 * <p>
 *   DirectKijiTableMapReduceJobOutput must be configured with the address of the Kiji table to
 *   write to and optionally the number of reduce tasks to use for the job:
 * </p>
 * <pre>
 *   <code>
 *     final MapReduceJobOutput kijiTableOutput =
 *         MapReduceJobOutputs.newDirectKijiTableMapReduceJobOutput(myURI);
 *   </code>
 * </pre>
 * @see HFileMapReduceJobOutput
 */
@ApiAudience.Public
@ApiStability.Stable
@Inheritance.Sealed
public class DirectKijiTableMapReduceJobOutput extends KijiTableMapReduceJobOutput {
  /** Default constructor. Accessible via {@link MapReduceJobOutputs}. */
  DirectKijiTableMapReduceJobOutput() {
  }

  /**
   * Creates a new <code>KijiTableMapReduceJobOutput</code> instance.
   *
   * @param tableURI The Kiji table to write to.
   */
  DirectKijiTableMapReduceJobOutput(KijiURI tableURI) {
    this(tableURI, 0);
  }

  /**
   * Creates a new <code>KijiTableMapReduceJobOutput</code> instance.
   *
   * @param tableURI The Kiji table to write to.
   * @param numReduceTasks The number of reduce tasks to use (use zero if using a producer).
   */
  DirectKijiTableMapReduceJobOutput(KijiURI tableURI, int numReduceTasks) {
    super(tableURI, numReduceTasks);
  }

  /** {@inheritDoc} */
  @Override
  public void configure(Job job) throws IOException {
    // sets Hadoop output format, Kiji output table and # of reducers:
    super.configure(job);

    final Configuration conf = job.getConfiguration();

    // Kiji table context:
    conf.setClass(
        KijiConfKeys.KIJI_TABLE_CONTEXT_CLASS,
        DirectKijiTableWriterContext.class,
        KijiTableContext.class);

    // Since there's no "commit" operation for an entire map task writing to a
    // Kiji table, do not use speculative execution when writing directly to a Kiji table.
    conf.setBoolean("mapred.map.tasks.speculative.execution", false);
  }

  /** {@inheritDoc} */
  @Override
  protected Class<? extends OutputFormat> getOutputFormatClass() {
    // No hadoop output:
    return NullOutputFormat.class;
  }
}

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

/**
 * Producer classes for Kiji MapReduce clients.
 * <p>
 *   A KijiProducer executes a function over a subset of the columns in a table row and produces
 *   output to be injected back into a column of that row. Producers can be run in the context of
 *   a MapReduce over entire Kiji tables, or on-demand over a single row at a time.
 *   The {@link org.kiji.mapreduce.produce.KijiProducer} class is the base class for all producers.
 *   Subclasses take inputs from {@link org.kiji.mapreduce.MapReduceJobInput} and produce output to
 *   Kiji.
 * </p>
 *
 * <h2>Constructing a produce job:</h2>
 * <p>
 *   A producer job that executes its function over each row in a table and injects the result
 *   back into the row can be constructed as follows:
 * </p>
 * <pre><code>
 *   // Configure and create the MapReduce job.
 *   final MapReduceJob job = KijiProduceJobBuilder.create()
 *       .withProducer(SimpleProducer.class)
 *       .withInputTable(mTable.getURI())
 *       .withOutput(new DirectKijiTableMapReduceJobOutput(mTable.getURI()))
 *       .build();
 * </code></pre>
 * <p>
 *   The <code>kiji produce</code> command line tool wraps this functionality and can be used
 *   for constructing produce jobs.
 * </p>
 * @see org.kiji.mapreduce.output.DirectKijiTableMapReduceJobOutput
 */

package org.kiji.mapreduce.produce;

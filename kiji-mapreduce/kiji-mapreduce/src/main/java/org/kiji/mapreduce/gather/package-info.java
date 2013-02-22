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
 * Gatherer classes for KijiMR clients.
 *
 * <p>
 *   A Kiji Gatherer scans over the rows of a Kiji table using the MapReduce framework to
 *   aggregate information which can be passed to a reducer.
 *   The {@link org.kiji.mapreduce.gather.KijiGatherer} class is the base class for all gatherers.
 *   Subclasses take inputs from {@link org.kiji.schema.KijiTable} and produce output to
 *   to HFiles.
 * </p>
 *
 * <h2>Constructing a gatherer job:</h2>
 * <p>
 *   A gatherer job that outputs to HFiles can be created here:
 * </p>
 * <pre><code>
 *   // Configure and create the MapReduce job.
 *   final MapReduceJob job = KijiGatherJobBuilder.create()
 *       .withConf(conf)
 *       .withInputTable(mTable)
 *       .withGatherer(SimpleGatherer.class)
 *       .withCombiner(MyCombiner.class)
 *       .withReducer(MyReducer.class)
 *       .withOutput(new TextMapReduceJobOutput(new Path("mypath"), 10))
 *       .build();
 * </code></pre>
 * <p>
 *   The <code>kiji gather</code> command line tool wraps this functionality and can be used
 *   for constructing gathererjobs.
 * </p>
 */

package org.kiji.mapreduce.gather;

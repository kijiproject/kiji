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
 * Pivot classes for KijiMR clients.
 *
 * <p>
 *   A {@link KijiPivoter} scans over the rows of an input Kiji table and writes cells
 *   into an output Kiji table which may or may not be the same table.
 *   The {@link KijiPivoter} class is the base class for all pivoter job.
 *   Conceptually, a {@link KijiPivoter} is a map-only job that reads from a Kiji table
 *   and writes to a Kiji table.
 * </p>
 *
 * <h2>Constructing a pivot job:</h2>
 * <p> A pivot job that writes HFiles can be created as follows: </p>
 * <pre><blockquote>
 *   final Configuration conf = ...;
 *   final KijiURI inputTableURI = ...;
 *   final KijiURI outputTableURI = ...;
 *   final KijiMapReduceJob job = KijiPivotJobBuilder.create()
 *       .withConf(conf)
 *       .withPivoter(SomePivoter.class)
 *       .withInputTable(inputTableURI)
 *       .withOutput(MapReduceJobOutputs
 *           .newHFileMapReduceJobOutput(outputTableURI, hfilePath))
 *       .build();
 *   job.run();
 * </blockquote></pre>
 * <p>
 *   The {@code kiji pivot} command line tool wraps this functionality and can be used
 *   to launch pivot jobs.
 * </p>
 */

package org.kiji.mapreduce.pivot;

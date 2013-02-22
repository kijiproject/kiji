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

/**
 * KijiMR utilities.
 *
 * <p>
 *   KijiMR includes APIs to build MapReduce jobs that run over Kiji tables, bringing
 *   MapReduce-based analytic techniques to a broad base of Kiji Schema users.
 * </p>
 *
 * <h2>Building MapReduce jobs:</h2>
 * <p>
 *   KijiMR contains many job builders for various types of MapReduce jobs.
 * </p>
 * <li>{@link org.kiji.mapreduce.bulkimport Bulk Importers} - for the creation of bulk
 *     import jobs which allow data to be inserted into Kiji tables efficiently.
 * <li>{@link org.kiji.mapreduce.produce Producers} - for the creation of produce jobs
 *     which generate per-row derived entity data.
 * <li>{@link org.kiji.mapreduce.gather Gatherers} - for the creation of gather jobs
 *     which scan over the rows of a Kiji table to aggregate information which can be passed to a
 *     reducer.
 * <li>{@link org.kiji.mapreduce.KijiMapReduceJobBuilder General MapReduce} - for the creation of
 *     general MapReduce jobs around Kiji mappers and reducers.</li>
 *
 * <h2>Utility packages for MapReduce job construction:</h2>
 * <li>{@link org.kiji.mapreduce.input} - input formats for MapReduce jobs</li>
 * <li>{@link org.kiji.mapreduce.output} - output formats for MapReduce jobs</li>
 * <li>{@link org.kiji.mapreduce.kvstore} - Key-Value store API used in MapReduce jobs</li>
 */
package org.kiji.mapreduce;

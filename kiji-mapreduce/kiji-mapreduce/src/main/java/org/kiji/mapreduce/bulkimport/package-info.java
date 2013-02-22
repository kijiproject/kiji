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
 * Bulk importer classes for KijiMR clients.
 *
 * <p>
 *   The {@link org.kiji.mapreduce.bulkimport.KijiBulkImporter} class is the base class for all
 *   bulk importers.  Subclasses take inputs from {@link org.kiji.mapreduce.MapReduceJobInput}
 *   and produce output to Kiji either directly or through HFiles.
 * </p>
 *
 * <h2>Constructing a bulk import job:</h2>
 * <p>
 *   A bulk import job that outputs to an intermediary HFile(which can subsequently be imported
 *   via a <code>kiji bulk-load</code> command can be created here:
 * </p>
 * <pre><code>
 *   // Configure and create the MapReduce job.
 *   final MapReduceJob job = KijiBulkImportJobBuilder.create()
 *       .withConf(conf)
 *       .withBulkImporter(JSONBulkImporter.class)
 *       .withInput(new TextMapReduceJobInput(new Path(inputFile.toString())))
 *       .withOutput(new DirectKijiTableMapReduceJobOutput(mOutputTable))
 *       .build();
 * </code></pre>
 * <p>
 *   Alternately a bulk import job that directly outputs to a Kiji table can be performed by
 *   replacing the .withOutput parameter.  This is generally not recommended as this can result
 *   in heavy load on the target HBase cluster.  Also if the job doesn't complete, this can result
 *   in partial writes.
 * </p>
 * <pre><code>
 *   // Configure and create the MapReduce job.
 *   final MapReduceJob job = KijiBulkImportJobBuilder.create()
 *       .withConf(conf)
 *       .withBulkImporter(JSONBulkImporter.class)
 *       .withInput(new TextMapReduceJobInput(new Path(inputFile.toString())))
 *       .withOutput(new HFileMapReduceJobOutput(mOutputTable, hfileDirPath))
 *       .build();
 * </code></pre>
 * <p>
 *   The <code>kiji bulk-import</code> command line tool wraps this functionality and can be used
 *   for constructing bulk import jobs.  If HFiles are created as the output for a bulk import job
 *   they can be loaded into HBase using the <code>kiji bulk-load</code> command.
 * </p>
 *
 * @see org.kiji.mapreduce.output.HFileMapReduceJobOutput
 * @see org.kiji.mapreduce.output.DirectKijiTableMapReduceJobOutput
 */

package org.kiji.mapreduce.bulkimport;

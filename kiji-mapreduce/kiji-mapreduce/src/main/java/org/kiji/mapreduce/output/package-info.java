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
 * MapReduce job output types.
 *
 * <p>
 *   {@link org.kiji.mapreduce.MapReduceJobOutput} is the base type for classes that can act
 *   as outputs to a MapReduce job for jobs that are created using a
 *   {@link org.kiji.mapreduce.framework.MapReduceJobBuilder}.
 * <p>
 *
 * <h2>Usable Kiji MapReduce output types</h2>
 * <li>{@link org.kiji.mapreduce.output.AvroKeyMapReduceJobOutput} - Avro container files containing
 *     keys as output.</li>
 * <li>{@link org.kiji.mapreduce.output.AvroKeyValueMapReduceJobOutput} - Avro container files
 *     containing key value pairs as output.</li>
 * <li>{@link org.kiji.mapreduce.output.DirectKijiTableMapReduceJobOutput} - Kiji table as
 *     output</li>
 * <li>{@link org.kiji.mapreduce.output.HFileMapReduceJobOutput} - HFile as output.</li>
 * <li>{@link org.kiji.mapreduce.output.MapFileMapReduceJobOutput} - Hadoop map files as
 *     output</li>
 * <li>{@link org.kiji.mapreduce.output.SequenceFileMapReduceJobOutput} - Hadoop sequence files as
 *     output</li>
 * <li>{@link org.kiji.mapreduce.output.TextMapReduceJobOutput} - text files as output</li>
 *
 * @see org.kiji.mapreduce.framework.MapReduceJobBuilder
 */

package org.kiji.mapreduce.output;

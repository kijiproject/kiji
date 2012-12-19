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

package org.kiji.mapreduce.tools;

import org.apache.hadoop.fs.Path;

import org.kiji.mapreduce.MapReduceJobOutput;
import org.kiji.mapreduce.output.AvroKeyMapReduceJobOutput;
import org.kiji.mapreduce.output.AvroKeyValueMapReduceJobOutput;
import org.kiji.mapreduce.output.MapFileMapReduceJobOutput;
import org.kiji.mapreduce.output.SequenceFileMapReduceJobOutput;
import org.kiji.mapreduce.output.TextMapReduceJobOutput;

/**
 * A factory for creating MapReduceJobOutput objects from outputspecs.
 *
 * See {@link JobOutputSpec} for the full outputspec specification.
 */
public class MapReduceJobOutputFactory {
  /**
   * Creates a MapReduceJobOutput from an outputspec string.
   *
   * @param outputSpec The outputspec.
   * @return A new MapReduceJobOutput instance.
   * @throws JobIOSpecParseException If the outputspec cannot be parsed.
   */
  public MapReduceJobOutput createFromOutputSpec(String outputSpec)
      throws JobIOSpecParseException {
    return createFromOutputSpec(JobOutputSpec.parse(outputSpec));
  }

  /**
   * Creates a MapReduceJobOutput from an outputspec.
   *
   * @param jobOutputSpec The outputspec.
   * @return A new MapReduceJobOutput instance.
   * @throws JobIOSpecParseException If the outputspec cannot be parsed.
   */
  public MapReduceJobOutput createFromOutputSpec(JobOutputSpec jobOutputSpec)
      throws JobIOSpecParseException {
    switch (jobOutputSpec.getFormat()) {
    case TEXT:
      return new TextMapReduceJobOutput(
          new Path(jobOutputSpec.getLocation()), jobOutputSpec.getSplits());
    case SEQUENCE_FILE:
      return new SequenceFileMapReduceJobOutput(
          new Path(jobOutputSpec.getLocation()), jobOutputSpec.getSplits());
    case MAP_FILE:
      return new MapFileMapReduceJobOutput(
          new Path(jobOutputSpec.getLocation()), jobOutputSpec.getSplits());
    case AVRO:
      return new AvroKeyMapReduceJobOutput(
          new Path(jobOutputSpec.getLocation()), jobOutputSpec.getSplits());
    case AVRO_KV:
      return new AvroKeyValueMapReduceJobOutput(
          new Path(jobOutputSpec.getLocation()), jobOutputSpec.getSplits());
    default:
      throw new JobIOSpecParseException(
          "Unknown format " + jobOutputSpec.getFormat(), jobOutputSpec.toString());
    }
  }
}

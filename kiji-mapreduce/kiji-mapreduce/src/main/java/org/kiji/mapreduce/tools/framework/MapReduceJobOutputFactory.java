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

package org.kiji.mapreduce.tools.framework;

import java.io.IOException;
import java.util.Map;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.MapReduceJobOutput;
import org.kiji.mapreduce.output.MapReduceJobOutputs;
import org.kiji.mapreduce.tools.framework.JobOutputSpec.Format;
import org.kiji.schema.tools.SpaceSeparatedMapParser;

/**
 * A factory for creating MapReduceJobOutput objects from outputspecs.
 *
 * See {@link JobOutputSpec} for the full outputspec specification.
 */
@ApiAudience.Framework
public final class MapReduceJobOutputFactory {
  /** Constructs a new factory for MapReduce job outputs. */
  private MapReduceJobOutputFactory() { }

  /**
   * Creates a new factory for instances of {@link MapReduceJobOutput}.
   *
   * @return a new factory for MapReduce job outputs.
   */
  public static MapReduceJobOutputFactory create() {
    return new MapReduceJobOutputFactory();
  }

  /**
   * Creates a job output from a command-line flag space-separated parameters map.
   *
   * @param ssm Space-separated parameters map from the command-line.
   * @return a job output configured from the command-line parameters.
   * @throws IOException on I/O error.
   */
  public MapReduceJobOutput fromSpaceSeparatedMap(String ssm) throws IOException {
    final Map<String, String> params = SpaceSeparatedMapParser.create().parse(ssm);
    final String format = params.get(JobIOConfKeys.FORMAT_KEY);
    try {
      final MapReduceJobOutput output = createJobOutput(Format.parse(format));
      output.initialize(params);
      return output;
    } catch (JobIOSpecParseException exn) {
      throw new IOException(String.format(
          "Invalid job output specification: '%s': %s", ssm, exn.getMessage()), exn);
    }
  }

  /**
   * Instantiates the appropriate job output.
   *
   * @param format Format of the job output.
   * @return an unconfigured job output instance.
   * @throws IOException on I/O error.
   */
  private static MapReduceJobOutput createJobOutput(Format format) throws IOException {
    switch (format) {
    case TEXT:
      return MapReduceJobOutputs.newTextMapReduceJobOutput();
    case SEQUENCE_FILE:
      return MapReduceJobOutputs.newSequenceFileMapReduceJobOutput();
    case MAP_FILE:
     return MapReduceJobOutputs.newMapFileMapReduceJobOutput();
    case AVRO:
      return MapReduceJobOutputs.newAvroKeyMapReduceJobOutput();
    case AVRO_KV:
      return MapReduceJobOutputs.newAvroKeyValueMapReduceJobOutput();
    case KIJI:
      return MapReduceJobOutputs.newDirectKijiTableMapReduceJobOutput();
    case HFILE:
      return MapReduceJobOutputs.newHFileMapReduceJobOutput();
    default:
      throw new RuntimeException(String.format("Unhandled job output format: '%s'.", format));
    }
  }
}

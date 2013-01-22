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

import java.util.Arrays;

import org.apache.hadoop.fs.Path;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.MapReduceJobInput;
import org.kiji.mapreduce.input.AvroKeyMapReduceJobInput;
import org.kiji.mapreduce.input.AvroKeyValueMapReduceJobInput;
import org.kiji.mapreduce.input.HTableMapReduceJobInput;
import org.kiji.mapreduce.input.SequenceFileMapReduceJobInput;
import org.kiji.mapreduce.input.TextMapReduceJobInput;
import org.kiji.mapreduce.input.WholeTextFileMapReduceJobInput;

/**
 * Constructs instances of MapReduceJobInput from a string specification.
 *
 * See {@link JobInputSpec} for the full inputspec specification.
 */
@ApiAudience.Framework
public class MapReduceJobInputFactory {
  /**
   * Creates a MapReduceJobInput from an inputspec string.
   *
   * @param inputSpec The inputspec.
   * @return A new MapReduceJobInput instance.
   * @throws JobIOSpecParseException If the inputspec cannot be parsed.
   */
  public MapReduceJobInput createFromInputSpec(String inputSpec) throws JobIOSpecParseException {
    return createFromInputSpec(JobInputSpec.parse(inputSpec));
  }

  /**
   * Creates a MapReduceJobInput from a JobInputSpec.
   *
   * @param jobInputSpec The <code>JobInputSpec</code> describing the input.
   * @return A new MapReduceJobInput instance.
   * @throws JobIOSpecParseException If the inputspec cannot be parsed.
   */
  public MapReduceJobInput createFromInputSpec(JobInputSpec jobInputSpec)
      throws JobIOSpecParseException {
    switch (jobInputSpec.getFormat()) {
    case AVRO:
      return new AvroKeyMapReduceJobInput(getPaths(jobInputSpec.getLocations()));
    case AVRO_KV:
      return new AvroKeyValueMapReduceJobInput(getPaths(jobInputSpec.getLocations()));
    case HTABLE:
      if (null == jobInputSpec.getLocations() || 1 != jobInputSpec.getLocations().length) {
        throw new UnsupportedOperationException("Can not use multiple htables as input."
            + "  Tables: " + Arrays.toString(jobInputSpec.getLocations()));
      }
      return new HTableMapReduceJobInput(jobInputSpec.getLocation());
    case SEQUENCE:
      return new SequenceFileMapReduceJobInput(getPaths(jobInputSpec.getLocations()));
    case SMALL_TEXT_FILES:
      return new WholeTextFileMapReduceJobInput(getPaths(jobInputSpec.getLocations()));
    case TEXT:
      return new TextMapReduceJobInput(getPaths(jobInputSpec.getLocations()));
    // TODO: Figure out how to handle Kiji table inputs:
    //    case KIJI: {
    //      final KijiURI uri = KijiURI.parse(jobInputSpec.getLocation());
    //      final Kiji kiji = Kiji.Factory.open(uri);
    //      final KijiTable table = kiji.openTable(uri.getTable());
    //      return new KijiTableMapReduceJobInput((HBaseKijiTable) table);
    //    }
    default:
      throw new JobIOSpecParseException(
          "Unknown format " + jobInputSpec.getFormat(), jobInputSpec.toString());
    }
  }

  /**
   * Tranforms a list of locations into a list of Paths.
   *
   * @param locations The array of locations to transform.
   * @return The list of Paths corresponding to these locations.
   */
  private static Path[] getPaths(String[] locations) {
    assert null != locations;

    Path[] paths = new Path[locations.length];
    for (int i = 0; i < locations.length; i++) {
      paths[i] = new Path(locations[i]);
    }
    return paths;
  }
}

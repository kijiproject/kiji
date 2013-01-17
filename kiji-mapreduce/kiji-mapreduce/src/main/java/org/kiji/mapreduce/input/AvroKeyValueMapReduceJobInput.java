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

package org.kiji.mapreduce.input;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;

import org.kiji.annotations.ApiAudience;
import org.kiji.avro.mapreduce.AvroKeyValueInputFormat;

/**
 * A MapReduce job input of Avro container files of generic records, where each entry has
 * a 'key' and a 'value' field.
 */
@ApiAudience.Public
public class AvroKeyValueMapReduceJobInput extends FileMapReduceJobInput {
  /**
   * Creates a new <code>AvroKeyValueMapReduceJobInput</code> instance.
   *
   * @param paths The paths to the avro input files.
   */
  public AvroKeyValueMapReduceJobInput(Path... paths) {
    super(paths);
  }

  /** {@inheritDoc} */
  @Override
  protected Class<? extends InputFormat> getInputFormatClass() {
    return AvroKeyValueInputFormat.class;
  }
}

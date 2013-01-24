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

package org.kiji.mapreduce.lib.examples;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.kiji.mapreduce.KijiBaseMapper;

/**
 * A map that inverts its (Text, IntWritable) records into (IntWritable, Text) records.
 *
 * <p>This useful if you need to sort the value from the results of a previous job.  For
 * example, you could use this map to invert the output of the
 * EmailDomainCountGatherJob to generate a dataset of email domains sorted by their
 * popularity.</p>
 */
public class InvertCountMapper extends KijiBaseMapper<Text, IntWritable, IntWritable, Text> {
  /** {@inheritDoc} */
  @Override
  public void map(Text key, IntWritable value, Context context)
      throws IOException, InterruptedException {
    context.write(value, key);
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputKeyClass() {
    return IntWritable.class;
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputValueClass() {
    return Text.class;
  }
}

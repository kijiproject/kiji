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

package org.kiji.music.reduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.kiji.common.flags.Flag;
import org.kiji.mapreduce.KijiTableContext;
import org.kiji.mapreduce.KijiTableReducer;

/**
 * A reducer that will sum the values associated with a key and write that sum
 * to a column specified via command line flags.
 */
public class TotalCountTableReducer extends KijiTableReducer<Text, LongWritable> {
  // For our music project, we are writing to column family "derived".
  @Flag(name="column-family", usage="Specify the column family to write to.")
  public String mColumnFamily;

  // For our music project, we are writing to column qualifier "number_of_plays".
  @Flag(name="column-qualifier", usage="Specify the column qualifier to write to.")
  public String mColumnQualifier;

  /** {@inheritDoc} */
  @Override
  protected void reduce(Text key, Iterable<LongWritable> values, KijiTableContext context)
      throws IOException {
    // Initialize sum to zero.
    long sum = 0L;
    // Add up all the values.
    for (LongWritable value : values) {
      sum += value.get();
    }
    // Write out result for this song
    context.put(context.getEntityId(key.toString()), mColumnFamily, mColumnQualifier, values);
  }
}

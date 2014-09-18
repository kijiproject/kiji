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

package org.kiji.examples.music.gather;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;

import org.kiji.mapreduce.KijiGatherer;
import org.kiji.mapreduce.MapReduceContext;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;

/**
 * Gatherer to count the total number of times each song has been played.
 */
public class SongPlayCounter extends KijiGatherer<LongWritable, LongWritable> {

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputKeyClass() {
    return LongWritable.class;
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputValueClass() {
    return LongWritable.class;
  }

  /** {@inheritDoc} */
  @Override
  public KijiDataRequest getDataRequest() {
    return KijiDataRequest.create("");
  }

  /** {@inheritDoc} */
  @Override
  public void gather(KijiRowData row, MapReduceContext<LongWritable, LongWritable> context)
      throws IOException {
    // TODO Auto-generated method stub
  }

}

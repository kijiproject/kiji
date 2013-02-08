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

package org.kiji.mapreduce.testlib;

import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.kiji.mapreduce.KijiGatherer;
import org.kiji.mapreduce.KijiTableContext;
import org.kiji.mapreduce.KijiTableReducer;
import org.kiji.mapreduce.MapReduceContext;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;

/**
 * Example of a Kiji table map/reduce.
 *
 * A table M/R is a mapper from a Kiji row key to (key, value) pairs (ie. a KijiGatherer),
 * followed by a reducer from (key, value) pairs to a Kiji table (ie. a KijiTableReducer).
 */
public class SimpleTableMapReducer {

  /** Table mapper that processes Kiji rows and emits (key, value) pairs. */
  public static class TableMapper extends KijiGatherer<LongWritable, Text> {
    /** {@inheritDoc} */
    @Override
    public KijiDataRequest getDataRequest() {
      return KijiDataRequest.create("primitives");
    }

    /** {@inheritDoc} */
    @Override
    public Class<?> getOutputKeyClass() {
      return LongWritable.class;
    }

    /** {@inheritDoc} */
    @Override
    public Class<?> getOutputValueClass() {
      return Text.class;
    }

    /** {@inheritDoc} */
    @Override
    public void gather(KijiRowData input, MapReduceContext<LongWritable, Text> context)
        throws IOException {
      final String rowKey = Bytes.toString((byte[]) input.getEntityId().getComponentByIndex(0));
      final CharSequence cseq = input.getMostRecentValue("primitives", "string");
      if (cseq == null) {
        return;
      }
      final long value = Long.parseLong(cseq.toString());
      context.write(new LongWritable(value), new Text(rowKey));
    }

  }

  /** Table reduces that processes (key, value) pairs and emits Kiji puts. */
  public static class TableReducer extends KijiTableReducer<LongWritable, Text> {
    /** {@inheritDoc} */
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, KijiTableContext context)
        throws IOException {
      int count = 0;
      for (Text text : values) {
        count += 1;
      }
      context.put(context.getEntityId("long-" + key.get()), "primitives", "int", count);
    }
  }
}

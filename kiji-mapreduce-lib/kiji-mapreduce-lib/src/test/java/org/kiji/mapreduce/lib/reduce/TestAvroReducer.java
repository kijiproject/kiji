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

package org.kiji.mapreduce.lib.reduce;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;

import org.kiji.mapreduce.lib.avro.Node;
import org.kiji.mapreduce.lib.graph.NodeBuilder;

public class TestAvroReducer {
  public static class MyAvroReducer extends AvroReducer<Text, AvroValue<Node>, Node> {
    @Override
    protected void reduce(Text key, Iterable<AvroValue<Node>> values, Context context)
        throws IOException, InterruptedException {
      for (AvroValue<Node> value : values) {
        write(value.datum(), context);
      }
    }

    @Override
    public Schema getAvroKeyWriterSchema() throws IOException {
      return Node.SCHEMA$;
    }
  }

  @Test
  public void testOutputTypes() throws IOException {
    MyAvroReducer reducer = new MyAvroReducer();
    assertEquals(AvroKey.class, reducer.getOutputKeyClass());
    assertEquals(NullWritable.class, reducer.getOutputValueClass());
    assertEquals(Node.SCHEMA$, reducer.getAvroKeyWriterSchema());
  }

  @Test
  public void testMapReduce() throws IOException {
    MyAvroReducer reducer = new MyAvroReducer();

    // Configure a job.
    Job job = new Job();
    // We've got to do a little hacking here since mrunit doesn't run exactly like
    // the real hadoop mapreduce framework.
    AvroJob.setMapOutputKeySchema(job, Node.SCHEMA$);
    AvroJob.setOutputKeySchema(job, reducer.getAvroKeyWriterSchema());
    AvroSerialization.setValueWriterSchema(job.getConfiguration(), Node.SCHEMA$);

    // Run the reducer.
    ReduceDriver<Text, AvroValue<Node>, AvroKey<Node>, NullWritable> driver
        = new ReduceDriver<Text, AvroValue<Node>, AvroKey<Node>, NullWritable>();
    driver.setReducer(reducer);
    driver.withConfiguration(job.getConfiguration());
    driver.withInput(new Text("foo"),
        Collections.singletonList(new AvroValue<Node>(new NodeBuilder("bar", 1.0).build())));
    List<Pair<AvroKey<Node>, NullWritable>> output = driver.run();
    assertEquals(1, output.size());
    assertEquals("bar", output.get(0).getFirst().datum().getLabel().toString());
  }
}

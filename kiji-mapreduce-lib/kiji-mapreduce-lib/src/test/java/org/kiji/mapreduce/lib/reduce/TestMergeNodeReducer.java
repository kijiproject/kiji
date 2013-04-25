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
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.List;

import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;

import org.kiji.mapreduce.lib.avro.Node;
import org.kiji.mapreduce.lib.graph.EdgeBuilder;
import org.kiji.mapreduce.lib.graph.NodeBuilder;

public class TestMergeNodeReducer {
  @Test
  public void testMergeNodeReducer() throws IOException {
    MergeNodeReducer<Text> reducer = new MergeNodeReducer<Text>();
    ReduceDriver<Text, AvroValue<Node>, Text, AvroValue<Node>> driver
        = new ReduceDriver<Text, AvroValue<Node>, Text, AvroValue<Node>>();
    driver.setReducer(reducer);

    // Configure avro serialization.
    Job job = new Job();
    // We've got to do a little hacking here since mrunit doesn't run exactly like
    // in the real hadoop mapreduce framework.
    AvroJob.setMapOutputValueSchema(job, reducer.getAvroValueWriterSchema());
    AvroJob.setOutputValueSchema(job, reducer.getAvroValueWriterSchema());
    driver.withConfiguration(job.getConfiguration());

    // Here's what our node graph looks like.
    //
    //  X: A/1.0 ----b/2.0---> C/3.0
    //  Y: A/1.0 ----b/2.0---> C/6.0
    //  Z: A/1.0 ----f/1.0---> C/7.0
    //  W: A/1.0 ----d/4.0---> E/5.0
    //
    driver.withInputKey(new Text("A"));

    Node node = new NodeBuilder()
        .setLabel("A")
        .setWeight(1.0)
        .addEdge(new EdgeBuilder()
            .setLabel("b")
            .setWeight(2.0)
            .setTarget(new NodeBuilder()
                .setLabel("C")
                .setWeight(3.0)
                .build())
            .build())
        .build();
    driver.withInputValue(new AvroValue<Node>(node));

    node = new NodeBuilder()
    .setLabel("A")
    .setWeight(1.0)
    .addEdge(new EdgeBuilder()
        .setLabel("b")
        .setWeight(2.0)
        .setTarget(new NodeBuilder()
            .setLabel("C")
            .setWeight(6.0)
            .build())
        .build())
    .build();
    driver.withInputValue(new AvroValue<Node>(node));

    node = new NodeBuilder()
    .setLabel("A")
    .setWeight(1.0)
    .addEdge(new EdgeBuilder()
        .setLabel("f")
        .setWeight(1.0)
        .setTarget(new NodeBuilder()
            .setLabel("C")
            .setWeight(7.0)
            .build())
        .build())
    .build();
    driver.withInputValue(new AvroValue<Node>(node));

    node = new NodeBuilder()
    .setLabel("A")
    .setWeight(1.0)
    .addEdge(new EdgeBuilder()
        .setLabel("d")
        .setWeight(4.0)
        .setTarget(new NodeBuilder()
            .setLabel("E")
            .setWeight(5.0)
            .build())
        .build())
    .build();
    driver.withInputValue(new AvroValue<Node>(node));

    //
    // A/4.0 ----b/4.0---> C/9.0
    //        \---d/4.0---> E/5.0
    //        \---f/1.0---> C/7.0
    //
    List<Pair<Text, AvroValue<Node>>> actual = driver.run();
    assertEquals(1, actual.size());
    assertEquals("A", actual.get(0).getFirst().toString());
    Node actualNode = actual.get(0).getSecond().datum();
    assertNotNull(actualNode);
    assertEquals("A", actualNode.getLabel().toString());
    assertEquals(4.0, actualNode.getWeight(), 1e-8);
    assertEquals(3, actualNode.getEdges().size());
    assertEquals("b", actualNode.getEdges().get(0).getLabel().toString());
    assertEquals(4.0, actualNode.getEdges().get(0).getWeight(), 1e-8);
    assertEquals("C", actualNode.getEdges().get(0).getTarget().getLabel().toString());
    assertEquals(9.0, actualNode.getEdges().get(0).getTarget().getWeight(), 1e-8);
    assertEquals("d", actualNode.getEdges().get(1).getLabel().toString());
    assertEquals(4.0, actualNode.getEdges().get(1).getWeight(), 1e-8);
    assertEquals("E", actualNode.getEdges().get(1).getTarget().getLabel().toString());
    assertEquals(5.0, actualNode.getEdges().get(1).getTarget().getWeight(), 1e-8);
    assertEquals("f", actualNode.getEdges().get(2).getLabel().toString());
    assertEquals(1.0, actualNode.getEdges().get(2).getWeight(), 1e-8);
    assertEquals("C", actualNode.getEdges().get(2).getTarget().getLabel().toString());
    assertEquals(7.0, actualNode.getEdges().get(2).getTarget().getWeight(), 1e-8);
  }
}

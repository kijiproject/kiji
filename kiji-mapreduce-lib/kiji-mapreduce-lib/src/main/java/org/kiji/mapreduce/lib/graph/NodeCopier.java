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

package org.kiji.mapreduce.lib.graph;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.io.DataInputBuffer;

import org.kiji.mapreduce.lib.avro.Node;

/**
 * Efficiently perform multiple deep-copies of Nodes.
 */
public class NodeCopier {
  // Reusable components for copying nodes.
  private ByteArrayOutputStream mOutputStream;
  private Encoder mEncoder;
  private SpecificDatumWriter<Node> mWriter;

  private DataInputBuffer mDataBuffer;
  private Decoder mDecoder;
  private SpecificDatumReader<Node> mReader;

  /**
   * Creates a new node copier.
   */
  public NodeCopier() {
    mOutputStream = new ByteArrayOutputStream();
    mEncoder = EncoderFactory.get().directBinaryEncoder(mOutputStream, null);
    mWriter = new SpecificDatumWriter<Node>(Node.SCHEMA$);

    mDataBuffer = new DataInputBuffer();
    mDecoder = DecoderFactory.get().binaryDecoder(mDataBuffer, null);
    mReader = new SpecificDatumReader<Node>(Node.SCHEMA$);
  }

  /**
   * Copies a node.
   *
   * @param node The node to copy.
   * @return A deep copy of the given node.
   */
  public Node copy(Node node) {
    try {
      mOutputStream.reset();
      mWriter.write(node, mEncoder);

      byte[] outBytes = mOutputStream.toByteArray();
      mDataBuffer.reset(outBytes, outBytes.length);
      return mReader.read(null, mDecoder);
    } catch (IOException e) {
      throw new RuntimeException("Unable to copy node.");
    }
  }
}

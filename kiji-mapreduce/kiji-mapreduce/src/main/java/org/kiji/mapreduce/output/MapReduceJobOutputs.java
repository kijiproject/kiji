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

package org.kiji.mapreduce.output;

import org.apache.hadoop.fs.Path;

import org.kiji.schema.KijiURI;

/**
 * Utility class for instantiating MapReduceJobOutputs.
 */
public final class MapReduceJobOutputs {

  /** Utility classes may not be instantiated. */
  private MapReduceJobOutputs() { }

  /**
   * Creates a new uninitialized HFileMapReduceJobOutput.
   * @return a new uninitialized HFileMapReduceJobOutput.
   */
  public static HFileMapReduceJobOutput newHFileMapReduceJobOutput() {
    return new HFileMapReduceJobOutput();
  }

  /**
   * Creates a new HFileMapReduceJobOutput.
   * @param tableURI The kiji table the resulting HFiles are intended for.
   * @param path The directory path to output the HFiles to.
   * @return a new HFileMapReduceJobOutput initialized with the intended kiji table and
   * output directory path.
   */
  public static HFileMapReduceJobOutput newHFileMapReduceJobOutput(KijiURI tableURI, Path path) {
    return new HFileMapReduceJobOutput(tableURI, path);
  }

  /**
   * Creates a new HFileMapReduceJobOutput.
   * @param tableURI The kiji table the resulting HFiles are intended for.
   * @param path The directory path to output the HFiles to.
   * @param numSplits Number of splits (determines the number of reduce tasks).
   * @return a new HFileMapReduceJobOutput initialized with the intended kiji table, output
   * directory path, and number of splits (hashRowKeys-enabled Kiji table only).
   */
  public static HFileMapReduceJobOutput newHFileMapReduceJobOutput(
      KijiURI tableURI, Path path, int numSplits) {
    return new HFileMapReduceJobOutput(tableURI, path, numSplits);
  }

  /**
   * Creates a new uninitialized TextMapReduceJobOutput.
   * @return a new uninitialized TextMapReduceJobOutput.
   */
  public static TextMapReduceJobOutput newTextMapReduceJobOutput() {
    return new TextMapReduceJobOutput();
  }

  /**
   * Creates a new TextMapReduceJobOutput.
   * @param filePath Path to output folder.
   * @param numSplits Number of output file splits.
   * @return a new TextMapReduceJobOutput initialized with the output folder path and number of
   * file splits.
   */
  public static TextMapReduceJobOutput newTextMapReduceJobOutput(Path filePath, int numSplits) {
    return new TextMapReduceJobOutput(filePath, numSplits);
  }

  /**
   * Creates a new uninitialized DirectKijiTableMapReduceJobOutput.
   * @return a new uninitialized DirectKijiTableMapReduceJobOutput.
   */
  public static DirectKijiTableMapReduceJobOutput newDirectKijiTableMapReduceJobOutput() {
    return new DirectKijiTableMapReduceJobOutput();
  }

  /**
   * Creates a new DirectKijiTableMapReduceJobOutput with the Kiji table only.
   * @param tableURI The Kiji table to write to.
   * @return a new DirectKijiTableMapReduceJobOutput initialized with a Kiji Table URI.
   */
  public static DirectKijiTableMapReduceJobOutput newDirectKijiTableMapReduceJobOutput(
      KijiURI tableURI) {
    return new DirectKijiTableMapReduceJobOutput(tableURI);
  }

  /**
   * Creates a new DirectKijiTableMapReduceJobOutput.
   * @param tableURI The Kiji table to write to.
   * @param numReduceTasks The number of reduce tasks to use (use zero if using a producer).
   * @return a new DirectKijiTableMapReduceJobOutput initialized with a Kiji Table URI and
   * the number of reduce tasks.
   */
  public static DirectKijiTableMapReduceJobOutput newDirectKijiTableMapReduceJobOutput(
      KijiURI tableURI, int numReduceTasks) {
    return new DirectKijiTableMapReduceJobOutput(tableURI, numReduceTasks);
  }

  /**
   * Creates a new uninitialized SequenceFileMapReduceJobOutput.
   * @return a new uninitialized SequenceFileMapReduceJobOutput.
   */
  public static SequenceFileMapReduceJobOutput newSequenceFileMapReduceJobOutput() {
    return new SequenceFileMapReduceJobOutput();
  }

  /**
   * Creates a new SequenceFileMapReduceJobOutput.
   * @param filePath The file system path for the output files.
   * @param numSplits The number of output file splits.
   * @return a new SequenceFileMapReduceJobOutput initialized with the output file system path and
   * number of file splits.
   */
  public static SequenceFileMapReduceJobOutput newSequenceFileMapReduceJobOutput(
      Path filePath, int numSplits) {
    return new SequenceFileMapReduceJobOutput(filePath, numSplits);
  }

  /**
   * Creates a new uninitialized AvroKeyMapReduceJobOutput.
   * @return a new uninitialized AvroKeyMapReduceJobOutput.
   */
  public static AvroKeyMapReduceJobOutput newAvroKeyMapReduceJobOutput() {
    return new AvroKeyMapReduceJobOutput();
  }

  /**
   * Creates a new AvroKeyMapReduceJobOutput.
   * @param filePath Path to output folder.
   * @param numSplits Number of output file splits.
   * @return a new AvroKeyMapReduceJobOutput initialized with the output folder path and
   * number of file splits.
   */
  public static AvroKeyMapReduceJobOutput newAvroKeyMapReduceJobOutput(
      Path filePath, int numSplits) {
    return new AvroKeyMapReduceJobOutput(filePath, numSplits);
  }

  /**
   * Creates a new uninitialized AvroKeyValueMapReduceJobOutput.
   * @return a new uninitialized AvroKeyValueMapReduceJobOutput.
   */
  public static AvroKeyValueMapReduceJobOutput newAvroKeyValueMapReduceJobOutput() {
    return new AvroKeyValueMapReduceJobOutput();
  }

  /**
   * Creates a new AvroKeyValueMapReduceJobOutput.
   * @param filePath The file system path for the output files.
   * @param numSplits The number of output file splits.
   * @return a new AvroKeyValueMapReduceJobOutput initialized with the output folder path and
   * number of file splits.
   */
  public static AvroKeyValueMapReduceJobOutput newAvroKeyValueMapReduceJobOutput(
      Path filePath, int numSplits) {
    return new AvroKeyValueMapReduceJobOutput(filePath, numSplits);
  }

  /**
   * Creates a new uninitialized MapFileMapReduceJobOutput.
   * @return a new uninitialized MapFileMapReduceJobOutput.
   */
  public static MapFileMapReduceJobOutput newMapFileMapReduceJobOutput() {
    return new MapFileMapReduceJobOutput();
  }

  /**
   * Creates a new MapFileMapReduceJobOutput.
   * @param filePath Path to output folder.
   * @param numSplits Number of output file splits.
   * @return a new MapFileMapReduceJobOutput initialized with the output folder path and
   * number of file splits.
   */
  public static MapFileMapReduceJobOutput newMapFileMapReduceJobOutput(
      Path filePath, int numSplits) {
    return new MapFileMapReduceJobOutput(filePath, numSplits);
  }
}

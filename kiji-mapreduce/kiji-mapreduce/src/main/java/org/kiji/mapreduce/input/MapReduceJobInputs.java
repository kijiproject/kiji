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

package org.kiji.mapreduce.input;

import org.apache.hadoop.fs.Path;

import org.kiji.mapreduce.input.KijiTableMapReduceJobInput.RowOptions;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiURI;

/**
 * Utility class for instantiating MapReduceJobInputs.
 */
public final class MapReduceJobInputs {

  /** Utility classes may not be instantiated. */
  private MapReduceJobInputs() { }

  /**
   * Create a new uninitialized HTableMapReduceJobInput.
   * @return a new uninitialized HTableMapReduceJobInput.
   */
  public static HTableMapReduceJobInput newHTableMapReduceJobInput() {
    return new HTableMapReduceJobInput();
  }

  /**
   * Create a new HTableMapReduceJobInput.
   * @param tableName the name of the HTable.
   * @return a new HTableMapReduceJobInput initialized with the name of the HTable.
   */
  public static HTableMapReduceJobInput newHTableMapReduceJobInput(String tableName) {
    return new HTableMapReduceJobInput(tableName);
  }

  /**
   * Create a new uninitialized TextMapReduceJobInput.
   * @return a new uninitialized TextMapReduceJobInput.
   */
  public static TextMapReduceJobInput newTextMapReduceJobInput() {
    return new TextMapReduceJobInput();
  }

  /**
   * Create a new TextMapReduceJobInput.
   * @param paths the file system paths to source files.
   * @return a new TextMapReduceJobInput configured with the given source file paths.
   */
  public static TextMapReduceJobInput newTextMapReduceJobInput(Path... paths) {
    return new TextMapReduceJobInput(paths);
  }

  /**
   * Create a new uninitialized WholeTextFileMapReduceJobInput.
   * @return a new uninitialized WholeTextFileMapReduceJobInput.
   */
  public static WholeTextFileMapReduceJobInput newWholeTextFileMapReduceJobInput() {
    return new WholeTextFileMapReduceJobInput();
  }

  /**
   * Create a new WholeTextFileMapReduceJobInput.
   * @param paths the file system paths to source files.
   * @return a new WholeTextFileMapReduceJobInput configured with the given source file paths.
   */
  public static WholeTextFileMapReduceJobInput newWholeTextFileMapReduceJobInput(Path... paths) {
    return new WholeTextFileMapReduceJobInput(paths);
  }

  /**
   * Create a new uninitialized SequenceFileMapReduceJobInput.
   * @return a new uninitialized SequenceFileMapReduceJobInput.
   */
  public static SequenceFileMapReduceJobInput newSequenceFileMapReduceJobInput() {
    return new SequenceFileMapReduceJobInput();
  }

  /**
   * Create a new SequenceFileMapReduceJobInput.
   * @param paths the file system paths to source files.
   * @return a new SequenceFileMapReduceJobInput configured with the given source file paths.
   */
  public static SequenceFileMapReduceJobInput newSequenceFileMapReduceJobInput(Path... paths) {
    return new SequenceFileMapReduceJobInput(paths);
  }

  /**
   * Create a new uninitialized AvroKeyMapReduceJobInput.
   * @return a new uninitialized AvroKeyMapReduceJobInput.
   */
  public static AvroKeyMapReduceJobInput newAvroKeyMapReduceJobInput() {
    return new AvroKeyMapReduceJobInput();
  }

  /**
   * Create a new AvroKeyMapReduceJobInput.
   * @param paths the file system paths to source files.
   * @return a new AvroKeyMapReduceJobInput configured with the given source file paths.
   */
  public static AvroKeyMapReduceJobInput newAvroKeyMapReduceJobInput(Path... paths) {
    return new AvroKeyMapReduceJobInput(paths);
  }

  /**
   * Create a new uninitialized AvroKeyValueMapReduceJobInput.
   * @return a new uninitialized AvroKeyValueMapReduceJobInput.
   */
  public static AvroKeyValueMapReduceJobInput newAvroKeyValueMapReduceJobInput() {
    return new AvroKeyValueMapReduceJobInput();
  }

  /**
   * Create a new AvroKeyValueMapReduceJobInput.
   * @param paths the file system paths to source files.
   * @return a new AvroKeyValueMapReduceJobInput configured with the given source file paths.
   */
  public static AvroKeyValueMapReduceJobInput newAvroKeyValueMapReduceJobInput(Path... paths) {
    return new AvroKeyValueMapReduceJobInput(paths);
  }

  /**
   * Create a new uninitialized KijiTableMapReduceJobInput.
   * @return a new uninitialized KijiTableMapReduceJobInput.
   */
  public static KijiTableMapReduceJobInput newKijiTableMapReduceJobInput() {
    return new KijiTableMapReduceJobInput();
  }

  /**
   * Create a new KijiTableMapReduceJobInput.
   * @param inputTableURI the KijiURI of the input table.
   * @param dataRequest the KijiDataRequest specifying columns to be used as input.
   * @param rowOptions options describing row ranges and filters.
   * @return a new KijiTableMapReduceJobInput configured with the given input table, data request,
   * and row options.
   */
  public static KijiTableMapReduceJobInput newKijiTableMapReduceJobInput(
      KijiURI inputTableURI, KijiDataRequest dataRequest, RowOptions rowOptions) {
    return new KijiTableMapReduceJobInput(inputTableURI, dataRequest, rowOptions);
  }

  /**
   * Create a new uninitialized XMLMapReduceJobInput.
   * @return a new uninitialized XMLMapReduceJobInput.
   */
  public static XMLMapReduceJobInput newXMLMapReduceJobInput() {
    return new XMLMapReduceJobInput();
  }

  /**
   * Create a new XMLMapReduceJobInput.
   * @param paths the file system paths to source files.
   * @return a new XMLMapReduceJobInput configured with the given source file paths.
   */
  public static XMLMapReduceJobInput newXMLMapReduceJobInput(Path... paths) {
    return new XMLMapReduceJobInput(paths);
  }
}

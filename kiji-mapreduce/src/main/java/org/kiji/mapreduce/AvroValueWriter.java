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

package org.kiji.mapreduce;

import java.io.IOException;

import org.apache.avro.Schema;

/**
 * Kiji analytic functions (mappers, reducers, producer, gatherers, etc.) that output
 * {@link org.apache.avro.mapred.AvroValue} objects are required to implement this interface
 * as a means of specifying the Avro writer schema for their output value data.
 */
public interface AvroValueWriter {
  /**
   * If the output key class is {@link org.apache.avro.mapred.AvroValue}, the Kiji framework
   * will call this method to determine what the wrapped datum's writer schema is.
   *
   * <p>If this class does not use an AvroKey as its output key class, the return value from
   * this method will not be used.  It may be null.</p>
   *
   * @throws IOException If there is an error.
   * @return The Avro schema of the datum wrapped in the AvroValue output.
   */
  Schema getAvroValueWriterSchema() throws IOException;
}

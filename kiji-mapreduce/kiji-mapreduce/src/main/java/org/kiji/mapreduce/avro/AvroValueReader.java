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

package org.kiji.mapreduce.avro;

import java.io.IOException;

import org.apache.avro.Schema;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.Inheritance;

/**
 * Kiji analytic functions (mappers, reducers, producer, gatherers, etc.) that read
 * {@link org.apache.avro.mapred.AvroValue} objects are required to implement this interface
 * as a means of specifying the Avro reader schema for their input value data.
 */
@ApiAudience.Public
@Inheritance.Extensible
public interface AvroValueReader {
  /**
   * If the input value class is {@link org.apache.avro.mapred.AvroValue}, this method
   * returns the Avro reader schema of the wrapped datum; otherwise null.
   *
   * @throws IOException If there is an error.
   * @return The Avro reader schema of the datum wrapped in the AvroValue input.
   */
  Schema getAvroValueReaderSchema() throws IOException;
}

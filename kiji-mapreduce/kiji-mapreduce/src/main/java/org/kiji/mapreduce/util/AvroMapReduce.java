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

package org.kiji.mapreduce.util;

import java.io.IOException;

import org.apache.avro.Schema;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.avro.AvroKeyReader;
import org.kiji.mapreduce.avro.AvroKeyWriter;
import org.kiji.mapreduce.avro.AvroValueReader;
import org.kiji.mapreduce.avro.AvroValueWriter;

/** A utility class for working with MapReduce jobs that use Avro for input or output data. */
@ApiAudience.Private
public final class AvroMapReduce {
  /** Constructor disabled for this utility class. */
  private AvroMapReduce() {}

  /**
   * If the given object implements AvroKeyReader, return the reader schema for its
   * AvroKey; otherwise return null.
   *
   * @param obj The target object.
   * @throws IOException If there is an error.
   * @return The reader schema for the key Avro datum <code>obj</code> reads.
   */
  public static Schema getAvroKeyReaderSchema(Object obj) throws IOException {
    if (!(obj instanceof AvroKeyReader)) {
      return null;
    }
    return ((AvroKeyReader) obj).getAvroKeyReaderSchema();
  }

  /**
   * If the given object implements AvroValueReader, return the reader schema for its
   * AvroValue; otherwise return null.
   *
   * @param obj The target object.
   * @throws IOException If there is an error.
   * @return The reader schema for the value Avro datum <code>obj</code> reads.
   */
  public static Schema getAvroValueReaderSchema(Object obj)
      throws IOException {
    if (!(obj instanceof AvroValueReader)) {
      return null;
    }
    return ((AvroValueReader) obj).getAvroValueReaderSchema();
  }

  /**
   * If the given object implements AvroKeyWriter, return the writer schema for its
   * AvroKey; otherwise return null.
   *
   * @param obj The target object.
   * @throws IOException If there is an error.
   * @return The writer schema for the key Avro datum <code>obj</code> writes.
   */
  public static Schema getAvroKeyWriterSchema(Object obj) throws IOException {
    if (!(obj instanceof AvroKeyWriter)) {
      return null;
    }
    return ((AvroKeyWriter) obj).getAvroKeyWriterSchema();
  }

  /**
   * If the given object implements AvroValueWriter, return the writer schema for its
   * AvroValue; otherwise return null.
   *
   * @param obj The target object.
   * @throws IOException If there is an error.
   * @return The writer schema for the value Avro datum <code>obj</code> writes.
   */
  public static Schema getAvroValueWriterSchema(Object obj)
      throws IOException {
    if (!(obj instanceof AvroValueWriter)) {
      return null;
    }
    return ((AvroValueWriter) obj).getAvroValueWriterSchema();
  }
}

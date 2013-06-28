#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
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

package ${package}.reduce;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;

import org.kiji.annotations.ApiAudience;
import ${package}.ExampleRecord;
import org.kiji.mapreduce.KijiReducer;
import org.kiji.mapreduce.avro.AvroKeyReader;
import org.kiji.mapreduce.avro.AvroValueReader;
import org.kiji.mapreduce.avro.AvroKeyWriter;
import org.kiji.mapreduce.avro.AvroValueWriter;

/**
 * This MapReduce reducer will pass through all of the input key-value pairs unchanged.
 *
 * <p>
 * The key and value types here are both the Avro record ExampleRecord.
 * </p>
 *
 * <p>
 * To implement your own Reducer, extend KijiReducer with the type parameters for your input
 * and output keys and values, and override the reduce method.  You can also override the
 * <code>setup</code> and <code>cleanup</code> methods if necessary.
 * </p>
 *
 * <p>
 * If your any of your input or output key or value types aren't Avro records,
 * you don't need to implement the AvroKeyReader, AvroKeyWriter, etc, interfaces.
 * </p>
 */
@ApiAudience.Public
public final class ExampleIdentityReducer
    extends KijiReducer<
        AvroKey<ExampleRecord>,
        AvroValue<ExampleRecord>,
        AvroKey<ExampleRecord>,
        AvroValue<ExampleRecord>>
    implements AvroKeyReader, AvroValueReader, AvroKeyWriter, AvroValueWriter {

  /** {@inheritDoc} */
  @Override
  protected void reduce(
        AvroKey<ExampleRecord> key,
        Iterable<AvroValue<ExampleRecord>> values,
        Context context) throws IOException, InterruptedException {
    for (AvroValue<ExampleRecord> value : values) {
      context.write(key, value);
    }
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputKeyClass() {
    return AvroKey.class;
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputValueClass() {
    return AvroValue.class;
  }

  /** {@inheritDoc} */
  @Override
  public Schema getAvroKeyReaderSchema() throws IOException {
    return ExampleRecord.SCHEMA${symbol_dollar};
  }

  /** {@inheritDoc} */
  @Override
  public Schema getAvroValueReaderSchema() throws IOException {
    return ExampleRecord.SCHEMA${symbol_dollar};
  }

  /** {@inheritDoc} */
  @Override
  public Schema getAvroKeyWriterSchema() throws IOException {
    return ExampleRecord.SCHEMA${symbol_dollar};
  }

  /** {@inheritDoc} */
  @Override
  public Schema getAvroValueWriterSchema() throws IOException {
    return ExampleRecord.SCHEMA${symbol_dollar};
  }
}

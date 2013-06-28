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

package ${package}.gather;

import java.io.IOException;
import java.util.NavigableMap;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ${package}.ExampleRecord;
import org.kiji.mapreduce.avro.AvroKeyWriter;
import org.kiji.mapreduce.avro.AvroValueWriter;
import org.kiji.mapreduce.gather.KijiGatherer;
import org.kiji.mapreduce.gather.GathererContext;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;

/**
 * Example gatherer.
 *
 * <p>
 * This gatherer emits a pair of the input text and the number 1. This is a pattern
 * often used while counting.
 * </p>
 * <p>
 * Change the <code>getDataRequest()</code> method to change the column this gatherer uses as input.
 * Change the <code>gather()</code> method to change what kind of output this gatherer produces.
 * </p>
 * <p>
 * This example gatherer implements both AvroKeyWriter and AvroValueWriter, but only the Key is
 * an Avro class.  The AvroValueWriter.getAvroValueWriterSchema() method is a dummy implementation
 * that you will have to edit if your output value is an Avro record.  If your keys or values are
 * not Avro classes, you can remove the AvroKeyWriter and AvroValueWriter interfaces, respectively.
 * </p>
 */
public class ExampleGatherer
    extends KijiGatherer<AvroKey<ExampleRecord>, LongWritable>
    implements AvroKeyWriter, AvroValueWriter {
  private static final Logger LOG = LoggerFactory.getLogger(ExampleGatherer.class);

  /** The family:qualifier of the column to read in. **/
  private static final String COLUMN_FAMILY = "your_column_family";
  private static final String COLUMN_QUALIFIER = "your_column_qualifier";

  /** Only keep one ExampleRecord around to reduce the chance of a garbage collection pause.*/
  private ExampleRecord mRecord;
  /** Only keep one LongWritable object around to reduce the chance of a garbage collection pause.*/
  private static final LongWritable ONE = new LongWritable(1);

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputKeyClass() {
    // Since our output key class is AvroKey, we also have to override
    // getAvroKeyWriterSchema() below to specify the exact schema to use.
    return AvroKey.class;
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputValueClass() {
    // TODO: If you want the value to be AvroValue, you will have to override
    // getAvroValueWriterSchema() below to specify the exact schema to use.
    return LongWritable.class;
  }

  /** {@inheritDoc} */
  @Override
  public void setup(
      GathererContext<AvroKey<ExampleRecord>,
      LongWritable> context) throws IOException {
    super.setup(context); // Any time you override setup, call super.setup(context);
    mRecord = new ExampleRecord();
  }

  /** {@inheritDoc} */
  @Override
  public KijiDataRequest getDataRequest() {
    // This method is how we specify which columns in each row the gatherer operates on.
    // In this case, we need all versions of the info:track_plays column.
    final KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef()
        .withMaxVersions(HConstants.ALL_VERSIONS) // Retrieve all versions.
        .add(COLUMN_FAMILY, COLUMN_QUALIFIER);
    return builder.build();
  }

  /** {@inheritDoc} */
  @Override
  public void gather(KijiRowData row, GathererContext<AvroKey<ExampleRecord>, LongWritable> context)
      throws IOException {
    // TODO: Implement the body of your gather method here.
    // This gatherer emits a pair of the key and the number 1, which is a pattern often used
    // when counting.
    NavigableMap<Long, CharSequence> counts = row.getValues(COLUMN_FAMILY, COLUMN_QUALIFIER);
    for (CharSequence key : counts.values()) {
      mRecord.setId(key);
      context.write(new AvroKey<ExampleRecord>(mRecord), ONE);
    }

  }

  /** {@inheritDoc} */
  @Override
  public Schema getAvroKeyWriterSchema() {
    // Since the class of our key is AvroKey, we specify here which schema to use.
    return ExampleRecord.SCHEMA${symbol_dollar};
  }

  /** {@inheritDoc} */
  @Override
  public Schema getAvroValueWriterSchema() {
    // TODO: If getOutputValueClass returns AvroValue, you must override this method
    // to provide the writer schema of the value class.
    return null;
  }
}

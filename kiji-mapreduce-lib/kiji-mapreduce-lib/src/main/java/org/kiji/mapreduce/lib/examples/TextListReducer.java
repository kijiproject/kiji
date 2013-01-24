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

package org.kiji.mapreduce.lib.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

import org.kiji.mapreduce.AvroValueWriter;
import org.kiji.mapreduce.KijiBaseReducer;

/**
 * A reducer that aggregates its text input values into a list.
 *
 * <p>The MapReduce framework will group all output records from the map phase with the
 * same key.  A call to the <code>reduce()</code> method will be made for each input
 * group.  This reducer will aggregate all the values within a group into a single list to
 * be output as the value.  Avro is used to output the complex list value.</p>
 *
 * @param <K> The input key type, which will also be used as the output key type.
 */
public class TextListReducer<K> extends KijiBaseReducer<K, Text, K, AvroValue<List<CharSequence>>>
    implements Configurable, AvroValueWriter {
  /** The job configuration. */
  private Configuration mConf;

  /** A reusable AvroValue to be used for writing output. */
  private AvroValue<List<CharSequence>> mValue;

  /** {@inheritDoc} */
  @Override
  public void setConf(Configuration conf) {
    mConf = conf;
  }

  /** {@inheritDoc} */
  @Override
  public Configuration getConf() {
    return mConf;
  }

  /** {@inheritDoc} */
  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    // Initialize a reusable AvroValue object to be used for writing the output.
    mValue = new AvroValue<List<CharSequence>>();
  }

  /** {@inheritDoc} */
  @Override
  public void reduce(K key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    // Construct a list to hold all of the values in the group.
    List<CharSequence> aggregateList = new ArrayList<CharSequence>();

    // Add each input value to our aggregate list.
    for (Text value : values) {
      aggregateList.add(value.toString());
    }

    // Put the aggregate list into the wrapper AvroValue to be written.
    mValue.datum(aggregateList);

    // Write the output key and its aggregate list of values.
    context.write(key, mValue);
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputKeyClass() {
    // This reducer passes the input key straight on to the output, so we'll set the
    // output key class to match whatever the map's output key class was.
    return new JobConf(getConf()).getMapOutputKeyClass();
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputValueClass() {
    // We will use Avro to output list of strings in the value record.
    return AvroValue.class;
  }

  /** {@inheritDoc} */
  @Override
  public Schema getAvroValueWriterSchema() {
    // Since we use Avro for the output value record, here we specify its schema.
    return Schema.createArray(Schema.create(Schema.Type.STRING));
  }
}

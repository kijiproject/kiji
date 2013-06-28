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

package ${package}.produce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;

import org.kiji.hadoop.configurator.HadoopConf;
import org.kiji.hadoop.configurator.HadoopConfigurator;
import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.mapreduce.produce.ProducerContext;
import org.kiji.schema.DecodedCell;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.NoSuchColumnException;

/**
 * This producer copies data from the column "family:qualifier" to the column
 * "family:outputqualifier" without modification, assuming both are encoded as bytes.
 */
public class ExampleProducer extends KijiProducer {
  private KijiColumnName mInputColumn = new KijiColumnName("family:qualifier");
  private KijiColumnName mOutputColumn = new KijiColumnName("family:outputqualifier");
  private Schema schema = Schema.create(Schema.Type.BYTES);

  /** {@inheritDoc} */
  @Override
  public KijiDataRequest getDataRequest() {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().withMaxVersions(Integer.MAX_VALUE)
        .add(mInputColumn.getFamily(), mInputColumn.getQualifier());
    return builder.build();
  }

  /** {@inheritDoc} */
  @Override
  public String getOutputColumn() {
    return mOutputColumn.toString();
  }

  /** {@inheritDoc} */
  @Override
  public void produce(KijiRowData input, ProducerContext context)
      throws IOException {
    for (long timestamp : input.getTimestamps(
        mInputColumn.getFamily(),
        mInputColumn.getQualifier())) {
      Object data = input.getValue(
          mInputColumn.getFamily(), mInputColumn.getQualifier(), timestamp);
      context.put(timestamp, new DecodedCell<Object>(schema, data));
    }
  }
}

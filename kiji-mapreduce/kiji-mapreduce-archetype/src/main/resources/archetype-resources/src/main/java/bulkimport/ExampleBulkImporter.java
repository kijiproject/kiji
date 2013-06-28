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

package ${package}.bulkimport;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.kiji.mapreduce.KijiTableContext;
import org.kiji.mapreduce.bulkimport.KijiBulkImporter;
import org.kiji.schema.EntityId;

/**
 * This example bulk importer parses colon-delimited mappings of strings to integers.
 *
 * To write your own bulk importer class, change the type parameters to those of your key
 * and value, and override the produce method, using methods of <code>context</code> to write
 * your results.
 */
public class ExampleBulkImporter extends KijiBulkImporter<LongWritable, Text> {
  /** {@inheritDoc} */
  @Override
  public void setup(KijiTableContext context) throws IOException {
    // TODO: Perform any setup you need here.
    super.setup(context);
  }

  /** {@inheritDoc} */
  @Override
   public void produce(LongWritable filePos, Text value, KijiTableContext context)
       throws IOException {
     final String[] split = value.toString().split(":");
     final String rowKey = split[0];
     final int integerValue = Integer.parseInt(split[1]);

     final EntityId eid = context.getEntityId(rowKey);
     context.put(eid, "primitives", "int", integerValue);
   }

  /** {@inheritDoc} */
  @Override
  public void cleanup(KijiTableContext context) throws IOException {
    // TODO: Perform any cleanup you need here.
    super.cleanup(context);
  }
}

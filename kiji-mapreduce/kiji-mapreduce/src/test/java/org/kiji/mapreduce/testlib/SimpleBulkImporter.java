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

package org.kiji.mapreduce.testlib;

import java.io.IOException;

import com.google.common.base.Preconditions;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.kiji.mapreduce.KijiTableContext;
import org.kiji.mapreduce.bulkimport.KijiBulkImporter;
import org.kiji.schema.EntityId;

/**
 * Simple bulk-importer for testing.
 *
 * Reads a text file formatted as "rowKey:integerValue"
 * and emits:
 *  <li> the integer value for the specified row at column "primitives:int";
 *  <li> the position of the row input in the text file at column "primitives:long";
 *  <li> a string "rowKey-integerValue" at column "primitives:string".
 *
 * Expects the layout described in src/test/resources/org/kiji/mapreduce/layout/test.json
 */
public class SimpleBulkImporter extends KijiBulkImporter<LongWritable, Text> {

  /** {@inheritDoc} */
  @Override
  public void produce(LongWritable filePos, Text value, KijiTableContext context)
      throws IOException {
    final String line = value.toString();
    final String[] split = line.split(":");
    Preconditions.checkState(split.length == 2,
        String.format("Unable to parse bulk-import test input line: '%s'.", line));
    final String rowKey = split[0];
    final int integerValue = Integer.parseInt(split[1]);

    final EntityId eid = context.getEntityId(rowKey);
    context.put(eid, "primitives", "int", integerValue);
    context.put(eid, "primitives", "long", filePos.get());
    context.put(eid, "primitives", "string", String.format("%s-%d", rowKey, integerValue));
  }
}

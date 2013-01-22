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

package org.kiji.mapreduce.bulkimport;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.Inheritance;
import org.kiji.mapreduce.KijiBulkImporter;
import org.kiji.mapreduce.KijiTableContext;

/**
 * <p>Use this Mapper over text files to import data into a Kiji
 * table.  Each line in the file will be treated data for one row.
 * This line should generate a single EntityId to write to, and any number
 * of writes to add to that entity.  Override the produce(String, Context)
 * method with this behavior.</p>
 */
@ApiAudience.Public
@Inheritance.Extensible
public abstract class BaseTextBulkImporter extends KijiBulkImporter<LongWritable, Text> {
  private static final Logger LOG = LoggerFactory.getLogger(BaseTextBulkImporter.class);

  /**
   * Converts a line of text to a set of writes to <code>context</code>, and
   * an EntityId for the row.
   *
   * @param line The line to parse.
   * @param context The context to write to.
   * @throws IOException if there is an error.
   */
  public abstract void produce(Text line, KijiTableContext context)
      throws IOException;

  /** {@inheritDoc} */
  @Override
  public void produce(LongWritable fileOffset, Text line, KijiTableContext context)
      throws IOException {
    produce(line, context);
  }
}

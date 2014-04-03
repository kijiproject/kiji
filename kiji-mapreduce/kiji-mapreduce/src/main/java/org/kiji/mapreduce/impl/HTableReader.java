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

package org.kiji.mapreduce.impl;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;

import org.kiji.annotations.ApiAudience;

/**
 * An interface for Kiji mappers or bulk importers that read from HBase HTables.
 *
 * <p>Reading from an HTable requires knowing what subset of rows and columns should be
 * read (accomplished by configuring a Scan descriptor).</p>
 *
 * <p>If your mapper or bulk importer uses an HTableInputFormat, you should implement this
 * interface as a means of specifying the input HTable scan descriptor.</p>
 */
@ApiAudience.Private
public interface HTableReader {
  /**
   * Returns an HBase scan descriptor that specifies what subset of rows and cells should
   * be read from the input HTable.
   *
   * <p>This method may return null, meaning that this instance is not really able to read
   * from an HTable.</p>
   *
   * @param conf The job configuration.
   * @return A new scan descriptor.
   * @throws IOException If the scan descriptor cannot be constructed.
   */
  Scan getInputHTableScan(Configuration conf) throws IOException;
}

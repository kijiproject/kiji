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

package org.kiji.mapreduce.tools;

/** Configuration keys for Job input and output specifications. */
public final class JobIOConfKeys {
  /** Format of the input or the ouptut. */
  public static final String FORMAT_KEY = "format";

  /** Number of splits for the output files. Zero means automatic. */
  public static final String NSPLITS_KEY = "nsplits";

  /**
   * Input or output file path, usually an hdfs:// URI.
   *
   * As an output path, this must specify exactly one file.
   * As an input path, this may specify multiple files, separated by commas.
   */
  public static final String FILE_PATH_KEY = "file";

  /** Input or output Kiji table, as a kiji:// URI. */
  public static final String TABLE_KEY = "table";

  /**
   * Name of the input HTable, for HTableMapReduceJobInput.
   *
   * Actual HBase cluster address is pulled from the Job configuration.
   */
  public static final String HTABLE_NAME_KEY = "htable";

  /** Utility class may not be instantiated. */
  private JobIOConfKeys() {
  }
}

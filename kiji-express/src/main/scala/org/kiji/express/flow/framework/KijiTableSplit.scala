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

package org.kiji.express.flow.framework

import java.io.DataInput
import java.io.DataOutput

import org.apache.hadoop.hbase.mapreduce.TableSplit
import org.apache.hadoop.mapred.InputSplit

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability

/**
 * An input split that specifies a region of rows from a Kiji table to be processed by a MapReduce
 * task. This input split stores exactly the same data as `TableSplit`, but does a better job
 * handling default split sizes.
 *
 * @param split to which functionality is delegated.
 */
@ApiAudience.Private
@ApiStability.Experimental
final private[express] class KijiTableSplit(
    split: TableSplit)
    extends InputSplit {
  /**
   * No argument constructor for KijiTableSplit so that it can be constructed via reflection. This
   * is required to be a separate constructor so that java has access to it.
   */
  // scalastyle:off public.methods.have.type
  def this() = this(new TableSplit())
  // scalastyle:on public.methods.have.type

  /**
   * Returns the length of the split.
   * TODO: EXP-180: KijiTableSplit should return a better estimate in getLength
   *
   * @return the length of the split.
   * @see org.apache.hadoop.mapreduce.InputSplit#getLength()
   */
  override def getLength(): Long = split.getLength()

  /**
   * Get the list of hostnames where the input split is located.
   *
   * @return a list of hostnames defining this input split.
   */
  override def getLocations(): Array[String] = split.getLocations()

  /**
   * Implementation of Hadoop's writable deserialization.
   *
   * @param input data stream containing data to populate this split with.
   */
  override def readFields(input: DataInput) {
    split.readFields(input)
  }

  /**
   * Implementation of Hadoop's writable serialization.
   *
   * @param output data stream to populate with the data this split contains.
   */
  override def write(output: DataOutput) {
    split.write(output)
  }

  /**
   * @return the HBase row key of the first row processed by this input split.
   */
  def getStartRow(): Array[Byte] = split.getStartRow()

  /**
   * @return the HBase row key of the last row processed by this input split.
   */
  def getEndRow(): Array[Byte] = split.getEndRow()
}

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

package org.kiji.chopsticks

import java.io.DataInput
import java.io.DataOutput
import java.io.IOException

import org.apache.hadoop.hbase.mapreduce.TableSplit
import org.apache.hadoop.mapred.InputSplit

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability

/**
 * An input split that specifies a region of rows from a Kiji table to be processed by a
 * MapReduce task.
 *
 * @param split to which functionality is delegated.
 */
@ApiAudience.Private
@ApiStability.Experimental
final class KijiTableSplit(
    private val split: TableSplit)
    extends InputSplit {

  /**
   * No argument constructor for KijiTableSplit. This is required to be a seperate constructor
   * so that java has access to it.
   */
  // scalastyle:off public.methods.have.type
  def this() = this(new TableSplit())
  // scalastyle:on public.methods.have.type

  override def getLength(): Long = split.getLength()

  override def getLocations(): Array[String] = split.getLocations()

  override def readFields(in: DataInput) {
    split.readFields(in)
  }

  override def write(out: DataOutput) {
    split.write(out)
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

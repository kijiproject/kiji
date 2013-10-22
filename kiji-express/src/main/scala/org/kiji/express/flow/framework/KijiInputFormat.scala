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

import scala.collection.JavaConverters._

import com.google.common.base.Preconditions.checkNotNull
import org.apache.hadoop.hbase.mapreduce.TableSplit
import org.apache.hadoop.mapred.InputFormat
import org.apache.hadoop.mapred.InputSplit
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.RecordReader
import org.apache.hadoop.mapred.Reporter

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.express.util.Resources.doAndClose
import org.kiji.express.util.Resources.doAndRelease
import org.kiji.mapreduce.framework.KijiConfKeys
import org.kiji.schema.Kiji
import org.kiji.schema.KijiRegion
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiURI
import org.kiji.schema.impl.HBaseKijiTable

/**
 * Tells the MapReduce framework how to divide a Kiji table into input splits for tasks,
 * and how to read records from those splits.
 *
 * MapReduce views a data set as a collection of key-value pairs divided into input splits,
 * where each input split is processed by a MapReduce task. This input format divides a Kiji
 * table into one input split per HBase region in the table. It also provides access to a record
 * reader (specifically [[org.kiji.express.flow.framework.KijiRecordReader]]) which knows how to
 * read rows from a Kiji table as key-value pairs.
 *
 * A MapReduce job reading from a Kiji table as part of the KijiExpress framework should be
 * configured with this input format. The job using this input format should have a configuration
 * containing a serialized `KijiDataRequest` at the key `kiji.input.data.request` and a Kiji URI
 * addressing the target table at the key `kiji.input.table.uri`.
 *
 * The Kiji framework already has an input format for reading from Kiji tables,
 * but it is written against a newer MapReduce API than the one supported by Cascading. This
 * input format exists to address this compatibility issue.
 */
@ApiAudience.Framework
@ApiStability.Experimental
@Inheritance.Sealed
final class KijiInputFormat
    extends InputFormat[KijiKey, KijiValue] {
  /**
   * Creates one input split per HBase region of a Kiji table.
   *
   * @param configuration containing a Kiji URI at the key `kiji.input.table.uri` that addresses
   *     the table to generate splits for.
   * @param numSplits is a MapReduce framework hint for the number of splits to produce,
   *     and is ignored here.
   * @return one input split per HBase region of the Kiji table.
   */
  override def getSplits(configuration: JobConf, numSplits: Int): Array[InputSplit] = {
    val uriString: String = checkNotNull(configuration.get(KijiConfKeys.KIJI_INPUT_TABLE_URI))
    val inputTableURI: KijiURI = KijiURI.newBuilder(uriString).build()

    doAndRelease(Kiji.Factory.open(inputTableURI, configuration)) { kiji: Kiji =>
      doAndRelease(kiji.openTable(inputTableURI.getTable())) { table: KijiTable =>
        doAndClose(HBaseKijiTable.downcast(table).openHTableConnection()) { htable =>
          table.getRegions().asScala.map { region: KijiRegion =>
            val startKey: Array[Byte] = region.getStartKey()
            // TODO(KIJIMR-65): For now pick the first available location (ie. region server),
            //     if any.
            val location: String = {
              if (region.getLocations().isEmpty()) {
                // scalastyle:off null
                null
                // scalastyle:on null
              } else {
                region.getLocations().iterator().next()
              }
            }
            val tableSplit: TableSplit = {
              new TableSplit(
                  htable.getTableName(),
                  startKey,
                  region.getEndKey(),
                  location)
            }
            new KijiTableSplit(tableSplit)
          }
          .toArray
        }
      }
    }
  }

  /**
   * Creates a record reader that will read rows from a region of a Kiji table as key-value pairs.
   *
   * @param split identifies the HBase region of the Kiji table that should be read.
   * @param configuration containing a serialized data request at the key
   *     `kiji.input.data.request` which will be used to configure how data will be read from
   *     HBase.
   * @param reporter is provided by the MapReduce framework as a means for tasks to report status,
   *     and is ignored here.
   * @return a record reader that will read rows from a region of a Kiji table as key-value pairs.
   */
  override def getRecordReader(
      split: InputSplit,
      configuration: JobConf,
      reporter: Reporter): RecordReader[KijiKey, KijiValue] = {
    split match {
      // TODO: Use reporter to report progress.
      case kijiSplit: KijiTableSplit => new KijiRecordReader(kijiSplit, configuration)
      case _ => sys.error("KijiInputFormat requires a KijiTableSplit.")
    }
  }
}

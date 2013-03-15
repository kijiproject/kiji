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

import scala.collection.JavaConverters._

import com.google.common.base.Preconditions.checkNotNull
import org.apache.hadoop.hbase.client.HTableInterface
import org.apache.hadoop.hbase.mapreduce.TableSplit
import org.apache.hadoop.mapred.InputFormat
import org.apache.hadoop.mapred.InputSplit
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.RecordReader
import org.apache.hadoop.mapred.Reporter

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.chopsticks.Resources.doAndRelease
import org.kiji.mapreduce.framework.KijiConfKeys
import org.kiji.schema.Kiji
import org.kiji.schema.KijiRegion
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiURI
import org.kiji.schema.impl.HBaseKijiTable

/**
 * <p>An [[InputFormat]] for use with Cascading whose input source is a Kiji table. This
 * input format will scan over a subset of rows in a Kiji table, retrieving
 * [[org.kiji.schema.KijiRowData]] as a result. The columns retrieved during the scan, as well as
 * the time and row ranges used, are configured through an [[org.kiji.schema.KijiDataRequest]].</p>
 *
 * <p>The input format itself is responsible for computing input splits over the Kiji table,
 * and retrieving a [[org.apache.hadoop.mapred.RecordReader]] for a particular split. See
 * [[KijiRecordReader]] for more information on how a particular input split is scanned
 * for rows. Record readers returned by this input format return key-value pairs of type
 * [[KijiKey]] and [[KijiValue]], which are simple wrappers around
 * [[org.kiji.schema.EntityId]] and [[org.kiji.schema.KijiRowData]], respectively.
 *
 * <p>This input format uses the "old" style MapReduce API for compatibility with Cascading.</p>
 */
@ApiAudience.Framework
@ApiStability.Unstable
final class KijiInputFormat
    extends InputFormat[KijiKey, KijiValue] {
  /**
   * Gets a set of input splits for a MapReduce job running over a Kiji table. One split is
   * created per region in the input Kiji table.
   *
   * @param configuration of the job using the splits. The configuration should specify the
   *     input Kiji table being used, through the configuration variable
   *     [[KijiConfKeys#KIJI_INPUT_TABLE_URI]].
   * @param numSplits desired for the job. This framework hint is ignored by this method.
   * @return an array of input splits to be operated on in the MapReduce job.
   */
  override def getSplits(configuration: JobConf, numSplits: Int): Array[InputSplit] = {
    val uriString: String = checkNotNull(configuration.get(KijiConfKeys.KIJI_INPUT_TABLE_URI))
    val inputTableURI: KijiURI = KijiURI.newBuilder(uriString).build()

    doAndRelease(Kiji.Factory.open(inputTableURI, configuration)) { kiji: Kiji =>
      doAndRelease(kiji.openTable(inputTableURI.getTable())) { table: KijiTable =>
        val htable: HTableInterface = HBaseKijiTable.downcast(table).getHTable()

        table.getRegions().asScala
            .map { region: KijiRegion =>
              val startKey: Array[Byte] = region.getStartKey()
              // TODO(KIJIMR-65): For now pick the first available location (ie. region server),
              //     if any.
              val location: String = if (region.getLocations().isEmpty()) {
                    null
                  } else {
                    region.getLocations().iterator().next()
                  }
              val tableSplit: TableSplit = new TableSplit(
                  htable.getTableName(),
                  startKey,
                  region.getEndKey(),
                  location)

              new KijiTableSplit(tableSplit)
            }
            .toArray
      }
    }
  }

  /**
   * Gets a record reader that will scan over a subset of rows in a Kiji table.
   *
   * @param split the record reader should operate over.
   * @param configuration of the job that uses the record reader. The configuration should specify
   *     the input Kiji table through the configuration variable
   *     [[KijiConfKeys#KIJI_INPUT_TABLE_URI]] and a serialized [[org.kiji.schema.KijiDataRequest]]
   *     through the configuration variable [[KijiConfKeys#KIJI_INPUT_DATA_REQUEST]].
   * @param reporter is ignored by this method.
   * @return An [[KijiRecordReader]] that will scan over a subset of rows in a Kiji table.
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

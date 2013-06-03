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

package org.kiji.express

import org.apache.commons.codec.binary.Base64
import org.apache.commons.lang.SerializationUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.RecordReader

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.express.util.Resources.doAndRelease
import org.kiji.mapreduce.framework.KijiConfKeys
import org.kiji.schema.HBaseEntityId
import org.kiji.schema.Kiji
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiRowData
import org.kiji.schema.KijiRowScanner
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiTableReader
import org.kiji.schema.KijiTableReader.KijiScannerOptions
import org.kiji.schema.KijiURI

/**
 * Reads rows from an HBase region of a Kiji table as key-value pairs that can be used with the
 * MapReduce framework.
 *
 * MapReduce views a data set as a collection of key-value pairs divided into input splits,
 * where each input split is processed by a MapReduce task. This record reader can scan a subset
 * of rows from a Kiji table (really an HBase region of the Kiji table identified by an input
 * split) and transform them into key-value pairs that can be processed by the MapReduce framework.
 *
 * Key-value pairs are obtained from Kiji rows as follows. For each row read,
 * the row's entity id is used as key (by populating an instance of
 * [[org.kiji.express.KijiKey]]) and the row data itself is used as value (by populating an
 * instance of [[org.kiji.express.KijiValue]]). The classes [[org.kiji.express.KijiKey]]
 * and [[org.kiji.express.KijiValue]] are simply reusable containers wrapping entity ids and
 * row data.
 *
 * A MapReduce job reading from a Kiji table as part of the KijiExpress framework should be
 * configured to use [[org.kiji.express.KijiInputFormat]], which allow the job to use this
 * record reader. The job using this record reader should have a configuration
 * containing a serialized `KijiDataRequest` at the key `kiji.input.data.request` and a Kiji URI
 * addressing the target table at the key `kiji.input.table.uri`.
 *
 * @param split identifying the HBase region of a Kiji table whose rows will be scanned.
 * @param configuration containing the Kiji URI of the target Kiji table, and a serialized data
 *     request.
 */
@ApiAudience.Framework
@ApiStability.Experimental
final class KijiRecordReader(
    private val split: KijiTableSplit,
    private val configuration: Configuration)
    extends RecordReader[KijiKey, KijiValue] {
  if (!split.isInstanceOf[KijiTableSplit]) {
    sys.error("KijiRecordReader received an InputSplit that was not a KijiTableSplit.")
  }

  /** The data request used to read from the Kiji table. */
  private val dataRequest: KijiDataRequest = {
    // Get data request from the job configuration.
    val dataRequestB64: String = {
      Option(configuration.get(KijiConfKeys.KIJI_INPUT_DATA_REQUEST)) match {
        case Some(str) => str
        case None => sys.error("Missing data request in job configuration.")
      }
    }

    val dataRequestBytes: Array[Byte] = Base64.decodeBase64(Bytes.toBytes(dataRequestB64))
    SerializationUtils.deserialize(dataRequestBytes).asInstanceOf[KijiDataRequest]
  }

  /** A Kiji URI addressing the target table. */
  private val inputURI: KijiURI = KijiURI
      .newBuilder(configuration.get(KijiConfKeys.KIJI_INPUT_TABLE_URI))
      .build()

  /** A reader for the above table. */
  private val reader: KijiTableReader = {
    doAndRelease(Kiji.Factory.open(inputURI, configuration)) { kiji: Kiji =>
      doAndRelease(kiji.openTable(inputURI.getTable())) { table: KijiTable =>
        table.openTableReader()
      }
    }
  }
  /** Used to scan a subset of rows from the table. */
  private val scanner: KijiRowScanner = {
    val scannerOptions: KijiScannerOptions = new KijiScannerOptions()
        .setStartRow(HBaseEntityId.fromHBaseRowKey(split.getStartRow()))
        .setStopRow(HBaseEntityId.fromHBaseRowKey(split.getEndRow()))

    reader.getScanner(dataRequest, scannerOptions)
  }
  /** An iterator over the rows retrieved by the scanner. */
  private val iterator: java.util.Iterator[KijiRowData] = scanner.iterator()

  /** This prevents errors when KijiRecordReader is closed multiple times. See CHOP-56. */
  private var isClosed: Boolean = false

  /**
   * Gets a key instance that can be populated with entity ids scanned from the table.
   *
   * @return a new, empty, reusable key instance that will hold entity ids scanned by this record
   *   reader. Note that until populated, the key instance will return `null` if you attempt to
   *   retrieve the entity id from the key.
   */
  override def createKey(): KijiKey = new KijiKey()

  /**
   * Gets a value instance that can be populated with row data scanned from the table.
   *
   * @return a new, empty, reusable value instance that will hold row data scanned by this record
   *     reader. Note that until populated, the value instance will return `null` if you attempt
   *     to retrieve the row data from the value.
   */
  override def createValue(): KijiValue = new KijiValue()

  /**
   * @return `OL` always, because it's impossible to tell how much we've read through
   *     a particular key range, because we have no knowledge of how many rows are actually in
   *     the range.
   */
  override def getPos(): Long = 0L

  /**
   * @return `0.0` always, because it's impossible to tell how much we've read through
   *     a particular key range, because we have no knowledge of how many rows are actually in
   *     the range.
   */
  override def getProgress(): Float = 0.0f

  /**
   * Scans the next row from the region of the Kiji table, and populates a key and value with the
   * entity id and row data obtained.
   *
   * @param key instance to populate with the next entity id read.
   * @param value instance to populate with the next row data read.
   * @return `true` if a new row was scanned and the key and value populated, `false` if the end
   *     of the region has been reached.
   */
  override def next(key: KijiKey, value: KijiValue): Boolean = {
    if (iterator.hasNext()) {
      // Read the next row and store it in the provided key/value pair.
      val row: KijiRowData = iterator.next()
      // scalastyle:off null
      if (null != key) {
        key.set(row.getEntityId())
      }
      if (null != value) {
        value.set(row)
      }
      // scalastyle:off null
      true
    } else {
      false
    }
  }

  /**
   * Closes the table scanner and table reader used by this instance to scan a region of a Kiji
   * table.
   *
   * It is safe (but unnecessary) to call this method multiple times.
   */
  override def close() {
    if (!isClosed) {
      isClosed = true

      scanner.close()
      reader.close()
    }
  }
}

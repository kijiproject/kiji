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

package org.kiji.lang

import org.apache.hadoop.hbase.client.Result
import org.scalatest.FunSuite

import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiSchemaTable
import org.kiji.schema.KijiURI
import org.kiji.schema.impl.HBaseKijiRowData
import org.kiji.schema.impl.HBaseKijiTableReader
import org.kiji.schema.layout.KijiTableLayout

class KijiValueSuite extends FunSuite {
  val kijiURI = KijiURI.newBuilder("kiji://.env/default").build();
  val tableName = "mTable"

  test("KijiValue should get the same RowData you put in.") {
    val dataRequest = KijiDataRequest.create("columnfamily")
    val result = new Result()
    val rowData = new HBaseKijiRowData(null, dataRequest, null, null, result, null)
    val testValue = new KijiValue()
    testValue.set(rowData)

    assert(rowData == testValue.get())
  }
}

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

package org.kiji.express.datarequest

import org.scalatest.FunSuite
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.filter.RegexQualifierColumnFilter

class DataRequestTestSuite extends FunSuite {
  test("ExpressDataRequest can be translated to a KijiDataRequest.") {
    val startingDataRequest: ExpressDataRequest = new ExpressDataRequest(0, 100010L,
        new ExpressColumnRequest("info:test", 2, Option(new RegexQualifierFilter("x*"))) :: Nil)
    val transformed2KDR: KijiDataRequest = startingDataRequest.toKijiDataRequest()

    assert(0 === transformed2KDR.getMinTimestamp)
    assert(100010L === transformed2KDR.getMaxTimestamp)
    val colReq: KijiDataRequest.Column = transformed2KDR.getColumn("info", "test")
    assert("info:test" === colReq.getColumnName.getName)
    assert(2 === colReq.getMaxVersions)
    assert(colReq.getFilter.isInstanceOf[RegexQualifierColumnFilter])
  }
}

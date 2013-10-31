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

package org.kiji.modeling.config

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import org.kiji.schema.KijiDataRequest
import org.kiji.schema.filter.RegexQualifierColumnFilter

@RunWith(classOf[JUnitRunner])
class ExpressDataRequestSuite extends FunSuite {
  test("ExpressDataRequest can be translated to a KijiDataRequest.") {
    val testDataRequest = ExpressDataRequest(
        minTimestamp = 0,
        maxTimestamp = 100010L,
        columnRequests = Seq(
            ExpressColumnRequest(
                name = "info:test",
                maxVersions = 2,
                filter = Some(RegexQualifierFilter("x*")))))

    val kijiDataRequest: KijiDataRequest = testDataRequest.toKijiDataRequest
    assert(0 === kijiDataRequest.getMinTimestamp)
    assert(100010L === kijiDataRequest.getMaxTimestamp)

    val columnRequest: KijiDataRequest.Column = kijiDataRequest.getColumn("info", "test")
    assert("info:test" === columnRequest.getColumnName.getName)
    assert(2 === columnRequest.getMaxVersions)
    assert(columnRequest.getFilter.isInstanceOf[RegexQualifierColumnFilter])
  }
}

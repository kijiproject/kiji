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

package ${package}

import com.twitter.scalding.Args
import com.twitter.scalding.TextLine

import org.kiji.express.flow._

class DemoKijiLoader(args: Args) extends KijiJob(args) {

  def parseLine(text: String): (String, String) = {
    text.split(' ') match {
      case Array(v1, v2) => (v1, v2)
    }
  }

  TextLine(args("input"))
    .map('line -> ('userId, 'name)) { parseLine }
    .map('userId -> 'entityId) { userId: String => EntityId(userId) }
    .write(KijiOutput.builder
        .withTableURI(args("table"))
        .withColumns('userId -> "info:userId",
            'name -> "info:name")
        .build)
}

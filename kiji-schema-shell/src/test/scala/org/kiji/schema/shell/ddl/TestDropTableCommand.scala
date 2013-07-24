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

package org.kiji.schema.shell.ddl

import scala.collection.mutable.Map

import org.kiji.schema.shell.DDLException
import org.kiji.schema.shell.ddl.key._

class TestDropTableCommand extends CommandTestCase {
  "DropTableCommand" should {
    "require the table exists" in {
      val dropcmd = new DropTableCommand(env, "missing-table-name")
      dropcmd.validateArguments() must throwA[DDLException]
    }

    "remove the table from the dictionary" in {
      val locGroup = new LocalityGroupClause("default", None, List())
      val ctcmd = new CreateTableCommand(env, "foo", None, new HashedFormattedKeySpec,
          List(locGroup), Map())
      ctcmd.exec() // Create a table.

      val dropcmd = new DropTableCommand(env, "foo")
      dropcmd.exec() // Remove it.

      dropcmd.validateArguments() must throwA[DDLException] // Can't drop it twice.
    }
  }
}

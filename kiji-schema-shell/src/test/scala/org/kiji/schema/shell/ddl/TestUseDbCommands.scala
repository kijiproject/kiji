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

import scala.collection.JavaConversions._
import org.specs2.mutable._

import org.kiji.schema.KConstants
import org.kiji.schema.shell.DDLException
import org.kiji.schema.shell.DDLParser

class TestUseDbCommands extends CommandTestCase {
  "UseInstanceCommand" should {
    "require the db exists" in {
      val usecmd = new UseInstanceCommand(env, "missing-instance")
      usecmd.exec() must throwA[DDLException]
    }

    "select the database we request" in {
      val usecmd = new UseInstanceCommand(env, "foo")
      val env2 = usecmd.exec()
      env2.instanceURI.getInstance() mustEqual "foo"
    }
  }

  "Parsed USE statements" should {
    "select the foo instance and then the default instance" in {
      val parser1: DDLParser = new DDLParser(env)
      val res1 = parser1.parseAll(parser1.statement, "USE foo;")
      res1.successful mustEqual true

      val env2 = res1.get.exec()
      env2.instanceURI.getInstance() mustEqual "foo"
      val parser2: DDLParser = new DDLParser(env2)
      val res2 = parser2.parseAll(parser2.statement, "USE DEFAULT INSTANCE;")
      res2.successful mustEqual true

      val env3 = res2.get.exec()
      env3.instanceURI.getInstance() mustEqual KConstants.DEFAULT_INSTANCE_NAME
    }
  }
}

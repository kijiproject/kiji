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

import org.kiji.schema.layout.KijiTableLayout

import org.kiji.schema.shell.DDLException
import org.kiji.schema.shell.DDLParser
import org.kiji.schema.shell.Environment

class TestLocalityGroupClause extends CommandTestCase {
  "LocalityGroupClause" should {
    "correctly extract family names" in {
      val parser = new DDLParser(env)
      val res = parser.parseAll(parser.lg_clause, """
LOCALITY GROUP default WITH DESCRIPTION 'main storage' (
  MAXVERSIONS = INFINITY,
  TTL = FOREVER,
  INMEMORY = false,
  COMPRESSED WITH GZIP,
  FAMILY info WITH DESCRIPTION 'basic information' (
    name "string" WITH DESCRIPTION 'The user\'s name',
    email "string",
    age "int"),
  MAP TYPE FAMILY integers COUNTER WITH DESCRIPTION 'metric tracking data'
)""")
      res.successful mustEqual true
      val locGroupClause: LocalityGroupClause = res.get
      locGroupClause.getFamilyNames() mustEqual List("info", "integers")
    }
  }
}

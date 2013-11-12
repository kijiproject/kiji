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

package org.kiji.modeling.shellext

import org.kiji.express.KijiSuite
import org.kiji.schema.shell.DDLParser
import org.kiji.schema.shell.Environment
import org.kiji.schema.shell.MockKijiSystem
import org.kiji.schema.shell.input.NullInputSource

/**
 * Provides utility functions for tests that employ the KijiExpress extension to the KijiSchema
 * DDL Shell language.
 */
trait ShellExtSuite extends KijiSuite {

  /**
   * Gets a DDL shell parser suitable for tests, with no modules pre-loaded.
   *
   * @return a DDL shell parser suitable for tests, with no modules pre-loaded.
   */
  protected def getBaseParser(): DDLParser = {
    val kijiURI = makeTestKiji().getURI()
    val environment = new Environment(
        instanceURI = kijiURI,
        printer = System.out,
        kijiSystem = new MockKijiSystem(),
        inputSource = new NullInputSource(),
        modules = List(),
        isInteractive = false)
    new DDLParser(environment)
  }

  /**
   * Gets an environment in which DDL shell statements can be executed,
   * pre-loaded with the KijiExpress `modeling` module.
   *
   * @return an environment DDL shell statements can be executed in,
   *     which can recognize KijiExpress extensions to the DDL shell language.
   */
  protected def getLoadedEnvironment(): Environment = {
    val parser = getBaseParser()
    val res = parser.parseAll(parser.statement, "MODULE modeling;")
    if (!res.successful) {
      sys.error("Unsuccessful parse of 'MODULE modeling;'")
    }
    res.get.exec()
  }

  /**
   * Gets an instance of the parser plugin used to parse KijiExpress extensions to the DDL shell
   * language.
   *
   * @return an instance of the parser plugin used to parse KijiExpress extensions to the DDL shell
   *     language.
   */
  protected def getParserPlugin(): ModelingParserPlugin = {
    new ModelingParserPlugin(getLoadedEnvironment())
  }

  /**
   * Gets a DDL shell parser pre-loaded with the KijiExpress `modeling` module,
   * and thus ready to parse statements written against the KijiExpress extension to the DDL
   * shell language.
   *
   * @return a DDL shell parser pre-loaded with the KijiExpress `modeling module`.
   */
  protected def getLoadedParser(): DDLParser = {
    new DDLParser(getLoadedEnvironment())
  }
}

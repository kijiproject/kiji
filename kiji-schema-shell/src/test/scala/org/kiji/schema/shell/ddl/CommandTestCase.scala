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

import org.specs2.mutable._

import org.kiji.schema.KConstants
import org.kiji.schema.KijiURI
import org.kiji.schema.shell.Environment
import org.kiji.schema.shell.MockKijiSystem
import org.kiji.schema.shell.input.NullInputSource

/**
 * Values &amp; Methods common to test cases for DDL commands.
 */
class CommandTestCase extends SpecificationWithJUnit {
  isolated // Create a new instance for every test, to get a fresh environment.

  /** URI pointing to the default Kiji instance. */
  val defaultURI = KijiURI.newBuilder().withInstanceName(KConstants.DEFAULT_INSTANCE_NAME).build()

  /** Return the environment to use for testing. */
  val env: Environment =
      new Environment(defaultURI, System.out, new MockKijiSystem,
          new NullInputSource, List(), false)
}

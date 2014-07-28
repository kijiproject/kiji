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

package org.kiji.mapreduce.shellext

import org.specs2.mutable._

import org.kiji.schema.shell.spi.ParserPluginTestKit

/** Tests that parser plugins adhere to the SPI contract. */
class TestSpiContract extends SpecificationWithJUnit {

  // Test that we implement the contract specified by ParserPluginTestKit:
  // Bulk importer parser plugin should past the PPTK
  new ParserPluginTestKit(classOf[BulkImportParserPluginFactory]).testAll
}

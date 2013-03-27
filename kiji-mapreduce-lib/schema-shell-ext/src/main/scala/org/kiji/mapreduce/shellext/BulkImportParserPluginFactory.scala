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

import org.kiji.schema.shell.Environment
import org.kiji.schema.shell.spi.ParserPlugin
import org.kiji.schema.shell.spi.ParserPluginFactory

/**
 * Factory for bulk importer-specific parser plugin that provides bulk importer capabilities in
 * the shell.
 *
 * <p>To use this module, load it with <tt>MODULE bulkimport;</tt></p>
 */
class BulkImportParserPluginFactory extends ParserPluginFactory {
  /** {@inheritDoc} */
  override def create(env: Environment): ParserPlugin = { new BulkImportParserPlugin(env) }

  /** {@inheritDoc} */
  override def getName(): String = "bulkimport"
}

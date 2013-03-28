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

import java.io.File
import java.io.FileOutputStream
import java.io.PrintStream

import org.kiji.annotations.ApiAudience
import org.kiji.schema.shell.DDLException
import org.kiji.schema.shell.Environment
import org.kiji.schema.util.ResourceUtils

/**
 * Emit the DDL to create all tables to stdout or a file.
 */
@ApiAudience.Private
final class DumpInstanceDDLCommand(val env: Environment, val maybeFile: Option[String])
    extends DDLCommand with AbstractDumpDDLCommand {

  override def exec(): Environment = {
    maybeFile match {
      case None => { dumpAllLayouts() } // Just dump to stdout as planned.
      case Some(filename) => {
        val outputStream = new PrintStream(new FileOutputStream(new File(filename)), false, "UTF-8")
        try {
          // Re-execute this command in an environment with the text output redirected
          // to the specified file.
          new DumpInstanceDDLCommand(env.withPrinter(outputStream), None).exec()
        } finally {
          ResourceUtils.closeOrLog(outputStream)
        }
      }
    }
    return env
  }

  /**
   * Find all tables and dump their layouts to the environment's output sink.
   */
  def dumpAllLayouts(): Unit = {
    val namesAndDescs = env.kijiSystem.getTableNamesDescriptions(getKijiURI())
    namesAndDescs.foreach { case (name, _) =>
      dumpLayout(env.kijiSystem.getTableLayout(getKijiURI(), name).getOrElse(
          // Shouldn't get this, since the KijiSystem just returned this name to us..
          // but we're racing with other clients of the system who may have dropped
          // the table.
          throw new DDLException("Cannot access table " + name)).getDesc())
      echo("")
    }
  }
}

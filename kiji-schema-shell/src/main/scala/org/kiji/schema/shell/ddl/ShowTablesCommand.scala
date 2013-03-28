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

import scala.math._

import org.kiji.annotations.ApiAudience
import org.kiji.schema.shell.Environment

@ApiAudience.Private
final class ShowTablesCommand(val env: Environment) extends DDLCommand with StrFormatting {
  override def exec(): Environment = {
    val tableNamesDescriptions = env.kijiSystem.getTableNamesDescriptions(getKijiURI())

    if (tableNamesDescriptions.size == 0) {
      // No tables; exit here.
      echo("(No tables available)")
      return env
    }

    // Get the max table name and description lengths for use in printing.
    val maxNameLength = max("table".length,
        tableNamesDescriptions.maxBy{ pair => pair._1.length() }._1.length())
    val maxDescrLength = max("description".length,
        tableNamesDescriptions.maxBy{ pair => pair._2.length() }._2.length())

    // Build an output, starting with a header for the Table/Description display.
    val output = new StringBuilder()
    output.append(padTo("Table", maxNameLength))
        .append("\tDescription\n")
        .append("=" * maxNameLength)
        .append("\t")
        .append("=" * maxDescrLength)
        .append("\n")
    // Add the table names and descriptions
    tableNamesDescriptions.foreach { case (name, description) =>
      output.append(padTo(name, maxNameLength))
          .append("\t")
          .append(description)
          .append("\n")
    }
    echoNoNL(output.toString())
    return env
  }
}

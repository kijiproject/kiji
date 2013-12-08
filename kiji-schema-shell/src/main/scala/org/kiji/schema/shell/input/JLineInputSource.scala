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

package org.kiji.schema.shell.input

import scala.tools.jline.console.ConsoleReader

import org.kiji.annotations.ApiAudience

/** Reads user input from the console using JLine. */
@ApiAudience.Private
final class JLineInputSource extends InputSource {
  private val consoleReader = new ConsoleReader

  override def readLine(prompt: String): Option[String] = {
    return Option(consoleReader.readLine(prompt))
  }

  override def close(): Unit = { }
}
